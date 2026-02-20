"""
Parallel image processing worker for forensic analysis.

This module provides parallel processing of images using ProcessPoolExecutor for CPU-bound
operations (hashing, perceptual hashing, EXIF extraction, thumbnail generation).

Maintains forensic integrity through:
- Deterministic ordering (results sorted by input path)
- Complete error handling and logging
- Reproducible results independent of processing order
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional, List
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED, TimeoutError as FuturesTimeoutError

from PIL import Image, UnidentifiedImageError
from PIL.Image import DecompressionBombError

from core.hashing import hash_file
from core.image_codecs import ensure_pillow_heif_registered
from core.phash import compute_phash
from core.logging import get_logger
from .exif import extract_exif, generate_thumbnail

LOGGER = get_logger("extractors._shared.carving.processor")
DEFAULT_MAX_IMAGE_PIXELS = 175_000_000  # keep below Pillow default to retain safety margin
DEFAULT_IMAGE_TIMEOUT = 30  # seconds per image - prevents hanging on corrupted files
DEFAULT_BATCH_TIMEOUT_BUFFER = 120  # extra seconds for batch completion
DEFAULT_STUCK_THRESHOLD = 60  # seconds without progress before considering stuck


@dataclass(slots=True)
class ImageProcessResult:
    """Result of processing a single image."""
    path: Path
    rel_path: str
    filename: str
    md5: Optional[str] = None
    sha256: Optional[str] = None
    phash: Optional[str] = None
    exif_json: str = "{}"
    thumbnail_path: Optional[Path] = None
    size_bytes: Optional[int] = None
    error: Optional[str] = None
    notes: Optional[str] = None  # For fallback/warning messages

    def to_db_record(self, discovered_by: str) -> dict:
        """Convert to database record format."""
        return {
            "rel_path": self.rel_path,
            "filename": self.filename,
            "md5": self.md5,
            "sha256": self.sha256,
            "phash": self.phash,
            "exif_json": self.exif_json,
            "size_bytes": self.size_bytes,
            "discovered_by": discovered_by,
            "notes": self.notes,
        }


def safe_probe_image(image_path: Path, max_pixels: int = DEFAULT_MAX_IMAGE_PIXELS) -> tuple[int, int]:
    """
    Open image headers safely and enforce a pixel cap before heavy processing.

    Raises:
        DecompressionBombError if pixel count exceeds cap or Pillow detects bomb
        UnidentifiedImageError / OSError for unreadable files
    """
    ensure_pillow_heif_registered()
    with Image.open(image_path) as img:
        width, height = img.size
        pixels = width * height
        if pixels > max_pixels:
            raise DecompressionBombError(
                f"Image pixels {pixels} exceeds limit of {max_pixels}"
            )
        # Verify basic integrity without decoding full image
        img.verify()
        return width, height


def process_image_worker(image_path: Path, out_dir: Path, thumb_size: tuple[int, int] = (256, 256)) -> ImageProcessResult:
    """
    Worker function to process a single image (CPU-bound operations).

    This function must be picklable (top-level function, no closures) for ProcessPoolExecutor.

    Args:
        image_path: Path to the image file
        out_dir: Output directory for carved files (for relative path calculation)
        thumb_size: Thumbnail dimensions (width, height)

    Returns:
        ImageProcessResult with computed hashes, EXIF, and thumbnail path

    Notes:
        - Computes MD5, SHA256, perceptual hash
        - Extracts EXIF metadata
        - Generates thumbnail (I/O operation, but small)
        - Returns error message if processing fails
        - Results are deterministic given the same input
    """
    try:
        # Get file size before processing
        size_bytes = image_path.stat().st_size

        # Lightweight header check to avoid decompression bombs or unreadable images
        try:
            safe_probe_image(image_path)
        except DecompressionBombError as exc:
            # Decompression bomb - skip entirely for safety
            LOGGER.warning("Skipping potential decompression bomb %s: %s", image_path, exc)
            try:
                rel_path = image_path.relative_to(out_dir).as_posix()
            except ValueError:
                rel_path = image_path.as_posix()
            return ImageProcessResult(
                path=image_path,
                rel_path=rel_path,
                filename=image_path.name,
                size_bytes=size_bytes,
                error=f"{type(exc).__name__}: {exc}",
            )
        except (UnidentifiedImageError, OSError) as exc:
            # PIL can't decode - compute hash-only fallback
            LOGGER.info("PIL cannot decode %s, creating hash-only record: %s", image_path, exc)
            try:
                rel_path = image_path.relative_to(out_dir).as_posix()
            except ValueError:
                rel_path = image_path.as_posix()

            # Compute hashes even for unrecognized formats
            try:
                md5 = hash_file(image_path, alg="md5")
                sha256 = hash_file(image_path, alg="sha256")
            except Exception as hash_exc:
                LOGGER.warning("Failed to hash %s: %s", image_path, hash_exc)
                md5 = None
                sha256 = None

            return ImageProcessResult(
                path=image_path,
                rel_path=rel_path,
                filename=image_path.name,
                md5=md5,
                sha256=sha256,
                phash=None,
                exif_json="{}",
                size_bytes=size_bytes,
                notes=f"PIL decode failed: {type(exc).__name__}: {exc}",
            )

        # CPU-bound: hash computation (works on any file)
        md5 = hash_file(image_path, alg="md5")
        sha256 = hash_file(image_path, alg="sha256")

        # CPU-bound: perceptual hash (may fail for corrupted images)
        # Note: Carved images may be false positives - phash will be None
        phash = compute_phash(image_path)

        # CPU-bound: EXIF extraction
        exif = extract_exif(image_path)
        exif_json = json.dumps(exif, sort_keys=True)

        # I/O-bound but small: thumbnail generation
        thumb_dir = out_dir / "thumbnails"
        thumb_dir.mkdir(exist_ok=True, parents=True)
        thumb_path = thumb_dir / f"{image_path.stem}_thumb.jpg"
        generate_thumbnail(image_path, thumb_path, size=thumb_size)

        # Calculate relative path
        try:
            rel_path = image_path.relative_to(out_dir).as_posix()
        except ValueError:
            # If not relative to out_dir, use absolute path
            rel_path = image_path.as_posix()

        return ImageProcessResult(
            path=image_path,
            rel_path=rel_path,
            filename=image_path.name,
            md5=md5,
            sha256=sha256,
            phash=phash,
            exif_json=exif_json,
            thumbnail_path=thumb_path,
            size_bytes=size_bytes,
        )

    except (ValueError, OSError) as exc:
        # Handle PIL decode errors that occur after probe passed
        # e.g., "tile cannot extend outside image" for corrupted GIFs
        error_msg = f"{type(exc).__name__}: {exc}"
        LOGGER.warning("PIL decode error for %s: %s - creating hash-only record", image_path, error_msg)

        try:
            rel_path = image_path.relative_to(out_dir).as_posix()
        except ValueError:
            rel_path = image_path.as_posix()

        # Still compute hashes even for malformed images
        try:
            size_bytes = image_path.stat().st_size
        except Exception:
            size_bytes = None
        try:
            md5 = hash_file(image_path, alg="md5")
            sha256 = hash_file(image_path, alg="sha256")
        except Exception as hash_exc:
            LOGGER.warning("Failed to hash %s: %s", image_path, hash_exc)
            md5 = None
            sha256 = None

        return ImageProcessResult(
            path=image_path,
            rel_path=rel_path,
            filename=image_path.name,
            md5=md5,
            sha256=sha256,
            size_bytes=size_bytes,
            notes=f"PIL decode error: {error_msg}",
        )

    except Exception as exc:
        # Catch all other errors to ensure worker doesn't crash the pool
        error_msg = f"{type(exc).__name__}: {exc}"
        LOGGER.warning("Failed to process image %s: %s", image_path, error_msg)

        try:
            rel_path = image_path.relative_to(out_dir).as_posix()
        except ValueError:
            rel_path = image_path.as_posix()

        return ImageProcessResult(
            path=image_path,
            rel_path=rel_path,
            filename=image_path.name,
            error=error_msg,
        )


class ParallelImageProcessor:
    """
    Parallel image processor using ProcessPoolExecutor.

    Processes multiple images in parallel while maintaining forensic integrity through
    deterministic ordering and complete error handling.
    """

    def __init__(
        self,
        max_workers: Optional[int] = None,
        enable_parallel: bool = True,
        timeout_per_image: float = DEFAULT_IMAGE_TIMEOUT
    ):
        """
        Initialize parallel image processor.

        Args:
            max_workers: Maximum number of worker processes (None = CPU count)
            enable_parallel: If False, process sequentially for debugging
            timeout_per_image: Max seconds to wait for a single image (prevents hanging)
        """
        self.max_workers = max_workers
        self.enable_parallel = enable_parallel
        self.timeout_per_image = timeout_per_image

    def process_images(
        self,
        image_paths: List[Path],
        out_dir: Path,
        thumb_size: tuple[int, int] = (256, 256),
    ) -> List[ImageProcessResult]:
        """
        Process multiple images in parallel.

        Args:
            image_paths: List of image paths to process
            out_dir: Output directory for carved files
            thumb_size: Thumbnail dimensions

        Returns:
            List of ImageProcessResult in deterministic order (sorted by path)

        Notes:
            - Results are always returned in the same order (sorted by path)
            - Failed images return ImageProcessResult with error field set
            - Logs processing statistics (success/failure counts)
        """
        if not image_paths:
            LOGGER.debug("No images to process")
            return []

        # Sort input for deterministic ordering
        sorted_paths = sorted(image_paths)

        LOGGER.info("Processing %d images with parallel=%s, timeout=%ds",
                    len(sorted_paths), self.enable_parallel, self.timeout_per_image)

        if not self.enable_parallel:
            # Sequential processing for debugging
            results = [
                process_image_worker(img_path, out_dir, thumb_size)
                for img_path in sorted_paths
            ]
        else:
            # Parallel processing with stuck detection
            try:
                path_to_result = {}
                with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                    # Submit all tasks
                    future_to_path = {
                        executor.submit(process_image_worker, img_path, out_dir, thumb_size): img_path
                        for img_path in sorted_paths
                    }

                    completed_count = 0
                    last_progress_time = time.monotonic()
                    stuck_timeout = DEFAULT_STUCK_THRESHOLD  # seconds without progress = stuck

                    # Collect results with stuck detection
                    pending = set(future_to_path.keys())
                    while pending:
                        # Wait for next completion with short timeout for responsiveness
                        done, pending = wait(pending, timeout=5.0, return_when=FIRST_COMPLETED)

                        if done:
                            # Made progress - reset stuck timer
                            last_progress_time = time.monotonic()

                            for future in done:
                                img_path = future_to_path[future]
                                try:
                                    result = future.result(timeout=1.0)  # Already done, should be instant
                                    path_to_result[img_path] = result
                                except Exception as exc:
                                    LOGGER.error("Unexpected error processing %s: %s", img_path, exc)
                                    path_to_result[img_path] = ImageProcessResult(
                                        path=img_path,
                                        rel_path=str(img_path),
                                        filename=img_path.name,
                                        error=f"Unexpected: {exc}",
                                    )
                                completed_count += 1
                                # Progress logging every 100 images
                                if completed_count % 100 == 0:
                                    LOGGER.info("Processed %d/%d images...", completed_count, len(sorted_paths))
                        else:
                            # No progress in this iteration - check if stuck
                            elapsed = time.monotonic() - last_progress_time
                            if elapsed > stuck_timeout:
                                stuck_count = len(pending)
                                LOGGER.warning(
                                    "No progress for %.0fs - %d workers appear stuck, aborting remaining",
                                    elapsed, stuck_count
                                )
                                # Mark all pending as failed and break out
                                for future in pending:
                                    img_path = future_to_path[future]
                                    path_to_result[img_path] = ImageProcessResult(
                                        path=img_path,
                                        rel_path=str(img_path),
                                        filename=img_path.name,
                                        error=f"Worker stuck - no progress for {elapsed:.0f}s",
                                    )
                                    future.cancel()
                                # Force shutdown executor - kills stuck workers
                                executor.shutdown(wait=False, cancel_futures=True)
                                break
                            else:
                                # Still within timeout, log waiting status
                                LOGGER.debug(
                                    "Waiting for %d pending images (%.0fs since last progress)...",
                                    len(pending), elapsed
                                )

                # Re-sort results to match input order (deterministic)
                results = [path_to_result[img_path] for img_path in sorted_paths]
            except PermissionError as exc:
                LOGGER.warning(
                    "Parallel processing unavailable (%s); falling back to sequential",
                    exc,
                )
                results = [
                    process_image_worker(img_path, out_dir, thumb_size)
                    for img_path in sorted_paths
                ]
                completed_count = len(results)
                if completed_count >= 100:
                    LOGGER.info(
                        "Processed %d/%d images...",
                        min(100, completed_count),
                        len(sorted_paths),
                    )

        # Log statistics
        success_count = sum(1 for r in results if r.error is None)
        error_count = len(results) - success_count
        LOGGER.info("Processed %d images: %d success, %d errors", len(results), success_count, error_count)

        return results

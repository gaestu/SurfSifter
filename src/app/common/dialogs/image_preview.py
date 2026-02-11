"""
Image preview dialog with metadata display.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt, QThread, QUrl, Signal, QSize
from PySide6.QtGui import QDesktopServices, QPixmap
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSplitter,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)


# Maximum file size (in bytes) before requiring confirmation
MAX_IMAGE_AUTO_LOAD_SIZE = 50 * 1024 * 1024  # 50 MB


class ImageLoaderThread(QThread):
    """Async thread for loading full-size images."""

    loaded = Signal(object)  # QPixmap
    error = Signal(str)

    def __init__(self, image_path: Path, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.image_path = image_path

    def run(self) -> None:
        try:
            if not self.image_path.exists():
                self.error.emit("Image file not found on disk.")
                return

            pixmap = QPixmap(str(self.image_path))
            if pixmap.isNull():
                self.error.emit("Failed to load image. File may be corrupted.")
                return

            self.loaded.emit(pixmap)
        except Exception as exc:
            self.error.emit(f"Error loading image: {exc}")


class ImagePreviewDialog(QDialog):
    """
    Dialog for displaying large image preview with metadata.

    Features:
    - Shows cached thumbnail immediately
    - Async loads full-size image on demand
    - Displays all image metadata (filename, path, hashes, EXIF, tags)
    - Shows all discovery sources with provenance
    - Open in external viewer button
    - Copy hash values to clipboard
    """

    def __init__(
        self,
        image_data: Dict[str, Any],
        thumbnail_path: Optional[Path],
        full_image_path: Optional[Path],
        parent: Optional[QWidget] = None,
        *,
        discoveries: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """
        Initialize image preview dialog.

        Args:
            image_data: Image metadata from images table
            thumbnail_path: Path to cached thumbnail
            full_image_path: Path to full-size image
            parent: Parent widget
            discoveries: Optional list of discovery records (from image_discoveries table)
        """
        super().__init__(parent)
        self.image_data = image_data
        self.thumbnail_path = thumbnail_path
        self.full_image_path = full_image_path
        self.discoveries = discoveries or []
        self._loader_thread: Optional[ImageLoaderThread] = None
        self._full_pixmap = None

        self.setWindowTitle("Image Preview")
        self.setMinimumSize(900, 600)
        self.resize(1100, 750)

        self._setup_ui()
        self._load_thumbnail()

    def _setup_ui(self) -> None:
        """Build the dialog UI."""
        layout = QVBoxLayout()

        # Main splitter: image on left, metadata on right
        splitter = QSplitter(Qt.Horizontal)

        # Left side: Image preview area
        image_container = QWidget()
        image_layout = QVBoxLayout()
        image_layout.setContentsMargins(0, 0, 0, 0)

        # Scroll area for image
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setAlignment(Qt.AlignCenter)

        # Image label
        self.image_label = QLabel()
        self.image_label.setAlignment(Qt.AlignCenter)
        self.image_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.image_label.setStyleSheet("background-color: #2d2d2d;")
        self.scroll_area.setWidget(self.image_label)

        image_layout.addWidget(self.scroll_area)

        # Status bar for image loading
        self.status_label = QLabel("Loading thumbnail...")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.status_label.setStyleSheet("color: gray; font-style: italic;")
        image_layout.addWidget(self.status_label)

        # Image action buttons
        image_buttons = QHBoxLayout()

        self.load_full_button = QPushButton("Load Full Size")
        self.load_full_button.clicked.connect(self._load_full_image)
        self.load_full_button.setEnabled(False)
        image_buttons.addWidget(self.load_full_button)

        self.open_external_button = QPushButton("Open in External Viewer")
        self.open_external_button.clicked.connect(self._open_external)
        image_buttons.addWidget(self.open_external_button)

        image_buttons.addStretch()
        image_layout.addLayout(image_buttons)

        image_container.setLayout(image_layout)
        splitter.addWidget(image_container)

        # Right side: Metadata panel
        metadata_container = QWidget()
        metadata_layout = QVBoxLayout()

        metadata_label = QLabel("<b>Image Details</b>")
        metadata_layout.addWidget(metadata_label)

        self.metadata_display = QTextEdit()
        self.metadata_display.setReadOnly(True)
        self._populate_metadata()
        metadata_layout.addWidget(self.metadata_display)

        # Hash copy buttons
        hash_buttons = QHBoxLayout()

        md5_button = QPushButton("Copy MD5")
        md5_button.clicked.connect(lambda: self._copy_to_clipboard("md5"))
        hash_buttons.addWidget(md5_button)

        sha256_button = QPushButton("Copy SHA256")
        sha256_button.clicked.connect(lambda: self._copy_to_clipboard("sha256"))
        hash_buttons.addWidget(sha256_button)

        phash_button = QPushButton("Copy pHash")
        phash_button.clicked.connect(lambda: self._copy_to_clipboard("phash"))
        hash_buttons.addWidget(phash_button)

        metadata_layout.addLayout(hash_buttons)

        metadata_container.setLayout(metadata_layout)
        splitter.addWidget(metadata_container)

        # Set splitter sizes (70% image, 30% metadata)
        splitter.setSizes([700, 300])

        layout.addWidget(splitter)

        # Dialog buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch()

        close_button = QPushButton("Close")
        close_button.clicked.connect(self.accept)
        button_layout.addWidget(close_button)

        layout.addLayout(button_layout)

        self.setLayout(layout)

    def _populate_metadata(self) -> None:
        """Fill metadata display with image information."""
        lines = []

        # Aggregate all discovery data
        all_sources = []
        all_offsets = []
        all_fs_paths = []
        all_cache_urls = []
        all_cache_keys = []
        all_fs_timestamps = []  # (path, mtime, crtime, atime, inode)

        if self.discoveries:
            for disc in self.discoveries:
                src = disc.get('discovered_by', 'unknown')
                if src and src not in all_sources:
                    all_sources.append(src)

                # Collect offsets
                offset = disc.get('carved_offset_bytes')
                if offset is not None and offset not in all_offsets:
                    all_offsets.append(offset)

                # Collect filesystem paths with timestamps
                fs_path = disc.get('fs_path')
                if fs_path and fs_path not in all_fs_paths:
                    all_fs_paths.append(fs_path)
                    all_fs_timestamps.append({
                        'path': fs_path,
                        'mtime': disc.get('fs_mtime'),
                        'crtime': disc.get('fs_crtime'),
                        'atime': disc.get('fs_atime'),
                        'inode': disc.get('fs_inode'),
                    })

                # Collect cache URLs
                cache_url = disc.get('cache_url')
                if cache_url and cache_url not in all_cache_urls:
                    all_cache_urls.append(cache_url)

                # Collect cache keys
                cache_key = disc.get('cache_key')
                if cache_key and cache_key not in all_cache_keys:
                    all_cache_keys.append(cache_key)

        # Fallback to legacy field or view columns if no discoveries
        if not all_sources:
            # Try sources from v_image_sources view first
            sources_str = self.image_data.get('sources', '')
            if sources_str:
                all_sources.extend(s.strip() for s in sources_str.split(',') if s.strip())
            else:
                first_source = self.image_data.get('first_discovered_by') or self.image_data.get('discovered_by', '')
                if first_source:
                    all_sources.append(first_source)

        # Also get fs_path from view if not in discoveries
        if not all_fs_paths:
            view_fs_path = self.image_data.get('fs_path')
            if view_fs_path:
                all_fs_paths.append(view_fs_path)
                all_fs_timestamps.append({'path': view_fs_path, 'mtime': None, 'crtime': None, 'atime': None, 'inode': None})

        # === HEADER SECTION ===
        lines.append(f"<b>Filename:</b> {self.image_data.get('filename', 'N/A')}")
        lines.append("")

        # === DISCOVERED BY (with source count) ===
        source_count = len(all_sources)
        if all_sources:
            sources_formatted = [self._format_source(s) for s in all_sources]
            if source_count > 1:
                lines.append(f"<b>Discovered by ({source_count} sources):</b>")
            else:
                lines.append(f"<b>Discovered by:</b>")
            for src_fmt in sources_formatted:
                lines.append(f"&nbsp;&nbsp;• {src_fmt}")
        else:
            lines.append(f"<b>Discovered by:</b> Unknown")
        lines.append("")

        # === OFFSETS (if any) ===
        if all_offsets:
            lines.append(f"<b>Offset:</b>")
            for offset in sorted(all_offsets):
                lines.append(f"&nbsp;&nbsp;• {offset:,} bytes (0x{offset:X})")
            lines.append("")

        # === FILESYSTEM PATHS (if any) ===
        if all_fs_paths:
            lines.append(f"<b>Filesystem Path:</b>")
            for ts_info in all_fs_timestamps:
                path = ts_info['path']
                lines.append(f"&nbsp;&nbsp;📁 <code>{path}</code>")
                # Show timestamps inline if available
                ts_parts = []
                if ts_info.get('mtime'):
                    ts_parts.append(f"Modified: {ts_info['mtime']}")
                if ts_info.get('crtime'):
                    ts_parts.append(f"Created: {ts_info['crtime']}")
                if ts_info.get('atime'):
                    ts_parts.append(f"Accessed: {ts_info['atime']}")
                if ts_info.get('inode') is not None:
                    ts_parts.append(f"Inode: {ts_info['inode']}")
                if ts_parts:
                    lines.append(f"&nbsp;&nbsp;&nbsp;&nbsp;<small>{' | '.join(ts_parts)}</small>")
            lines.append("")

        # === CACHE URLs (if any) ===
        if all_cache_urls:
            lines.append(f"<b>Cache URL:</b>")
            for url in all_cache_urls:
                url_display = url[:100] + '...' if len(url) > 100 else url
                lines.append(f"&nbsp;&nbsp;🌐 <code>{url_display}</code>")
            lines.append("")

        # === CACHE KEYS (if different from URLs) ===
        cache_keys_unique = [k for k in all_cache_keys if k not in all_cache_urls]
        if cache_keys_unique:
            lines.append(f"<b>Cache Key:</b>")
            for key in cache_keys_unique:
                key_display = key[:80] + '...' if len(key) > 80 else key
                lines.append(f"&nbsp;&nbsp;🔑 <code>{key_display}</code>")
            lines.append("")

        # === OUTPUT PATH (where the extracted file is stored) ===
        rel_path = self.image_data.get('rel_path')
        if rel_path:
            lines.append(f"<b>Output Path:</b> <code>{rel_path}</code>")
            lines.append("")

        # === HASHES ===
        lines.append(f"<b>MD5:</b> <code>{self.image_data.get('md5', 'N/A')}</code>")
        sha256 = self.image_data.get('sha256', 'N/A')
        # Break long SHA256 for display
        if sha256 and len(sha256) > 40:
            sha256_display = f"{sha256[:32]}<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{sha256[32:]}"
        else:
            sha256_display = sha256
        lines.append(f"<b>SHA256:</b> <code>{sha256_display}</code>")
        phash = self.image_data.get('phash')
        lines.append(f"<b>pHash:</b> <code>{phash if phash else 'N/A'}</code>")
        lines.append("")
        lines.append("")

        # Size info
        size_bytes = self.image_data.get('size_bytes')
        if size_bytes:
            if size_bytes < 1024:
                size_str = f"{size_bytes} bytes"
            elif size_bytes < 1024 * 1024:
                size_str = f"{size_bytes / 1024:.1f} KB"
            else:
                size_str = f"{size_bytes / (1024 * 1024):.1f} MB"
            lines.append(f"<b>{'Size:'}</b> {size_str}")

        # Timestamp and tags
        lines.append(f"<b>{'Timestamp:'}</b> {self.image_data.get('ts_utc', 'N/A')}")
        tags = self.image_data.get('tags', '')
        lines.append(f"<b>{'Tags:'}</b> {tags if tags else 'None'}")
        lines.append("")

        # EXIF data
        exif_json = self.image_data.get('exif_json')
        if exif_json:
            lines.append(f"<b>{'EXIF Data:'}</b>")
            try:
                exif = json.loads(exif_json)
                for key, value in exif.items():
                    if value is not None:
                        lines.append(f"&nbsp;&nbsp;{key}: {value}")
            except json.JSONDecodeError:
                lines.append(f"&nbsp;&nbsp;{'(invalid EXIF data)'}")
        else:
            lines.append(f"<b>{'EXIF Data:'}</b> {'None'}")

        # Notes
        notes = self.image_data.get('notes', '')
        if notes:
            lines.append("")
            lines.append(f"<b>{'Notes:'}</b>")
            lines.append(notes)

        self.metadata_display.setHtml("<br/>".join(lines))

    def _get_source_icon(self, source: str) -> str:
        """Get icon for discovery source type."""
        if not source:
            return "❓"
        # Added browser cache/storage icons
        return {
            # Carving tools
            "foremost_carver": "🔨",
            "scalpel": "✂️",
            "bulk_extractor:images": "🔍",
            "bulk_extractor_images": "🔍",
            "image_carving": "🖼️",
            # Filesystem
            "filesystem_images": "📂",
            # Browser cache/storage
            "cache_simple": "🌐",
            "cache_blockfile": "🌐",
            "cache_firefox": "🦊",
            "browser_storage_indexeddb": "💾",
            "safari": "🧭",
            "browser_cache": "🌐",  # Legacy
        }.get(source, "📁")

    def _format_source(self, source: str) -> str:
        """Format source with icon."""
        if not source:
            return "Unknown"
        icon = self._get_source_icon(source)
        return f"{icon} {source}"

    def _load_thumbnail(self) -> None:
        """Load and display cached thumbnail."""
        if self.thumbnail_path and self.thumbnail_path.exists():
            pixmap = QPixmap(str(self.thumbnail_path))
            if not pixmap.isNull():
                # Scale up thumbnail for display
                scaled = pixmap.scaled(
                    QSize(400, 400),
                    Qt.KeepAspectRatio,
                    Qt.SmoothTransformation,
                )
                self.image_label.setPixmap(scaled)
                self.status_label.setText(
                    "Showing thumbnail. Click 'Load Full Size' for original image."
                )
                self.load_full_button.setEnabled(self.full_image_path is not None)
                return

        # No thumbnail available
        self.image_label.setText("No preview available")
        self.status_label.setText("Thumbnail not found.")
        self.load_full_button.setEnabled(self.full_image_path is not None)

    def _load_full_image(self) -> None:
        """Start async loading of full-size image."""
        if not self.full_image_path:
            QMessageBox.warning(
                self,
                "Image Not Found",
                "The original image file path is not available.",
            )
            return

        if not self.full_image_path.exists():
            QMessageBox.warning(
                self,
                "Image Not Found",
                "The original image file no longer exists on disk.",
            )
            return

        # Check file size
        try:
            file_size = self.full_image_path.stat().st_size
            if file_size > MAX_IMAGE_AUTO_LOAD_SIZE:
                size_mb = file_size / (1024 * 1024)
                reply = QMessageBox.question(
                    self,
                    "Large Image",
                    f"This image is {size_mb:.1f} MB. Loading large images may "
                    "take time and memory.\n\nContinue?",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No,
                )
                if reply != QMessageBox.Yes:
                    return
        except OSError:
            pass  # Continue anyway

        self.load_full_button.setEnabled(False)
        self.status_label.setText("Loading full-size image...")

        self._loader_thread = ImageLoaderThread(self.full_image_path, self)
        self._loader_thread.loaded.connect(self._on_image_loaded)
        self._loader_thread.error.connect(self._on_image_error)
        self._loader_thread.start()

    def _on_image_loaded(self, pixmap) -> None:
        """Handle successful image load."""
        self._full_pixmap = pixmap

        # Scale to fit scroll area while maintaining aspect ratio
        available_size = self.scroll_area.size()
        scaled = pixmap.scaled(
            available_size,
            Qt.KeepAspectRatio,
            Qt.SmoothTransformation,
        )
        self.image_label.setPixmap(scaled)

        # Update status
        self.status_label.setText(
            f"Full image loaded ({pixmap.width()}×{pixmap.height()} pixels)"
        )
        self.load_full_button.setText("Reload Full Size")
        self.load_full_button.setEnabled(True)

    def _on_image_error(self, message: str) -> None:
        """Handle image load error."""
        self.status_label.setText(f"Error: {message}")
        self.load_full_button.setEnabled(True)

        QMessageBox.warning(
            self,
            "Image Load Error",
            message,
        )

    def _open_external(self) -> None:
        """Open image in system default viewer."""
        if not self.full_image_path:
            QMessageBox.warning(
                self,
                "Image Not Found",
                "The original image file path is not available.",
            )
            return

        if not self.full_image_path.exists():
            QMessageBox.warning(
                self,
                "Image Not Found",
                "The original image file no longer exists on disk.",
            )
            return

        QDesktopServices.openUrl(QUrl.fromLocalFile(str(self.full_image_path)))

    def _copy_to_clipboard(self, hash_type: str) -> None:
        """Copy hash value to clipboard."""
        value = self.image_data.get(hash_type, "")
        if value:
            QApplication.clipboard().setText(value)
            self.status_label.setText(
                f"{hash_type.upper()} copied to clipboard."
            )
        else:
            self.status_label.setText(
                f"No {hash_type.upper()} value available."
            )

    def closeEvent(self, event) -> None:
        """Clean up loader thread on close."""
        if self._loader_thread and self._loader_thread.isRunning():
            self._loader_thread.quit()
            self._loader_thread.wait(1000)
        super().closeEvent(event)

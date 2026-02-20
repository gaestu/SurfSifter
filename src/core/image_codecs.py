from __future__ import annotations

from threading import Lock

from core.logging import get_logger


_LOGGER = get_logger("core.image_codecs")
_INIT_LOCK = Lock()
_HEIF_REGISTERED = False
_HEIF_INIT_DONE = False


def ensure_pillow_heif_registered() -> bool:
    """Register pillow-heif opener once so Pillow can decode HEIC/HEIF."""
    global _HEIF_REGISTERED, _HEIF_INIT_DONE

    if _HEIF_INIT_DONE:
        return _HEIF_REGISTERED

    with _INIT_LOCK:
        if _HEIF_INIT_DONE:
            return _HEIF_REGISTERED

        try:
            from pillow_heif import register_heif_opener
        except ImportError:
            _LOGGER.debug("pillow-heif is not installed; HEIC/HEIF decoding unavailable")
            _HEIF_REGISTERED = False
            _HEIF_INIT_DONE = True
            return False
        except Exception as exc:  # pragma: no cover - defensive
            _LOGGER.warning("Failed to import pillow-heif: %s", exc)
            _HEIF_REGISTERED = False
            _HEIF_INIT_DONE = True
            return False

        try:
            register_heif_opener()
            _HEIF_REGISTERED = True
            _LOGGER.debug("Registered pillow-heif opener for HEIC/HEIF decoding")
        except Exception as exc:
            _LOGGER.warning("Failed to register pillow-heif opener: %s", exc)
            _HEIF_REGISTERED = False
        finally:
            _HEIF_INIT_DONE = True

        return _HEIF_REGISTERED

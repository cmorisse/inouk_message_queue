# CLI commands for IMQ Workers v3
try:
    from .imq_worker import IMQWorker
    from .imq_test import IMQTest
    from .imq_dump import IMQDump
except ImportError as e:
    # Log import errors but don't fail module loading
    import logging
    logging.getLogger(__name__).warning(f"Failed to import CLI components: {e}")
# inbuilt
import sys
import traceback
import logging


class BaseException(Exception):
    """
    Unified base exception for all SDMF pipeline errors.
    Automatically logs a clean, human-readable error block.
    """

    def __init__(
        self,
        message=None,
        details=None,
        context=None,
        original_exception=None,
        log=True,
    ):
        super().__init__(message)

        self.message = message or self.__class__.__name__
        self.details = details
        self.context = context or {}
        self.original_exception = original_exception

        # Capture traceback safely
        exc_type, exc_value, exc_tb = sys.exc_info()
        self.exc_type = exc_type.__name__ if exc_type else None
        self.exc_value = str(exc_value) if exc_value else None
        self.full_traceback = (
            "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
            if exc_type
            else None
        )

        self.logger = logging.getLogger(__name__)

        # Log once, cleanly
        if log:
            self.logger.error(self.to_pretty_text())

    # --------------------------------------------------
    # Human-readable output (for logs / console)
    # --------------------------------------------------
    def __str__(self):
        return self.to_pretty_text()

    def to_pretty_text(self):
        return f"""
        ==================== SDMF ERROR ====================

        Error Type:
        {self.__class__.__name__}

        Message:
        {self.message}

        -------------------- DETAILS --------------------
        {self._format_block(self.details)}

        -------------------- CONTEXT --------------------
        {self._format_block(self.context)}

        ------------- ORIGINAL EXCEPTION ---------------
        {self._format_block(repr(self.original_exception) if self.original_exception else None)}

        ------------------ STACK TRACE ------------------
        {self._format_block(self.full_traceback)}

        =================================================
        """.strip()

    # --------------------------------------------------
    # Structured output (for MLflow / REST / JSON)
    # --------------------------------------------------
    def to_dict(self):
        """Structured error payload for APIs, MLflow, or persistence."""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "details": self.details,
            "context": self.context,
            "original_exception": repr(self.original_exception)
            if self.original_exception
            else None,
            "exception_type": self.exc_type,
            "exception_message": self.exc_value,
            "traceback": self.full_traceback,
        }

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------
    @staticmethod
    def _format_block(value):
        if value in (None, "", {}, []):
            return "N/A"
        return value

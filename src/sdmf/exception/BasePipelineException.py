import sys
import traceback
import logging

class BasePipelineException(Exception):
    """
    Unified base exception for all pipeline errors.
    Automatically logs in pretty console format.
    """

    def __init__(self, message=None, details=None, context=None, original_exception=None):
        super().__init__(message)

        self.message = message or self.__class__.__name__
        self.details = details
        self.context = context or {}
        self.original_exception = original_exception
        self.traceback = details or None
        self.logger = logging.getLogger(__name__)
        
        # Capture exception info if available
        exc_type, exc_value, _ = sys.exc_info()
        self.exc_type = exc_type.__name__ if exc_type else None
        self.exc_value = str(exc_value) if exc_value else None
        self.full_error_info = ''.join(traceback.format_exception(*sys.exc_info())) if sys.exc_info()[0] else None


        error_msg = self.__str__()
        self.logger.error(f"{error_msg}, Full Message: {self.to_dict()}")

    def __str__(self):
        parts = [f"[{self.__class__.__name__}] {self.message}"]
        
        if self.full_error_info:
            parts.append(f"\nStack Trace:\n{self.full_error_info}")
        if self.details:
            parts.append(f"Details: {self.details}")
        if self.context:
            parts.append(f"Context: {self.context}")
        if self.original_exception:
            parts.append(f"Caused by: {repr(self.original_exception)}")
        if self.exc_type:
            parts.append(f"Exception Type: {self.exc_type}")
        if self.exc_value:
            parts.append(f"Exception Message: {self.exc_value}")
        
        return " | ".join(parts)

    def to_dict(self):
        """Optional structured output if needed in MLflow or REST."""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "details": self.details,
            "context": self.context,
            "original_exception": repr(self.original_exception),
            "traceback": self.traceback,
        }


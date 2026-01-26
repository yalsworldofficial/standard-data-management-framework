import logging

logger = logging.getLogger(__name__)

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

        try:
            error_msg = self.__str__()
            logger.error(f"{error_msg}, Full Message: {self.to_dict()}")
        except:
            pass

    def __str__(self):
        base = f"[{self.__class__.__name__}] {self.message}"

        if self.details:
            base += f" | Details: {self.details}"

        if self.context:
            base += f" | Context: {self.context}"

        if self.original_exception:
            base += f" | Caused by: {repr(self.original_exception)}"

        return base

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


from sdmf.exception.BaseException import BaseException

class ValidationError(BaseException):
    def __init__(self, message=None, details=None, original_exception=None):
        super().__init__(
            message or "Validation Error",
            details=details,
            original_exception=original_exception
        )
from sdmf.exception.BasePipelineException import BasePipelineException

class SystemError(BasePipelineException):
    def __init__(self, message=None, details=None, original_exception=None):
        super().__init__(
            message or "System Error",
            details=details,
            original_exception=original_exception
        )
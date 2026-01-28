from sdmf.exception.BasePipelineException import BasePipelineException

class DataSpecValidationError(BasePipelineException):
    def __init__(self, message=None, details=None, original_exception=None):
        super().__init__(
            message or "Failed to Validate.",
            details=details,
            original_exception=original_exception
        )
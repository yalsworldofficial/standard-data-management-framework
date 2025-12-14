from sdmf.exception.BasePipelineException import BasePipelineException

class DataQualityError(BasePipelineException):
    def __init__(self, message=None, details=None, original_exception=None):
        super().__init__(
            message or "DataQualityError",
            details=details,
            original_exception=original_exception
        )
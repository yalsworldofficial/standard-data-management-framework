from sdmf.exception.BasePipelineException import BasePipelineException

class ResultGenerationException(BasePipelineException):
    def __init__(self, message: str,original_exception, details=None):
        super().__init__(message)
        self.message = message
        self.details = details
        self.original_exception = original_exception
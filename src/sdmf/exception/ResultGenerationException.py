from sdmf.exception.BasePipelineException import BasePipelineException

class ResultGenerationException(BasePipelineException):
    def __init__(self, message: str, original_exception, details: str):
        super().__init__(message)
        self.message = message
        self.original_exception = original_exception
        self.details = details
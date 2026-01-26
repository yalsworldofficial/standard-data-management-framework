from sdmf.exception.BasePipelineException import BasePipelineException

class DataLoadException(BasePipelineException):
    def __init__(self, message: str, load_type: str, original_exception, details:str):
        super().__init__(message)
        self.message = message
        self.load_type = load_type
        self.original_exception = original_exception
        self.details = details
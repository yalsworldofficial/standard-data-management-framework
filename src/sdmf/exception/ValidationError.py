from sdmf.exception.BasePipelineException import BasePipelineException

class ValidationError(BasePipelineException):
    def __init__(self, message: str, rule_name: str, original_exception):
        super().__init__(message)
        self.message = message
        self.rule_name = rule_name

        self.original_exception = original_exception
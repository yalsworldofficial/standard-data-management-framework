from sdmf.exception.BasePipelineException import BasePipelineException

class DataSpecRuleExecutionError(BasePipelineException):
    def __init__(self, message=None, details=None, original_exception=None):
        super().__init__(
            message or "DataSpecRuleExecutionError",
            details=details,
            original_exception=original_exception
        )
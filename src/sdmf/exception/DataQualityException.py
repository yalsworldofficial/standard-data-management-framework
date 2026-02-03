from sdmf.exception.BaseException import BaseException

class DataQualityException(BaseException):
    def __init__(self, message=None, details=None, original_exception=None):
        super().__init__(
            message or "Data Quality Exception",
            details=details,
            original_exception=original_exception
        )
        
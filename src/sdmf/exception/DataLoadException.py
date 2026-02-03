from sdmf.exception.BaseException import BaseException

class DataLoadException(BaseException):
    def __init__(self, message=None, details=None, original_exception=None):
        super().__init__(
            message or "Data Load Exception",
            details=details,
            original_exception=original_exception
        )

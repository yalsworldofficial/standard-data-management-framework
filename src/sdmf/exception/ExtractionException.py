from sdmf.exception.BaseException import BaseException

class ExtractionException(BaseException):
    def __init__(self, message=None, details=None, original_exception=None):
        super().__init__(
            message or "Extraction Exception",
            details=details,
            original_exception=original_exception
        )
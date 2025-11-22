from sdmf.core.BasePipelineException import BasePipelineException

class KeyVaultSecretFetchError(BasePipelineException):
    def __init__(self, message=None, details=None, original_exception=None):
        super().__init__(
            message or "Failed to fetch secrets from Azure Key Vault.",
            details=details,
            original_exception=original_exception
        )
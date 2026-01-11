from abc import ABC, abstractmethod

from sdmf.validation.ValidationContext import ValidationContext

class ValidationRule(ABC):
    name: str

    @abstractmethod
    def validate(self, context: ValidationContext):
        """Raise ValidationError on failure"""
        pass

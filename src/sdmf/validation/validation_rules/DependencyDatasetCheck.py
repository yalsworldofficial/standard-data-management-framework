from sdmf.validation.ValidationRule import ValidationRule
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.exception.ValidationError import ValidationError

class DependencyDatasetCheck(ValidationRule):
    name = "Dependency Dataset Check"

    def validate(self, context: ValidationContext):
        if context.data is None:
            raise ValidationError(
                message="JSON has not been parsed yet",
                original_exception=None,
                rule_name=self.name
            )

        if "comprehensive_checks" not in context.data:
            return  # nothing to validate

        for idx, check in enumerate(context.data["comprehensive_checks"]):
            deps = check.get("dependency_dataset", [])

            if not isinstance(deps, list):
                raise ValidationError(
                    f"comprehensive_checks[{idx}].dependency_dataset must be a list",
                    original_exception=None,
                    rule_name=self.name
                )

            for dep in deps:
                if not context.spark.catalog.tableExists(dep):
                    raise ValidationError(
                        message=(
                            f"comprehensive_checks[{idx}].dependency_dataset "
                            f"table does not exist: '{dep}'"
                        ),
                        original_exception=None,
                        rule_name=self.name
                    )
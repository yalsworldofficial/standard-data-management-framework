# inbuilt
import logging

# external
from pyspark.sql import SparkSession
import pandas as pd

# internal
from sdmf.validation.Validator import Validator
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.validation.validation_rules.ValidateMasterSpecs import ValidateMasterSpecs
from sdmf.validation.validation_rules.EnforceMasterSpecsStructure import EnforceMasterSpecsStructure
from sdmf.validation.validation_rules.ValidateFeedSpecsJSON import ValidateFeedSpecsJSON
from sdmf.validation.validation_rules.PrimaryKey import PrimaryKey
from sdmf.validation.validation_rules.ColumnExistsInSelection import ColumnExistsInSelection
from sdmf.validation.validation_rules.EnforceStandardChecks import EnforceStandardChecks
from sdmf.validation.validation_rules.StandardCheckStructureCheck import StandardCheckStructureCheck
from sdmf.validation.validation_rules.ComprehensiveChecksDependencyDatasetCheck import ComprehensiveChecksDependencyDatasetCheck
from sdmf.validation.validation_rules.PartitionKeysCheck import PartitionKeysCheck
from sdmf.validation.validation_rules.CompositeKeysCheck import CompositeKeysCheck

class SystemLaunchValidator():

    def __init__(self, file_hunt_path: str, spark: SparkSession) -> None:
        self.logger = logging.getLogger(__name__)
        self.spark = spark

        self.context = ValidationContext(
            spark=spark,
            file_hunt_path = file_hunt_path
        )

    def __init_rules(self):
        self.rules = [
            ValidateMasterSpecs(),
            EnforceMasterSpecsStructure(),
            ValidateFeedSpecsJSON(),
            PrimaryKey(),
            ColumnExistsInSelection(),
            EnforceStandardChecks(),
            StandardCheckStructureCheck(),
            ComprehensiveChecksDependencyDatasetCheck(),
            PartitionKeysCheck(),
            CompositeKeysCheck()
        ]

    def run(self):
        self.__init_rules()
        validator = Validator(self.rules, fail_fast=True)
        return validator.validate(self.context)

    def get_validated_master_specs(self):
        return self.context.get_master_specs()


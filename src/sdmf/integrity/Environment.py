import os
import logging
import pandas as pd
from typing import Dict, Tuple, List, Type

from sdmf.validation.ValidateSource import ValidateSource
from sdmf.validation.ValidateBronze import ValidateBronze
from sdmf.validation.ValidateSilver import ValidateSilver
from sdmf.validation.ValidateGold import ValidateGold

from sdmf.exception.EnvironmentPreparationError import EnvironmentPreparationError


class Environment:
    """
    Loads, validates, and writes Data Specs for all layers (Source, Bronze, Silver, Gold).
    """

    SCHEMAS = [
        'bronze',
        'silver',
        'gold',
        'analytics',
        'cleanup',
        'config'
    ]

    EXCEL_SHEETS = {
        "source": "source",
        "bronze": "bronze",
        "silver": "silver",
        "gold": "gold",
    }

    VALIDATORS: List[Tuple[str, Type, str]] = [
        ("Source Layer Validation",  ValidateSource, "source"),
        ("Bronze Layer Validation",  ValidateBronze, "bronze"),
        ("Silver Layer Validation",  ValidateSilver, "silver"),
        ("Gold Layer Validation",    ValidateGold, "gold"),
    ]

    def __init__(self, unity_catalog: str, excel_file_path: str):
        self.unity_catalog = unity_catalog
        self.config_schema_name = self.SCHEMAS[5]
        self.excel_file_path = excel_file_path

        self.logger = logging.getLogger(__name__)
        self.dataframes: Dict[str, pd.DataFrame] = {}

        # Precompute UC table paths
        self.table_paths = {
            layer: f"`{unity_catalog}`.`{self.config_schema_name}`.`t_sdmf_data_specs_{layer}`"
            for layer in self.EXCEL_SHEETS.keys()
        }


        self.feed_logs_table = f"`{unity_catalog}`.`{self.config_schema_name}`.`t_sdmf_data_specs_feed_logs`"



    

    # ----------------------------------------------------------
    # MAIN ENTRY POINT
    # ----------------------------------------------------------
    def prepare_environment(self) -> bool:
        """
        Full pipeline: Load → Validate → Write.
        Returns True if successful.
        """
        try:
            if not self.__load_data_specs():
                raise EnvironmentPreparationError("Data specs were not loaded.")

            if not self.__validate_data_specs():
                self.logger.error("Data Specs Validation Failed.")
                raise EnvironmentPreparationError("Something went wrong validating data specs.")
            
            self.logger.info("Setting up Unity Catalog with required schemas...")
            self.__prepare_uc_schemas()

            self.logger.info("Writing feed specs in config database...")
            self.__write_data_specs()

            self.logger.info("Creating status table..")
            self.__create_final_feed_status_table()


            self.logger.info("Environment preparation completed successfully.")
            return True

        except Exception as e:
            raise EnvironmentPreparationError(
                message="Failed to prepare environment.",
                original_exception=e
            )
        
    def __prepare_uc_schemas(self):
        try:
            for schema in self.SCHEMAS:
                schema_query = f"CREATE SCHEMA IF NOT EXIST `{self.unity_catalog}`.`{schema}`;"
        except Exception as e:
            raise EnvironmentPreparationError(
                message="Error while preparing UC catalog",
                original_exception=e
            )


            # complete this

    # ----------------------------------------------------------
    # LOAD
    # ----------------------------------------------------------
    def __load_data_specs(self) -> bool:
        """
        Loads all required Data Spec sheets from Excel.
        """
        if not os.path.exists(self.excel_file_path):
            raise FileNotFoundError(f"Excel file not found: {self.excel_file_path}")

        try:
            for key, sheet in self.EXCEL_SHEETS.items():
                df = self.__read_excel_sheet(self.excel_file_path, sheet)
                self.dataframes[key] = df

            self.logger.info("All data specs successfully loaded from Excel.")
            return True

        except Exception as e:
            raise EnvironmentPreparationError(
                message="Error while loading data specs from Excel.",
                original_exception=e
            )
        
    # ---- Helper to assign required columns ----
    def __prepare(self, df: pd.DataFrame, layer_name: str):
        df = df.copy()

        # Required output columns
        df = df[["id", "feed_name"]]  # select only mandatory columns

        df["layer"] = layer_name

        # These metadata fields start empty (None)
        df["solution_name"] = df.get("solution_name", None)
        df["last_ingestion_timestamp"] = None
        df["run_start_timestamp"] = None
        df["run_end_timestamp"] = None
        df["error_if_any"] = None
        df["error_message"] = None

        return df
        
    def __create_final_feed_status_table(self):
        try:
            self.logger.info("Creating Feed Status table for all layers")

            # ---- Extract DataFrames ----
            source_df = self.dataframes['source'].copy()
            bronze_df = self.dataframes['bronze'].copy()
            silver_df = self.dataframes['silver'].copy()
            gold_df = self.dataframes['gold'].copy()

            # ---- Populate solution_name for chained dependencies ----
            bronze_df["solution_name"] = bronze_df["dependency_key"].map(
                source_df.set_index("id")["solution_name"]
            )

            silver_df["solution_name"] = silver_df["dependency_key"].map(
                bronze_df.set_index("id")["solution_name"]
            )

            gold_df["solution_name"] = gold_df["dependency_key"].map(
                silver_df.set_index("id")["solution_name"]
            )

            source_final = self.__prepare(source_df, "source")
            bronze_final = self.__prepare(bronze_df, "bronze")
            silver_final = self.__prepare(silver_df, "silver")
            gold_final = self.__prepare(gold_df, "gold")

            final_df = pd.concat(
                [source_final, bronze_final, silver_final, gold_final],
                ignore_index=True
            )

            final_df = final_df.insert(0, "surrogate_key", range(1, len(final_df) + 1))


            # return final_df
        except Exception as e:
            raise EnvironmentPreparationError(
                message="Error while creating final analytics table.",
                original_exception=e
            )


    # ----------------------------------------------------------
    # VALIDATE
    # ----------------------------------------------------------
    def __validate_data_specs(self) -> bool:
        """
        Validates each Data Spec using its respective validator.
        """
        all_passed = True

        for label, validator_class, key in self.VALIDATORS:
            df = self.dataframes.get(key)

            self.logger.info(f"Validating {label}...")

            try:
                validator = validator_class(df)
                valid = validator.validate()

                if valid:
                    self.logger.info(f"{label}: PASS")
                else:
                    self.logger.error(f"{label}: FAIL")
                    all_passed = False

            except Exception as e:
                self.logger.error(f"{label} crashed during validation.")
                raise EnvironmentPreparationError(
                    message=f"{label} crashed during validation.",
                    original_exception=e
                )

        self.logger.info(f"Overall Validation: {all_passed}")
        return all_passed

    # ----------------------------------------------------------
    # WRITE
    # ----------------------------------------------------------
    def __write_data_specs(self):
        """
        Writes the validated dataframes into Unity Catalog tables.
        (Implementation placeholder — you’ll implement this on Databricks)
        """
        # Example pseudocode:
        # for key, df in self.dataframes.items():
        #     df.write.mode("overwrite").saveAsTable(self.table_paths[key])
        pass

    # ----------------------------------------------------------
    # UTILS
    # ----------------------------------------------------------
    def __read_excel_sheet(self, file_path: str, sheet_name: str) -> pd.DataFrame:
        """
        Reads an Excel sheet into a pandas DataFrame with dtype inference.
        """
        try:
            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                header=0,
                engine="openpyxl"
            )
            return df.convert_dtypes()

        except Exception as e:
            raise RuntimeError(
                f"Failed to read Excel sheet '{sheet_name}': {str(e)}"
            )

# inbuilt
import os
import time
import logging
import configparser
from datetime import datetime
from io import BytesIO
from dataclasses import asdict
import shutil

# external
from openpyxl import Workbook
import pandas as pd

#internal
from sdmf.exception.ResultGenerationException import ResultGenerationException
from sdmf.data_movement_framework.data_class.LoadResult import LoadResult

class ResultGenerator():

    def __init__(
            self, 
            payload: list[dict], 
            file_hunt_path: str, 
            run_id: str, 
            config: configparser.ConfigParser, 
            system_report: pd.DataFrame, 
            load_results: list[LoadResult]
        ) -> None:
        self.payload = payload
        self.file_hunt_path = file_hunt_path
        self.run_id = run_id
        self.final_feed_status = []
        self.standard_results = []
        self.comprehensive_results = []
        self.sheets = []
        self.save_path = None
        self.logger = logging.getLogger(__name__)
        self.config = config

        self.dashboard_cols = [
            "feed_id",
            "feed_name",
            "system_name",
            "subsystem_name",
            "category",
            "sub_category",
            "standard_checks_passed",
            "comprehensive_pre_load_passed",
            "comprehensive_post_load_passed",
            "can_ingest",
        ]

        self.sheets.append(
            {
                "sheet_name": "System Readiness",
                "df":system_report
            }
        )
        self.sheets.append(
            {
                "sheet_name": "Load Report",
                "df": pd.DataFrame([asdict(r) for r in load_results]).sort_values(by="feed_id")
            }
        )

    def run(self):
        try:
            self.logger.info(f"Segregating results...")
            self.__segregate_results()
            self.logger.info(f"Creating sheets...")
            self.__generate_full_feed_status()
            self.logger.info(f"Generating results...")
            self.__generate_standard_results()
            self.logger.info(f"Generate comprehensive results...")
            self.__generate_comprehensive_results()
            self.logger.info(f"Generating dashboard...")
            self.__generate_dashboard()
            self.logger.info(f"Generating final file...")
            self.__generate_result_file()
        except Exception as e:
            raise ResultGenerationException(
                "Something went wrong while running result generator.",
                original_exception=e         
            )

    def __sheet_generator(self, df: pd.DataFrame, sheet_name: str):
        df = df.copy()   # ← THIS is mandatory
        df["run_id"] = self.run_id

        self.sheets.append(
            {
                "sheet_name": sheet_name,
                "df": df
            }
        )

    def __generate_full_feed_status(self):
        final_feed_status_df = pd.DataFrame(self.final_feed_status)
        col = "feed_id"
        final_feed_status_df.insert(0, col, final_feed_status_df.pop(col))
        final_feed_status_df = final_feed_status_df.sort_values(by=col)
        self.__sheet_generator(final_feed_status_df, "Feed Status (B-G)")

    def __generate_standard_results(self):
        df = pd.DataFrame(self.standard_results)
        df = df.explode("standard_checks_result").reset_index(drop=True)
        df = pd.concat(
            [
                df.drop(columns=["standard_checks_result"]),
                df["standard_checks_result"].apply(pd.Series),
            ],
            axis=1,
        )
        df.insert(0, "check_number", range(1, len(df) + 1))
        self.__sheet_generator(df, "Standard Check Result")

    def __generate_comprehensive_results(self):
        if len(self.comprehensive_results) != 0:
            df = pd.DataFrame(self.comprehensive_results)
            df = df.explode("comprehensive_results").reset_index(drop=True)
            df = pd.concat(
                [
                    df.drop(columns=["comprehensive_results"]),
                    df["comprehensive_results"].apply(pd.Series),
                ],
                axis=1,
            )
            df.insert(0, "check_number", range(1, len(df) + 1))
            self.__sheet_generator(df, "Comprehensive Check Result")

    
    def __generate_dashboard(self):
        final_feed_status_df = pd.DataFrame(self.final_feed_status)
        dashboard_df = final_feed_status_df[[c for c in self.dashboard_cols if c in final_feed_status_df.columns]]
        self.__sheet_generator(dashboard_df, "Dashboard")
        

    def __generate_file_name(self) -> str:
        output_directory = os.path.join(self.file_hunt_path, self.config['DEFAULT']['outbound_directory_name'])
        if os.path.exists(output_directory) == False:
            os.mkdir(output_directory)
        now = datetime.now()
        date_str = now.strftime("%Y%m%d")
        return os.path.join(output_directory, f"results_{self.run_id}_{date_str}_{int(time.time())}.xlsx")

    def __normalize_excel_value(self, v):
        if isinstance(v, (list, tuple)):
            return ", ".join(map(str, v))
        return v

    def __generate_result_file(self):
        try:
            wb = Workbook()
            default_sheet = wb.active
            if default_sheet is not None:
                wb.remove(default_sheet)

            for sheet in self.sheets:
                df = sheet["df"]

                ws = wb.create_sheet(title=sheet["sheet_name"][:31])
                ws.append(list(df.columns))
                for row in df.itertuples(index=False, name=None):
                    ws.append([self.__normalize_excel_value(v) for v in row])

            self.save_path = self.__generate_file_name()

            buffer = BytesIO()
            wb.save(buffer)
            buffer.seek(0)

            with open(self.save_path, "wb") as f:
                shutil.copyfileobj(buffer, f)

            self.logger.info(f"Excel report created: {os.path.abspath(self.save_path)}")
        except Exception as e:
            raise ResultGenerationException(
                "Something went wrong while generating excel file.",
                original_exception=e
            )



    def __segregate_results(self):
        try:
            for status in self.payload:
                self.standard_results.append(
                    {
                        "feed_id": status['feed_id'],
                        "standard_checks_result": status['standard_checks_result']
                    }
                )
                self.comprehensive_results.append(
                    {
                        "feed_id": status['feed_id'],
                        "comprehensive_results": status['comprehensive_results']
                    }
                )
                status.pop('standard_checks_result')
                status.pop('comprehensive_results')
                self.final_feed_status.append(status)
                
        except Exception as e:
            raise ResultGenerationException(
                "Something went wrong while segregating results",
                original_exception=e
            )


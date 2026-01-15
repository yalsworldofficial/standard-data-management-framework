import logging
from pyspark.sql import SparkSession
from sdmf.data_quality.executors.StandardDQExecutor import StandardDQExecutor
from sdmf.data_quality.executors.ComprehensiveDQExecutor import ComprehensiveDQExecutor
# import pandas as pd
# from openpyxl import load_workbook
# from openpyxl.styles import Font, PatternFill, Alignment
# from openpyxl.utils import get_column_letter
logger = logging.getLogger(__name__)


class FeedDataQualityRunner:
    """
    Orchestrates full data quality lifecycle.
    """

    def __init__(self, spark: SparkSession, feed_specs: list[dict]):
        self.spark = spark
        self.feed_specs = feed_specs

        self.standard_checker = StandardDQExecutor(spark)
        self.comprehensive_checker = ComprehensiveDQExecutor(spark)

        self.results = []

    # --------------------------------------------------
    # ENTRY POINT
    # --------------------------------------------------
    def _run(self):
        logger.info("Starting Data Quality Runner")

        passed_feeds = []

        for feed in self.feed_specs:
            result = self._run_standard_checks(feed)
            self.results.append(result)

            if result["standard_checks_passed"]:
                passed_feeds.append(feed["source_table_name"])

        can_ingest = len(passed_feeds) > 0

        if can_ingest:
            # self._trigger_ingestion(passed_feeds)
            self._run_comprehensive_checks(passed_feeds)


        # self._generate_report()

    # --------------------------------------------------
    # PRE LOAD
    # --------------------------------------------------
    def _run_standard_checks(self, feed: dict) -> dict:
        table = feed["source_table_name"]
        logger.info("Standard checks for %s", table)

        df = self.spark.table(table)
        total_count = df.count()

        passed = True

        check_results = []

        for check in feed.get("standard_checks", []):
            for method in check["check_sequence"]:
                ok = self.standard_checker.run_check(
                    method_name=method,
                    df=df,
                    column=check.get("column_name"),
                    total_count=total_count,
                    threshold=check.get("threshold", 0),
                )
                
                passed = bool(ok['passed'])

                check_results.append(ok)

        return {
            "check_table_name": table,
            "standard_checks_passed": passed,
            "standard_checks_result": check_results,
            "comprehensive_passed":None,
            "comprehensive_results": None
        }

    # --------------------------------------------------
    # POST LOAD
    # --------------------------------------------------
    def _run_comprehensive_checks(self, tables: list[str]):
        for feed in self.feed_specs:
            if feed["source_table_name"] not in tables:
                continue

            # for check in feed.get("comprehensive_checks", []):
            #     if check.get("load_stage") != "POST_LOAD":
            #         continue

            has_errors, check_result = self.comprehensive_checker.run_pre_load(feed)

            self._apply_post_load_result(feed["source_table_name"], has_errors, check_result)

    def _apply_post_load_result(self, table: str, has_errors:bool, result: list):
        for r in self.results:
            if r["check_table_name"] == table:
                r["comprehensive_results"] = result
                r["comprehensive_passed"] = has_errors
                r["can_ingest"] = True if r["standard_checks_passed"] and r["comprehensive_passed"] else False


    def _finalize(self) -> list:
        return self.results
    
    # def _generate_report(self):
    #     summary_rows = []
    #     standard_rows = []
    #     comprehensive_rows = []

    #     output_path = "/home/myvm2/Documents/standard-data-management-framework/tests/opt.xlsx"

    #     # ---------------------------------------------------
    #     # Build Data
    #     # ---------------------------------------------------
    #     for table_result in self.results:
    #         table = table_result["check_table_name"]

    #         standard_checks = table_result.get("standard_checks_result", [])
    #         comp_checks_pre = table_result.get("comprehensive_results", [])

    #         total_standard = len(standard_checks)
    #         all_standard_passed = all(c["passed"] for c in standard_checks)
    #         passed1 = sum(
    #             1 for c in standard_checks
    #             if c.get("status") == "FAILED"
    #         )
    #         all_preload_comp_passed = all(c["did_check_pass"] for c in comp_checks_pre)



    #         passed = sum(
    #             1 for c in comp_checks_pre
    #             if c.get("status") == "FAILED"
    #         )

    #         summary_rows.append({
    #             "Table Name": table,
    #             "All Standard Checks Passed": all_standard_passed,
    #             "All Comprehensive Pre-Load Checks Passed": all_preload_comp_passed,

    #             "Standard Check Status": f"{passed1}/{total_standard}",
    #             "Comprehensive Check Pre-Load Status": f"{passed}/{len(comp_checks_pre)}",
    #         })

    #         for c in standard_checks:
    #             standard_rows.append({
    #                 "Table": table,
    #                 "Check Name": c["check_name"],
    #                 "Column": c["column"],
    #                 "Passed": c["passed"],
    #                 "Bad Count": c["bad_count"],
    #                 "Total Count": c["total_count"],
    #                 "Failure Ratio": c["failure_ratio"],
    #                 "Threshold": c["threshold"]
    #             })

    #         for c in comp_checks_pre:
    #             comprehensive_rows.append({
    #                 "Table": c["table"],
    #                 "Check Name": c["check_name"],
    #                 "Load Stage": c["load_stage"],
    #                 "Query": c["query"],
    #                 "Failed Records": c["failed_records"],
    #                 "Threshold": c["threshold"],
    #                 "Status": c["status"],
    #                 "Severity": c.get("severity", "UNKNOWN")
    #             })

    #     df_summary = pd.DataFrame(summary_rows)
    #     df_standard = pd.DataFrame(standard_rows)
    #     df_comprehensive = pd.DataFrame(comprehensive_rows)

    #     # ---------------------------------------------------
    #     # Write Excel
    #     # ---------------------------------------------------
    #     with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    #         df_summary.to_excel(writer, sheet_name="SDMF_Report", index=False, startrow=6)
    #         df_standard.to_excel(writer, sheet_name="Standard Checks", index=False)
    #         df_comprehensive.to_excel(writer, sheet_name="Comprehensive Pre Load Checks", index=False)

    #     wb = load_workbook(output_path)

    #     # ---------------------------------------------------
    #     # Styles
    #     # ---------------------------------------------------
    #     header_fill = PatternFill("solid", fgColor="BDD7EE")
    #     pass_fill = PatternFill("solid", fgColor="C6EFCE")
    #     fail_fill = PatternFill("solid", fgColor="FFC7CE")
    #     warn_fill = PatternFill("solid", fgColor="FFEB9C")

    #     header_font = Font(bold=True)
    #     title_font = Font(size=18, bold=True)
    #     subtitle_font = Font(size=12, italic=True)

    #     # ---------------------------------------------------
    #     # Main Report Sheet
    #     # ---------------------------------------------------
    #     ws = wb["SDMF_Report"]

    #     ws["A1"] = "SDMF (Standard Data Movement Framework)"
    #     ws["A1"].font = title_font
    #     ws["A1"].alignment = Alignment(horizontal="center")

    #     ws.merge_cells("A1:E1")


    #     ws["A4"].font = subtitle_font

    #     ws["A6"] = "Summary"
    #     ws.merge_cells("A6:E6")
    #     # ws["A6"].font = Font(size=14, bold=True)

    #     # Header styling
    #     for cell in ws[6]:
    #         cell.fill = header_fill
    #         cell.font = header_font

    #     self._center_align_column(ws, "A")
    #     self._center_align_column(ws, "B")
    #     self._center_align_column(ws, "C")
    #     self._center_align_column(ws, "D")
    #     self._center_align_column(ws, "E")


    #     # Row coloring logic
    #     for row in ws.iter_rows(min_row=8, max_row=ws.max_row):
    #         all_passed = True if row[1].value and row[2].value else False

    #         if all_passed:
    #             fill = pass_fill
    #         else:
    #             fill = fail_fill

    #         for cell in row:
    #             cell.fill = fill

    #     # ---------------------------------------------------
    #     # Style Other Sheets
    #     # ---------------------------------------------------
    #     for sheet_name in ["Standard Checks", "Comprehensive Pre Load Checks"]:
    #         ws_other = wb[sheet_name]
    #         for cell in ws_other[1]:
    #             cell.fill = header_fill
    #             cell.font = header_font

    #         for col in ws_other.columns:
    #             max_len = max(len(str(c.value)) if c.value else 0 for c in col)
    #             ws_other.column_dimensions[get_column_letter(col[0].column)].width = max_len + 3 # pyright: ignore[reportArgumentType]

    #     self._auto_format_workbook(wb)
    #     wb.save(output_path)


    
    # def _center_align_column(self, ws, column_letter):
    #     for cell in ws[column_letter]:
    #         cell.alignment = Alignment(horizontal="center", vertical="center")



    # def _auto_format_workbook(
    #     self,
    #     workbook,
    #     header_row=1,
    #     min_width=10,
    #     max_width=80,
    #     wrap_keywords=("query", "sql", "message")
    # ):
    #     """
    #     Auto-format all worksheets in an existing openpyxl workbook.
    #     Modifies the workbook in place.
    #     """

    #     for ws in workbook.worksheets:

    #         # Freeze header row
    #         ws.freeze_panes = f"A{header_row + 1}"

    #         # Auto-fit columns
    #         for col in ws.columns:
    #             max_length = 0
    #             col_letter = get_column_letter(col[0].column)

    #             for cell in col:
    #                 if cell.value:
    #                     cell_len = len(str(cell.value))
    #                     if cell.font and cell.font.bold:
    #                         cell_len *= 1.1
    #                     max_length = max(max_length, cell_len)

    #             adjusted_width = min(
    #                 max(int(max_length) + 2, min_width),
    #                 max_width
    #             )

    #             ws.column_dimensions[col_letter].width = adjusted_width

    #         # Center-align headers
    #         for cell in ws[header_row]:
    #             cell.alignment = Alignment(horizontal="center", vertical="center")

    #         # Wrap long-text columns
    #         for col in ws.columns:
    #             header = str(col[0].value).lower() if col[0].value else ""
    #             if any(k in header for k in wrap_keywords):
    #                 for cell in col:
    #                     cell.alignment = Alignment(wrap_text=True)

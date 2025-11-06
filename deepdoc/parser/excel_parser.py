#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import logging
import sys
from io import BytesIO

import pandas as pd
from openpyxl import Workbook, load_workbook

from rag.nlp import find_codec


class RAGFlowExcelParser:

    @staticmethod
    def _load_excel_to_workbook(file_like_object):
        if isinstance(file_like_object, bytes):
            file_like_object = BytesIO(file_like_object)

        # Read first 4 bytes to determine file type
        file_like_object.seek(0)
        file_head = file_like_object.read(4)
        file_like_object.seek(0)

        if not (file_head.startswith(b'PK\x03\x04') or file_head.startswith(b'\xD0\xCF\x11\xE0')):
            logging.info("****wxy: Not an Excel file, converting CSV to Excel Workbook")

            try:
                file_like_object.seek(0)
                df = pd.read_csv(file_like_object)
                return RAGFlowExcelParser._dataframe_to_workbook(df)

            except Exception as e_csv:
                raise Exception(f"****wxy: Failed to parse CSV and convert to Excel Workbook: {e_csv}")

        try:
            return load_workbook(file_like_object,data_only= True)
        except Exception as e:
            logging.info(f"****wxy: openpyxl load error: {e}, try pandas instead")
            try:
                file_like_object.seek(0)
                df = pd.read_excel(file_like_object)
                return RAGFlowExcelParser._dataframe_to_workbook(df)
            except Exception as e_pandas:
                raise Exception(f"****wxy: pandas.read_excel error: {e_pandas}, original openpyxl error: {e}")

    @staticmethod
    def _dataframe_to_workbook(df):
        wb = Workbook()
        ws = wb.active
        ws.title = "Data"

        for col_num, column_name in enumerate(df.columns, 1):
            ws.cell(row=1, column=col_num, value=column_name)

        for row_num, row in enumerate(df.values, 2):
            for col_num, value in enumerate(row, 1):
                ws.cell(row=row_num, column=col_num, value=value)

        return wb

    def html(self, fnm, chunk_rows=256):
        file_like_object = BytesIO(fnm) if not isinstance(fnm, str) else fnm
        wb = RAGFlowExcelParser._load_excel_to_workbook(file_like_object)
        tb_chunks = []
        for sheetname in wb.sheetnames:
            ws = wb[sheetname]
            rows = list(ws.rows)
            if not rows:
                continue

            tb_rows_0 = "<tr>"
            for t in rows[0]:
                tb_rows_0 += f"<th>{t.value}</th>"
            tb_rows_0 += "</tr>"

            for chunk_i in range((len(rows) - 1) // chunk_rows + 1):
                tb = ""
                tb += f"<table><caption>{sheetname}</caption>"
                tb += tb_rows_0
                for r in rows[1 + chunk_i * chunk_rows: 1 + (chunk_i + 1) * chunk_rows]:
                    tb += "<tr>"
                    for i, c in enumerate(r):
                        if c.value is None:
                            tb += "<td></td>"
                        else:
                            tb += f"<td>{c.value}</td>"
                    tb += "</tr>"
                tb += "</table>\n"
                tb_chunks.append(tb)

        return tb_chunks

    def __call__(self, fnm):
        """
        Parse Excel file using memory-efficient generator pattern.
        No row limits - generators handle memory efficiently.

        Args:
            fnm: File name or binary content

        Returns:
            List of parsed text lines
        """
        file_like_object = BytesIO(fnm) if not isinstance(fnm, str) else fnm

        try:
            wb = RAGFlowExcelParser._load_excel_to_workbook(file_like_object)
        except Exception as e:
            logging.error(f"Failed to load Excel workbook: {e}")
            raise

        res = []
        total_rows_processed = 0

        for sheetname in wb.sheetnames:
            try:
                ws = wb[sheetname]
                rows_iter = ws.iter_rows()  # Generator - memory efficient!

                # Get header row
                try:
                    header_row = next(rows_iter)
                except StopIteration:
                    logging.debug(f"Sheet '{sheetname}' is empty, skipping")
                    continue

                # Process all data rows using generator (no limit needed - memory efficient!)
                row_count = 0
                for r in rows_iter:
                    fields = []
                    for i, c in enumerate(r):
                        if not c.value:
                            continue
                        t = str(header_row[i].value) if i < len(header_row) else ""
                        t += ("：" if t else "") + str(c.value)
                        fields.append(t)

                    if fields:  # Only append non-empty rows
                        line = "; ".join(fields)
                        if sheetname.lower().find("sheet") < 0:
                            line += " ——" + sheetname
                        res.append(line)

                    row_count += 1

                total_rows_processed += row_count
                logging.debug(f"Processed {row_count} rows from sheet '{sheetname}'")

            except Exception as e:
                logging.error(f"Error processing sheet '{sheetname}': {e}")
                # Continue with next sheet instead of failing completely
                continue

        logging.info(f"Excel parsing complete: {len(res)} lines from {total_rows_processed} rows across {len(wb.sheetnames)} sheets")
        return res

    @staticmethod
    def row_number(fnm, binary):
        if fnm.split(".")[-1].lower().find("xls") >= 0:
            wb = RAGFlowExcelParser._load_excel_to_workbook(BytesIO(binary))
            total = 0
            for sheetname in wb.sheetnames:
                ws = wb[sheetname]
                total += len(list(ws.rows))
            return total

        if fnm.split(".")[-1].lower() in ["csv", "txt"]:
            encoding = find_codec(binary)
            txt = binary.decode(encoding, errors="ignore")
            return len(txt.split("\n"))


if __name__ == "__main__":
    psr = RAGFlowExcelParser()
    psr(sys.argv[1])

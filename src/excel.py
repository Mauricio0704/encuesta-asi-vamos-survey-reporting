import pandas as pd
from pathlib import Path


class ExcelContext:
    def __init__(self, writer, sheet_name, start_row=0):
        self.writer = writer
        self.sheet_name = sheet_name
        self.start_row = start_row


def get_writer_config(output_path: Path) -> dict:
    if output_path.exists():
        mode = "a"
        if_sheet = "overlay"
    else:
        mode = "w"
        if_sheet = None

    return {
        "path": output_path,
        "engine": "openpyxl",
        "mode": mode,
        "if_sheet_exists": if_sheet,
    }


def write_text_to_excel(ctx: ExcelContext, text: str) -> None:
    pd.DataFrame([[text]]).to_excel(
        ctx.writer,
        sheet_name=ctx.sheet_name,
        startrow=ctx.start_row,
        index=False,
        header=False,
    )
    ctx.start_row += 2


def write_table_to_excel(ctx: ExcelContext, df: pd.DataFrame) -> None:
    df.to_excel(
        ctx.writer,
        sheet_name=ctx.sheet_name,
        startrow=ctx.start_row,
        index=False,
    )
    ctx.start_row += len(df) + 3

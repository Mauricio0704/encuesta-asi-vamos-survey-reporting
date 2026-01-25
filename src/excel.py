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


def add_total_row(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    total_row = df.iloc[:, 2:].sum()
    total_row.name = "Total"
    total_df = pd.DataFrame(total_row).T
    total_df.insert(0, df.columns[0], "Total")
    total_df.insert(1, df.columns[1], "Total")

    return pd.concat([df, total_df], ignore_index=True)


def add_weighted_average_row(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    valid_df = df[~df[df.columns[0]].isin(["Total", "Promedio"])]
    weighted_averages = {}
    for col in df.columns[2:]:
        weights = pd.to_numeric(valid_df[col], errors="coerce")
        values = pd.to_numeric(valid_df["Respuesta"], errors="coerce")

        mask = ~values.isin([7777, 8888, 9999]) & ~weights.isin([7777, 8888, 9999])
        filtered_values = values[mask]
        filtered_weights = weights[mask]

        if filtered_weights.sum() == 0:
            weighted_avg = 0
        else:
            weighted_avg = (
                filtered_values * filtered_weights
            ).sum() / filtered_weights.sum()

        weighted_averages[col] = weighted_avg
    weighted_avg_row = pd.DataFrame(weighted_averages, index=["Promedio"])
    weighted_avg_row.insert(0, df.columns[0], "Promedio")
    weighted_avg_row.insert(1, df.columns[1], "Promedio")

    return pd.concat([df, weighted_avg_row], ignore_index=True)


def get_relative_table(df: pd.DataFrame) -> pd.DataFrame:
    # Remove Promedio row if exists
    df = df[df[df.columns[0]] != "Promedio"]

    if df.empty:
        return df

    relative_df = df.copy()
    for col in df.columns[2:]:
        total = df[col].iloc[-1]
        if total == 0:
            relative_df[col] = 0
        else:
            relative_df[col] = df[col] / total
    return relative_df


def add_total_column(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["Total"] = df.iloc[:, 2:].sum(axis=1)
    return df

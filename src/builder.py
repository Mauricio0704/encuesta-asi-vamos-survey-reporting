import pandas as pd
import json
from tqdm import tqdm

from src.paths import OUTPUT_DIR, PROCESSED_DATA_DIR
from src.extend_tables import (
    add_total_row,
    get_relative_table,
    add_weighted_average_row,
    add_total_column,
)
from src.repository import (
    get_questions_by_section,
)
from src.excel import (
    ExcelContext,
    write_text_to_excel,
    write_table_to_excel,
    get_writer_config,
)
from src.repository import build_disaggregation_report

with open(PROCESSED_DATA_DIR / "disaggregations.json", "r") as file:
    data: dict = json.load(file)


def build_question_report(
    conn,
    question: pd.Series,
    section: str,
    initial_only: bool = True,
) -> None:
    question_id = question["id"]
    sheet_name = question["id"][:31]
    question_text = question["q_text"]
    question_type = question["type"]

    titles_with_dfs = []

    question_specific_disaggregations = data.get(question_id, [])

    for disaggregation in question_specific_disaggregations:
        df = add_total_row(
            build_disaggregation_report(
                conn,
                question_id,
                disaggregation["type"],
                initial_only,
            )
        )

        if "municipio" not in disaggregation["type"]:
            df = add_total_column(df)

        if question_type == "numerica":
            df = add_weighted_average_row(df)

        titles_with_dfs.append((f"Respuesta por {disaggregation["type"]}", df))

    output_path = OUTPUT_DIR / f"{section}.xlsx"
    config = get_writer_config(output_path)

    with pd.ExcelWriter(**config) as writer:
        ctx = ExcelContext(writer, sheet_name)

        if question["q_notes"] and isinstance(question["q_notes"], str):
            write_text_to_excel(ctx, question["q_notes"])

        write_text_to_excel(ctx, f"{question_id} - {question_text}")

        for title, df in titles_with_dfs:
            write_text_to_excel(ctx, title)
            write_table_to_excel(ctx, df)
            relative_df = get_relative_table(df)
            write_table_to_excel(ctx, relative_df)


def build_section_report(conn, section) -> None:
    questions_df = get_questions_by_section(conn, section)

    for _, question in tqdm(
        questions_df.iterrows(),
        total=len(questions_df),
        desc=f"Building {section} section report",
        unit="question",
        colour="green",
    ):
        build_question_report(conn, question, section)

        if question["id"].startswith("cp"):
            build_question_report(
                conn,
                question,
                section + "_sin_factor",
                initial_only=False,
            )

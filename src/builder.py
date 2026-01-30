import pandas as pd
import json
from tqdm import tqdm

from src.paths import OUTPUT_DIR, PROCESSED_DATA_DIR
from src.repository import get_questions_by_section
from src.excel import (
    ExcelContext,
    write_text_to_excel,
    write_table_to_excel,
    get_writer_config,
    add_total_row,
    get_relative_table,
    add_weighted_average_row,
    add_total_column,
)
from src.repository import build_disaggregation_report

with open(PROCESSED_DATA_DIR / "disaggregations.json", "r") as file:
    data: dict = json.load(file)


def _build_question_tables(
    conn,
    question: pd.Series,
    initial_only: bool = True,
) -> tuple[str | None, str, list[tuple[str, pd.DataFrame, pd.DataFrame]]]:
    question_id = question["id"]
    question_text = question["q_text"]
    question_type = question["type"]

    question_specific_disaggregations = data.get(question_id, [])
    tables: list[tuple[str, pd.DataFrame, pd.DataFrame]] = []

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

        relative_df = get_relative_table(df)
        tables.append((f"Respuesta por {disaggregation['type']}", df, relative_df))

    notes = question["q_notes"] if isinstance(question["q_notes"], str) else None
    question_title = f"{question_id} - {question_text}"

    return notes, question_title, tables


def _append_question_to_sheet(
    ctx: ExcelContext,
    notes: str | None,
    question_title: str,
    tables: list[tuple[str, pd.DataFrame, pd.DataFrame]],
) -> None:
    if notes:
        write_text_to_excel(ctx, notes)

    write_text_to_excel(ctx, question_title)

    for title, df, relative_df in tables:
        write_text_to_excel(ctx, title)
        write_table_to_excel(ctx, df)
        write_table_to_excel(ctx, relative_df)


def build_question_report(
    conn,
    question: pd.Series,
    section: str,
    initial_only: bool = True,
) -> None:
    sheet_name = question["id"][:31]
    notes, question_title, tables = _build_question_tables(
        conn,
        question,
        initial_only,
    )

    output_path = OUTPUT_DIR / f"{section}.xlsx"
    config = get_writer_config(output_path)

    with pd.ExcelWriter(**config) as writer:
        ctx = ExcelContext(writer, sheet_name)
        _append_question_to_sheet(ctx, notes, question_title, tables)


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


def build_topics_workbook(
    conn,
    sections: list[str],
    output_filename: str = "tabulados_por_tema.xlsx",
) -> None:
    output_path = OUTPUT_DIR / output_filename
    if output_path.exists():
        output_path.unlink()

    with pd.ExcelWriter(output_path, engine="openpyxl", mode="w") as writer:
        contexts: dict[str, ExcelContext] = {}

        for section in sections:
            questions_df = get_questions_by_section(conn, section)
            sheet_name = section[:31]
            if sheet_name not in contexts:
                contexts[sheet_name] = ExcelContext(writer, sheet_name)

            for _, question in tqdm(
                questions_df.iterrows(),
                total=len(questions_df),
                desc=f"Building {section} topic sheet",
                unit="question",
                colour="green",
            ):
                notes, question_title, tables = _build_question_tables(
                    conn,
                    question,
                    initial_only=True,
                )
                _append_question_to_sheet(
                    contexts[sheet_name],
                    notes,
                    question_title,
                    tables,
                )

                if question["id"].startswith("cp"):
                    sin_factor_section = f"{section}_sin_factor"
                    sin_factor_sheet = sin_factor_section[:31]
                    if sin_factor_sheet not in contexts:
                        contexts[sin_factor_sheet] = ExcelContext(
                            writer,
                            sin_factor_sheet,
                        )

                    notes_sf, question_title_sf, tables_sf = _build_question_tables(
                        conn,
                        question,
                        initial_only=False,
                    )
                    _append_question_to_sheet(
                        contexts[sin_factor_sheet],
                        notes_sf,
                        question_title_sf,
                        tables_sf,
                    )

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
from src.metadata import DISAGGREGATIONS_TO_TITLES

with open(PROCESSED_DATA_DIR / "disaggregations.json", "r") as file:
    data: dict = json.load(file)


def _parse_question_id(question_id: str) -> tuple[str, int, int]:
    """
    Parse question ID into (prefix, main_number, suffix_number) for sorting.
    
    Examples:
        cp6 -> ('cp', 6, 0)
        cp7_1 -> ('cp', 7, 1)
        p14_10 -> ('p', 14, 10)
    """
    if question_id.startswith("cp"):
        prefix = "cp"
        rest = question_id[2:]
    elif question_id.startswith("p"):
        prefix = "p"
        rest = question_id[1:]
    else:
        # Unknown format, return as is
        return (question_id, 0, 0)
    
    # Split by underscore to get main number and suffix
    parts = rest.split("_")
    try:
        main_num = int(parts[0])
        suffix_num = int(parts[1]) if len(parts) > 1 else 0
    except ValueError:
        # If parsing fails, return defaults
        return (prefix, 0, 0)
    
    return (prefix, main_num, suffix_num)


def _sort_questions(questions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Sort questions by grouping cp questions first, then p questions.
    
    Example:
        Input: cp6, cp7_1, cp7_2, p23, p10, p14_1, p14_10, p14_11, cp4
        Output: cp4, cp6, cp7_1, cp7_2, p10, p14_1, p14_10, p14_11, p23
    """
    questions_df['sort_key'] = questions_df['id'].apply(_parse_question_id)
    questions_df = questions_df.sort_values('sort_key').drop('sort_key', axis=1).reset_index(drop=True)
    return questions_df


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
        tables.append((f"{DISAGGREGATIONS_TO_TITLES[disaggregation['type']]}", df, relative_df))

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
        write_text_to_excel(ctx, notes, is_hdr=True)
        ctx.start_row -= 1

    write_text_to_excel(ctx, question_title, is_hdr=True)

    for title, df, relative_df in tables:
        write_text_to_excel(ctx, title)
        write_table_to_excel(ctx, df)
        write_table_to_excel(ctx, relative_df, is_rel=True)


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
    questions_df = _sort_questions(questions_df)

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
            questions_df = _sort_questions(questions_df)
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

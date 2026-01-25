import pandas as pd

from src.metadata import DESIRED_ORDERS
from src.queries.provisional import DISAGGREGATIONS_MAP
from src.queries.questions import (
    get_question_sections_query,
    get_questions_by_section_query,
)


def get_question_sections(conn) -> list[str]:
    query = get_question_sections_query()
    df = pd.read_sql_query(query, conn)

    return df["section"].tolist()


def get_questions_by_section(conn, section: str) -> pd.DataFrame:
    query = get_questions_by_section_query(section)
    df = pd.read_sql_query(query, conn)

    return df


def build_disaggregation_report(
    conn,
    question_id: str,
    disaggregation: str,
    initial_only: bool = True,
) -> pd.DataFrame:

    sql_function = DISAGGREGATIONS_MAP.get(disaggregation)

    if not sql_function:
        raise ValueError(f"Disaggregation '{disaggregation}' is not supported.")

    sql = sql_function(initial_only)

    params = {"question_id": question_id}
    df_long = pd.read_sql_query(sql, conn, params=params)

    if df_long.empty:
        return df_long

    df_pivot = df_long.pivot_table(
        index=["id_respuesta", "Respuesta"],
        columns="grupo",
        values="valor",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    fixed_cols = ["id_respuesta", "Respuesta"]
    group_cols = [c for c in df_pivot.columns if c not in fixed_cols]

    desired_order = None

    if disaggregation == "nivel_max_estudios" or disaggregation.startswith("nivel_actual_estudios"):
        desired_order = DESIRED_ORDERS.get("estudios")
    elif disaggregation == "ingreso":
        desired_order = DESIRED_ORDERS.get("ingreso")
    elif disaggregation == "edad":
        desired_order = DESIRED_ORDERS.get("edad")
    elif "municipio" in disaggregation:
        desired_order = DESIRED_ORDERS.get("municipio")
    elif disaggregation == "tipo_escuela":
        desired_order = DESIRED_ORDERS.get("tipo_escuela")
    elif disaggregation == "sexo":
        desired_order = DESIRED_ORDERS.get("sexo")
    
    if desired_order:
        group_cols = [col for col in desired_order if col in group_cols]

    df_pivot = df_pivot[fixed_cols + group_cols]

    return df_pivot

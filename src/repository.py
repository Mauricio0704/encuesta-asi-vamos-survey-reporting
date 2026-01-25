import pandas as pd

from src.provisional import DISAGGREGATIONS_MAP
from src.questions import (
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

    if disaggregation == "nivel_max_estudios":
        desired_order = [
            "Ninguno",
            "Preescolar",
            "Primaria",
            "Secundaria",
            "Preparatoria o bachillerato general",
            "Bachillerato tecnológico",
            "Estudios técnicos o comerciales con primaria terminada",
            "Estudios técnicos o comerciales con secundaria terminada",
            "Estudios técnicos o comerciales con preparatoria terminada",
            "Normal con primaria o secundaria terminada",
            "Normal de licenciatura",
            "Licenciatura",
            "Especialidad",
            "Maestría",
            "Doctorado",
        ]
        group_cols = [col for col in desired_order if col in group_cols]
    elif disaggregation == "ingreso":
        desired_order = [
            "Sin ingreso",
            "No contesta",
            "Menos de 1 SM ($1 - $8,364)",
            "1-2 SM ($8,364 - $16,728)",
            "2-3 SM ($16,728 - $25,092)",
            "3-4 SM ($25,092 - $33,456)",
            "4-5 SM ($33,456 - $41,820)",
            "5-6 SM ($41,820 - $50,184)",
            "6-7 SM ($50,184 - $58,548)",
            "7-8 SM ($58,548 - $66,912)",
            "8-9 SM ($66,912 - $75,276)",
            "9-10 SM ($75,276 - $83,640)",
            "10 o más SM ($83,640 o más)",
        ]
        group_cols = [col for col in desired_order if col in group_cols]
    elif disaggregation == "edad":
        desired_order = [
            "0-5",
            "6-12",
            "13-17",
            "18-24",
            "25-34",
            "35-44",
            "45-54",
            "55-64",
            "65-74",
            "75 o más",
        ]
        group_cols = [col for col in desired_order if col in group_cols]
    elif "municipio" in disaggregation:
        desired_order = [
            "Apodaca",
            "Cadereyta",
            "Escobedo",
            "García",
            "Guadalupe",
            "Juárez",
            "Monterrey",
            "San Nicolás de los Garza",
            "San Pedro Garza García",
            "Santa Catarina",
            "Santiago",
            "AMM",
            "Periferia",
            "Resto NL",
            "Nuevo León",
        ]
        group_cols = [col for col in desired_order if col in group_cols]
    elif disaggregation.startswith("nivel_actual_estudios"):
        desired_order = [
            "Ninguno",
            "Preescolar",
            "Primaria",
            "Secundaria",
            "Preparatoria o bachillerato general",
            "Bachillerato tecnológico",
            "Estudios técnicos o comerciales con primaria terminada",
            "Estudios técnicos o comerciales con secundaria terminada",
            "Estudios técnicos o comerciales con preparatoria terminada",
            "Normal con primaria o secundaria terminada",
            "Normal de licenciatura",
            "Licenciatura",
            "Especialidad",
            "Maestría",
            "Doctorado",
        ]
        group_cols = [col for col in desired_order if col in group_cols]
    elif disaggregation == "tipo_escuela":
        desired_order = [
            "Pública",
            "Privada",
            "Otro tipo",
        ]
        group_cols = [col for col in desired_order if col in group_cols]
    elif disaggregation == "sexo":
        desired_order = [
            "Hombre",
            "Mujer",
        ]
        group_cols = [col for col in desired_order if col in group_cols]

    df_pivot = df_pivot[fixed_cols + group_cols]

    return df_pivot

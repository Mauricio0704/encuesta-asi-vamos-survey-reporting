from src.metadata import (
    AMM_ID,
    ID_TO_CITY_NAME,
    PERIFERIA_ID,
    AGE_BINS,
    AGE_LABELS,
)


def _get_weight_clause(initial_only: bool) -> str:
    return "r.factor_cvnl" if initial_only else "1"


def _get_initial_filter(initial_only: bool) -> str:
    return "AND r.is_initial_respondent = 1" if initial_only else ""


def get_trabajo_remunerado_query(initial_only: bool = True) -> str:
    """Get paid work data (tipo_trabajo values 1, 4, 6) for male respondents."""

    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    query = f"""
        SELECT
            COALESCE(o.option_id, a.value)        AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT))     AS Respuesta,
            oa.option_label    AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o
        ON a.question_id = o.question_id
        AND a.option_id   = o.option_id

        LEFT JOIN respondent_attributes ra
        ON a.respondent_id = ra.respondent_id
        AND ra.attribute    = 'tipo_trabajo'

        LEFT JOIN options oa
        ON ra.question_id = oa.question_id
        AND ra.value       = oa.option_id
        AND ra.value IN (1, 4, 6)

        JOIN responses r
        ON a.respondent_id = r.respondent_id

        WHERE a.question_id = :question_id
        {initial_filter}

        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            oa.option_label
    """

    return query


def get_trabajo_remunerado_by_sex_query(
    sex_id: int = 0, initial_only: bool = True
) -> str:
    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    query = f"""
        SELECT
            COALESCE(o.option_id, a.value)        AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT))     AS Respuesta,
            oa.option_label    AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o
        ON a.question_id = o.question_id
        AND a.option_id   = o.option_id

        LEFT JOIN respondent_attributes ra
        ON a.respondent_id = ra.respondent_id
        AND ra.attribute    = 'tipo_trabajo'

        LEFT JOIN options oa
        ON ra.question_id = oa.question_id
        AND ra.value       = oa.option_id
        AND ra.value IN (1, 4, 6)

        LEFT JOIN respondent_attributes rs
        ON a.respondent_id = rs.respondent_id
        AND rs.attribute    = 'sexo'

        JOIN responses r
        ON a.respondent_id = r.respondent_id

        WHERE a.question_id = :question_id
        AND rs.value = {sex_id}
        {initial_filter}

        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            oa.option_label
    """
    return query


def get_tipo_trabajo_query(initial_only: bool = True) -> str:
    """Categorize respondents by trabajo remunerado (1,4,6) vs trabajo no remunerado (5)."""

    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    query = f"""
        SELECT
            COALESCE(o.option_id, a.value)        AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT))     AS Respuesta,
            CASE
                WHEN ra.value IN (1, 4, 6) THEN 'Trabajo remunerado'
                WHEN ra.value = 5 THEN 'Trabajo no remunerado'
                ELSE 'Otro'
            END AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o
        ON a.question_id = o.question_id
        AND a.option_id   = o.option_id

        LEFT JOIN respondent_attributes ra
        ON a.respondent_id = ra.respondent_id
        AND ra.attribute    = 'tipo_trabajo'

        JOIN responses r
        ON a.respondent_id = r.respondent_id

        WHERE a.question_id = :question_id
        {initial_filter}

        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            grupo
    """
    return query


def get_tipo_trabajo_by_sex_query(sex_id: int = 0, initial_only: bool = True) -> str:
    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    query = f"""
        SELECT
            COALESCE(o.option_id, a.value)        AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT))     AS Respuesta,
            CASE
                WHEN ra.value IN (1, 4, 6) THEN 'Trabajo remunerado'
                WHEN ra.value = 5 THEN 'Trabajo no remunerado'
                ELSE 'Otro'
            END AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o
        ON a.question_id = o.question_id
        AND a.option_id   = o.option_id
        LEFT JOIN respondent_attributes ra
        ON a.respondent_id = ra.respondent_id
        AND ra.attribute    = 'tipo_trabajo'
        LEFT JOIN respondent_attributes rs
        ON a.respondent_id = rs.respondent_id
        AND rs.attribute    = 'sexo'
        JOIN responses r
        ON a.respondent_id = r.respondent_id
        WHERE a.question_id = :question_id
        AND rs.value = {sex_id}
        {initial_filter}
        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            grupo
    """

    return query


def get_afiliacion_servicio_salud_query(initial_only: bool = True) -> str:
    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    query = f"""
        SELECT
            COALESCE(o.option_id, a.value)        AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT))     AS Respuesta,
            oa.option_label    AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o
        ON a.question_id = o.question_id
        AND a.option_id   = o.option_id

        LEFT JOIN respondent_attributes ra
        ON a.respondent_id = ra.respondent_id
        AND ra.attribute    = 'afiliacion_servicio_salud'

        LEFT JOIN options oa
        ON ra.question_id = oa.question_id
        AND ra.value       = oa.option_id

        JOIN responses r
        ON a.respondent_id = r.respondent_id

        WHERE a.question_id = :question_id
        {initial_filter}

        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            oa.option_label
    """
    return query


def get_nivel_max_estudios_query(initial_only: bool = True) -> str:
    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    query = f"""
        SELECT
            COALESCE(o.option_id, a.value)        AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT))     AS Respuesta,
            oa.option_label    AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o
        ON a.question_id = o.question_id
        AND a.option_id   = o.option_id 

        LEFT JOIN respondent_attributes ra
        ON a.respondent_id = ra.respondent_id
        AND ra.attribute    = 'nivel_max_estudios'

        LEFT JOIN options oa
        ON ra.question_id = oa.question_id
        AND ra.value       = oa.option_id

        JOIN responses r
        ON a.respondent_id = r.respondent_id

        WHERE a.question_id = :question_id
        {initial_filter}

        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            oa.option_label
    """
    return query


def get_servicio_salud_donde_se_atendio_query(initial_only: bool = True) -> str:
    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    query = f"""
        SELECT
            COALESCE(o.option_id, a.value)        AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT))     AS Respuesta,
            oa.option_label    AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o
        ON a.question_id = o.question_id
        AND a.option_id   = o.option_id

        LEFT JOIN respondent_attributes ra
        ON a.respondent_id = ra.respondent_id
        AND ra.attribute    = 'servicio_salud_donde_se_atendio'

        LEFT JOIN options oa
        ON ra.question_id = oa.question_id
        AND ra.value       = oa.option_id

        JOIN responses r
        ON a.respondent_id = r.respondent_id

        WHERE a.question_id = :question_id
        {initial_filter}

        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            oa.option_label
    """
    return query


def get_tipo_servicio_salud_donde_se_atendio_query(initial_only: bool = True) -> str:
    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    query = f"""
        SELECT
            COALESCE(o.option_id, a.value)        AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT))     AS Respuesta,
            CASE
                WHEN ra.value IN (2, 3, 7, 8, 9, 10, 12, 13) THEN 'Servicios Privados'
                WHEN ra.value IN (1, 4, 5, 6, 11) THEN 'Servicios Publicos'
            END AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o
        ON a.question_id = o.question_id
        AND a.option_id   = o.option_id

        LEFT JOIN respondent_attributes ra
        ON a.respondent_id = ra.respondent_id
        AND ra.attribute    = 'servicio_salud_donde_se_atendio'

        JOIN responses r
        ON a.respondent_id = r.respondent_id

        WHERE a.question_id = :question_id
        {initial_filter}

        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            grupo
    """
    return query


def get_tipo_escuela_query(initial_only: bool = True) -> str:
    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    query = f"""
        SELECT
            COALESCE(o.option_id, a.value)        AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT))     AS Respuesta,
            oa.option_label    AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o
        ON a.question_id = o.question_id
        AND a.option_id   = o.option_id

        LEFT JOIN respondent_attributes ra
        ON a.respondent_id = ra.respondent_id
        AND ra.attribute    = 'tipo_escuela'

        LEFT JOIN options oa
        ON ra.question_id = oa.question_id
        AND ra.value       = oa.option_id

        JOIN responses r
        ON a.respondent_id = r.respondent_id

        WHERE a.question_id = :question_id
        {initial_filter}

        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            oa.option_label
    """
    return query


def get_nivel_actual_estudios_query(initial_only: bool = True) -> str:
    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    query = f"""
        SELECT
            COALESCE(o.option_id, a.value)        AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT))     AS Respuesta,
            oa.option_label    AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o
        ON a.question_id = o.question_id
        AND a.option_id   = o.option_id

        LEFT JOIN respondent_attributes ra
        ON a.respondent_id = ra.respondent_id
        AND ra.attribute    = 'nivel_actual_estudios'

        LEFT JOIN options oa
        ON ra.question_id = oa.question_id
        AND ra.value       = oa.option_id

        JOIN responses r
        ON a.respondent_id = r.respondent_id

        WHERE a.question_id = :question_id
        {initial_filter}

        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            oa.option_label
    """
    return query


def get_nivel_actual_estudios_by_tipo_escuela_query(
    school_type_id: int, initial_only: bool = True
) -> str:
    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    query = f"""
        SELECT
            COALESCE(o.option_id, a.value)        AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT))     AS Respuesta,
            oa.option_label    AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o
        ON a.question_id = o.question_id
        AND a.option_id   = o.option_id

        LEFT JOIN respondent_attributes ra
        ON a.respondent_id = ra.respondent_id
        AND ra.attribute    = 'nivel_actual_estudios'

        LEFT JOIN respondent_attributes rt
        ON a.respondent_id = rt.respondent_id
        AND rt.attribute    = 'tipo_escuela'

        LEFT JOIN options oa
        ON ra.question_id = oa.question_id
        AND ra.value       = oa.option_id
        AND rt.value = {school_type_id}

        JOIN responses r
        ON a.respondent_id = r.respondent_id
        WHERE a.question_id = :question_id
        {initial_filter}

        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            oa.option_label
    """

    return query


def get_sexo_query(initial_only: bool = True) -> str:
    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    query = f"""
        SELECT
            COALESCE(o.option_id, a.value)        AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT))     AS Respuesta,
            oa.option_label    AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o
        ON a.question_id = o.question_id
        AND a.option_id   = o.option_id

        LEFT JOIN respondent_attributes ra
        ON a.respondent_id = ra.respondent_id
        AND ra.attribute    = 'sexo'

        LEFT JOIN options oa
        ON ra.question_id = oa.question_id
        AND ra.value       = oa.option_id

        JOIN responses r
        ON a.respondent_id = r.respondent_id

        WHERE a.question_id = :question_id
        {initial_filter}

        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            oa.option_label
    """
    return query


def get_municipio_query(initial_only: bool = True) -> str:
    """
    Should take into account AMM_ID and PERIFERIA_ID constants.
    The ids are mapped to their names using ID_TO_CITY_NAME.
    The final groups should be
    All municipalities in AMM_ID as their names, a group "AMM", a group "Periferia", a group "Resto NL", and a group "Nuevo León".
    It should use the city_id from the responses table.
    """
    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    amm_list = ", ".join(map(str, AMM_ID))
    periferia_list = ", ".join(map(str, PERIFERIA_ID))
    amm_plus_perif = ", ".join(map(str, AMM_ID + PERIFERIA_ID))

    # Subquery 1: specific city rows (only for AMM municipalities)
    city_case = ""
    for city_id in AMM_ID:
        city_name = ID_TO_CITY_NAME[city_id]
        city_case += f"WHEN r.city_id = {city_id} THEN '{city_name}'\n            "

    query = f"""
        -- City-level rows (only for AMM municipalities)
        SELECT
            COALESCE(o.option_id, a.value) AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT)) AS Respuesta,
            CASE
            {city_case}
            END AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o ON a.question_id = o.question_id AND a.option_id = o.option_id
                JOIN responses r ON a.respondent_id = r.respondent_id
                WHERE a.question_id = :question_id
                    AND r.city_id IN ({amm_list})
        {initial_filter}
        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            grupo

        UNION ALL

        -- Regional rows: AMM / Periferia / Resto NL
        SELECT
            COALESCE(o.option_id, a.value) AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT)) AS Respuesta,
            CASE
                WHEN r.city_id IN ({amm_list}) THEN 'AMM'
                WHEN r.city_id IN ({periferia_list}) THEN 'Periferia'
                WHEN r.city_id NOT IN ({amm_plus_perif}) THEN 'Resto NL'
            END AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o ON a.question_id = o.question_id AND a.option_id = o.option_id
                JOIN responses r ON a.respondent_id = r.respondent_id
                WHERE a.question_id = :question_id
                    AND r.city_id IS NOT NULL
        {initial_filter}
        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            grupo

        UNION ALL

        -- Entire state row: Nuevo León (any non-null municipio)
        SELECT
            COALESCE(o.option_id, a.value) AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT)) AS Respuesta,
            'Nuevo León' AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o ON a.question_id = o.question_id AND a.option_id = o.option_id
                JOIN responses r ON a.respondent_id = r.respondent_id
                WHERE a.question_id = :question_id
                    AND r.city_id IS NOT NULL
        {initial_filter}
        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT))
    """

    return query


def get_municipio_by_sex_query(sex_id: int = 0, initial_only: bool = True) -> str:
    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    amm_list = ", ".join(map(str, AMM_ID))
    periferia_list = ", ".join(map(str, PERIFERIA_ID))
    amm_plus_perif = ", ".join(map(str, AMM_ID + PERIFERIA_ID))

    city_case = ""
    for city_id in AMM_ID:
        city_name = ID_TO_CITY_NAME[city_id]
        city_case += f"WHEN r.city_id = {city_id} THEN '{city_name}'\n            "

    query = f"""
        -- City-level rows (only for AMM municipalities)
        SELECT
            COALESCE(o.option_id, a.value) AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT)) AS Respuesta,
            CASE
            {city_case}
            END AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o ON a.question_id = o.question_id AND a.option_id = o.option_id
            LEFT JOIN respondent_attributes rs ON a.respondent_id = rs.respondent_id AND rs.attribute = 'sexo'
            JOIN responses r ON a.respondent_id = r.respondent_id
                WHERE a.question_id = :question_id
                        AND r.city_id IN ({amm_list})
                    AND rs.value = {sex_id}
        {initial_filter}
        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            grupo
        UNION ALL
        -- Regional rows: AMM / Periferia / Resto NL
        SELECT
            COALESCE(o.option_id, a.value) AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT)) AS Respuesta,
            CASE
                WHEN r.city_id IN ({amm_list}) THEN 'AMM'
                WHEN r.city_id IN ({periferia_list}) THEN 'Periferia'
                WHEN r.city_id NOT IN ({amm_plus_perif}) THEN 'Resto NL'
            END AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o ON a.question_id = o.question_id AND a.option_id = o.option_id
            LEFT JOIN respondent_attributes rs ON a.respondent_id = rs.respondent_id AND rs.attribute = 'sexo'
            JOIN responses r ON a.respondent_id = r.respondent_id
                WHERE a.question_id = :question_id
                        AND r.city_id IS NOT NULL
                    AND rs.value = {sex_id}
        {initial_filter}
        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            grupo
        UNION ALL
        -- Entire state row: Nuevo León (any non-null municipio)
        SELECT
            COALESCE(o.option_id, a.value) AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT)) AS Respuesta,
            'Nuevo León' AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o ON a.question_id = o.question_id AND a.option_id = o.option_id
            LEFT JOIN respondent_attributes rs ON a.respondent_id = rs.respondent_id AND rs.attribute = 'sexo'
            JOIN responses r ON a.respondent_id = r.respondent_id
                WHERE a.question_id = :question_id
                        AND r.city_id IS NOT NULL
                    AND rs.value = {sex_id}
        {initial_filter}
        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT))
    """
    return query


def get_edad_query(initial_only: bool = True) -> str:
    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    age_cases = ""
    start_index = 0 if not initial_only else 3
    for i in range(start_index, len(AGE_BINS) - 1):
        lower = AGE_BINS[i] + (0 if i == 0 else 1)
        upper = AGE_BINS[i + 1]
        label = AGE_LABELS[i]
        age_cases += (
            f"WHEN ra.value BETWEEN {lower} AND {upper} THEN '{label}'\n            "
        )

    query = f"""
        SELECT
            COALESCE(o.option_id, a.value) AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT)) AS Respuesta,
            CASE
            {age_cases}
            END AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o ON a.question_id = o.question_id AND a.option_id = o.option_id
        LEFT JOIN respondent_attributes ra ON a.respondent_id = ra.respondent_id AND ra.attribute = 'edad_anos'
        JOIN responses r ON a.respondent_id = r.respondent_id
        WHERE a.question_id = :question_id
          AND ra.value IS NOT NULL
        {initial_filter}
        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            grupo
    """
    return query


def get_totales_query(initial_only: bool = True) -> str:
    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    query = f"""
        SELECT
            COALESCE(o.option_id, a.value) AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT)) AS Respuesta,
            'Total' AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o ON a.question_id = o.question_id AND a.option_id = o.option_id
        JOIN responses r ON a.respondent_id = r.respondent_id
        WHERE a.question_id = :question_id
        {initial_filter}
        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT))
    """
    return query


def get_ingreso_query(initial_only: bool = True) -> str:
    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    query = f"""
        SELECT
            COALESCE(o.option_id, a.value) AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT)) AS Respuesta,
            oa.option_label AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o 
        ON a.question_id = o.question_id 
        AND a.option_id = o.option_id

        LEFT JOIN respondent_attributes ra 
        ON a.respondent_id = ra.respondent_id 
        AND ra.attribute = 'ingreso'

        LEFT JOIN options oa 
        ON ra.question_id = oa.question_id 
        AND ra.value = oa.option_id
        
        JOIN responses r ON a.respondent_id = r.respondent_id
        WHERE a.question_id = :question_id
        {initial_filter}
        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            oa.option_label
    """
    return query


def get_tipo_consulta_query(initial_only: bool = True) -> str:
    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    query = f"""
        SELECT
            COALESCE(o.option_id, a.value) AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT)) AS Respuesta,
            oa.option_label AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o 
        ON a.question_id = o.question_id 
        AND a.option_id = o.option_id

        LEFT JOIN respondent_attributes ra 
        ON a.respondent_id = ra.respondent_id 
        AND ra.attribute = 'tipo_consulta'

        LEFT JOIN options oa 
        ON ra.question_id = oa.question_id 
        AND ra.value = oa.option_id
        
        JOIN responses r ON a.respondent_id = r.respondent_id
        WHERE a.question_id = :question_id
        {initial_filter}
        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            oa.option_label
    """
    return query


def get_municipio_by_promedio_modo_transporte_query(initial_only: bool = True) -> str:
    """
    The table should look like:
    id_respuesta | Respuesta | City1 | City2 | ... | AMM | Resto NL | Nuevo León
    1            | Camina    |  avg  |  avg  | ... | avg |   avg    |   avg
    2            | Auto      |  avg  |  avg  | ... | avg |   avg    |   avg
    3            | Camion    |  avg  |  avg  | ... | avg |   avg    |   avg
    The averages should be calculated using the weight clause.
    Lets say we have the following data:
    Respondent | Modo Transporte | Municipio | Tiempo de Viaje | Weight
        1      |      Camina     |    City1  |       30       |   1.5
        2      |      Camina     |    City1  |       40       |   0.5
        3      |      Auto       |    City2  |       20       |   2.0
    The average time for Camina in City1 would be:
    (30*1.5 + 40*0.5) / (1.5 + 0.5) = 32.5
    """
    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    amm_list = ", ".join(map(str, AMM_ID))
    periferia_list = ", ".join(map(str, PERIFERIA_ID))
    amm_plus_perif = ", ".join(map(str, AMM_ID + PERIFERIA_ID))

    city_case = ""
    for city_id in AMM_ID:
        city_name = ID_TO_CITY_NAME[city_id]
        city_case += f"WHEN r.city_id = {city_id} THEN '{city_name}'\n            "

    query = f"""
        SELECT
            ra_modo.value AS id_respuesta,
            oa.option_label AS Respuesta,
            CASE
            {city_case}
            END AS grupo,
            SUM(CAST(a.value AS NUMERIC) * {weight}) / SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN respondent_attributes ra_modo
            ON a.respondent_id = ra_modo.respondent_id
            AND ra_modo.attribute = 'modo_transporte'
        LEFT JOIN options oa
            ON ra_modo.question_id = oa.question_id
            AND ra_modo.value = oa.option_id
        JOIN responses r
            ON a.respondent_id = r.respondent_id
        WHERE a.question_id = :question_id
          AND r.city_id IS NOT NULL
          AND ra_modo.value IS NOT NULL
          AND r.city_id IN ({amm_list})
        {initial_filter}
        GROUP BY
            ra_modo.value,
            oa.option_label,
            grupo

        UNION ALL

        SELECT
            ra_modo.value AS id_respuesta,
            oa.option_label AS Respuesta,
            CASE
                WHEN r.city_id IN ({amm_list}) THEN 'AMM'
                WHEN r.city_id IN ({periferia_list}) THEN 'Periferia'
                WHEN r.city_id NOT IN ({amm_plus_perif}) THEN 'Resto NL'
            END AS grupo,
            SUM(CAST(a.value AS NUMERIC) * {weight}) / SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN respondent_attributes ra_modo
            ON a.respondent_id = ra_modo.respondent_id
            AND ra_modo.attribute = 'modo_transporte'
        LEFT JOIN options oa
            ON ra_modo.question_id = oa.question_id
            AND ra_modo.value = oa.option_id
        JOIN responses r
            ON a.respondent_id = r.respondent_id
        WHERE a.question_id = :question_id
          AND r.city_id IS NOT NULL
          AND ra_modo.value IS NOT NULL
        {initial_filter}
        GROUP BY
            ra_modo.value,
            oa.option_label,
            grupo

        UNION ALL

        SELECT
            ra_modo.value AS id_respuesta,
            oa.option_label AS Respuesta,
            'Nuevo León' AS grupo,
            SUM(CAST(a.value AS NUMERIC) * {weight}) / SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN respondent_attributes ra_modo
            ON a.respondent_id = ra_modo.respondent_id
            AND ra_modo.attribute = 'modo_transporte'
        LEFT JOIN options oa
            ON ra_modo.question_id = oa.question_id
            AND ra_modo.value = oa.option_id
        JOIN responses r
            ON a.respondent_id = r.respondent_id
        WHERE a.question_id = :question_id
          AND r.city_id IS NOT NULL
          AND ra_modo.value IS NOT NULL
        {initial_filter}
        GROUP BY
            ra_modo.value,
            oa.option_label
    """

    return query


def get_ingreso_by_municipio_query(city_id: int, initial_only: bool = True) -> str:
    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    query = f"""
        SELECT
            COALESCE(o.option_id, a.value) AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT)) AS Respuesta,
            oa.option_label AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o 
        ON a.question_id = o.question_id 
        AND a.option_id = o.option_id

        LEFT JOIN respondent_attributes ra 
        ON a.respondent_id = ra.respondent_id 
        AND ra.attribute = 'ingreso'

        LEFT JOIN respondent_attributes rm 
        ON a.respondent_id = rm.respondent_id 
        AND rm.attribute = 'municipio'

        LEFT JOIN options oa 
        ON ra.question_id = oa.question_id 
        AND ra.value = oa.option_id
        
        JOIN responses r ON a.respondent_id = r.respondent_id
                WHERE a.question_id = :question_id
                    AND r.city_id = {city_id}
        {initial_filter}
        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            oa.option_label
    """
    return query


def get_ingreso_by_region(region_id: int, initial_only: bool = True) -> str:
    """
    1: AMM
    2: Periferia
    3: Resto NL
    """

    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)

    if region_id == 1:
        region_condition = f"r.city_id IN ({', '.join(map(str, AMM_ID))})"
    elif region_id == 2:
        region_condition = f"r.city_id IN ({', '.join(map(str, PERIFERIA_ID))})"
    elif region_id == 3:
        region_condition = (
            f"r.city_id NOT IN ({', '.join(map(str, AMM_ID + PERIFERIA_ID))})"
        )
    else:
        raise ValueError("Invalid region_id. Must be 1, 2, 3, or 4.")

    query = f"""
        SELECT
            COALESCE(o.option_id, a.value) AS id_respuesta,
            COALESCE(o.option_label, CAST(a.value AS TEXT)) AS Respuesta,
            oa.option_label AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN options o 
        ON a.question_id = o.question_id 
        AND a.option_id = o.option_id

        LEFT JOIN respondent_attributes ra 
        ON a.respondent_id = ra.respondent_id 
        AND ra.attribute = 'ingreso'

        

        LEFT JOIN options oa 
        ON ra.question_id = oa.question_id 
        AND ra.value = oa.option_id
        
        JOIN responses r ON a.respondent_id = r.respondent_id
                WHERE a.question_id = :question_id
                    AND {region_condition}
        {initial_filter}
        GROUP BY
            COALESCE(o.option_id, a.value),
            COALESCE(o.option_label, CAST(a.value AS TEXT)),
            oa.option_label
    """
    return query


def get_particion_modal_agregada_por_region_query(initial_only: bool = True) -> str:
    medios_motorizados_no_colectivos = [3, 4, 5, 9]
    medios_no_motorizados = [1, 6, 7]
    transporte_publico_colectivo = [2, 8, 10]
    transporte_privado_colectivo = [11, 12, 13]
    otros = [14, 15]

    weight = _get_weight_clause(initial_only)
    initial_filter = _get_initial_filter(initial_only)
    amm_list = ", ".join(map(str, AMM_ID))
    periferia_list = ", ".join(map(str, PERIFERIA_ID))
    amm_plus_perif = ", ".join(map(str, AMM_ID + PERIFERIA_ID))

    city_case = ""
    for city_id in AMM_ID:
        city_name = ID_TO_CITY_NAME[city_id]
        city_case += f"WHEN r.city_id = {city_id} THEN '{city_name}'\n            "

    mode_case = f"""CASE
                WHEN ra.value IN ({', '.join(map(str, medios_motorizados_no_colectivos))}) THEN 'Medios motorizados no colectivos'
                WHEN ra.value IN ({', '.join(map(str, medios_no_motorizados))}) THEN 'Medios no motorizados'
                WHEN ra.value IN ({', '.join(map(str, transporte_publico_colectivo))}) THEN 'Transporte publico colectivo'
                WHEN ra.value IN ({', '.join(map(str, transporte_privado_colectivo))}) THEN 'Transporte privado colectivo'
                WHEN ra.value IN ({', '.join(map(str, otros))}) THEN 'Otros'
                WHEN ra.value IN (8888) THEN 'No Sabe'
                WHEN ra.value IN (9999) THEN 'No Contesta'
            END"""

    query = f"""
        -- City-level rows (only for AMM municipalities)
        SELECT
            {mode_case} AS id_respuesta,
            {mode_case} AS Respuesta,
            CASE
            {city_case}
            END AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN respondent_attributes ra 
        ON a.respondent_id = ra.respondent_id 
        AND ra.attribute = 'modo_transporte'
        JOIN responses r ON a.respondent_id = r.respondent_id
        WHERE a.question_id = :question_id
          AND ra.value IS NOT NULL
             AND r.city_id IN ({amm_list})
        {initial_filter}
        GROUP BY
            id_respuesta,
            Respuesta,
            grupo

        UNION ALL

        -- Regional rows: AMM / Periferia / Resto NL
        SELECT
            {mode_case} AS id_respuesta,
            {mode_case} AS Respuesta,
            CASE
                    WHEN r.city_id IN ({amm_list}) THEN 'AMM'
                    WHEN r.city_id IN ({periferia_list}) THEN 'Periferia'
                    WHEN r.city_id NOT IN ({amm_plus_perif}) THEN 'Resto NL'
            END AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN respondent_attributes ra 
        ON a.respondent_id = ra.respondent_id 
        AND ra.attribute = 'modo_transporte'
        JOIN responses r ON a.respondent_id = r.respondent_id
                WHERE a.question_id = :question_id
                    AND ra.value IS NOT NULL
                    AND r.city_id IS NOT NULL
        {initial_filter}
        GROUP BY
            id_respuesta,
            Respuesta,
            grupo

        UNION ALL

        -- Entire state row: Nuevo León
        SELECT
            {mode_case} AS id_respuesta,
            {mode_case} AS Respuesta,
            'Nuevo León' AS grupo,
            SUM({weight}) AS valor
        FROM answers a
        LEFT JOIN respondent_attributes ra 
        ON a.respondent_id = ra.respondent_id 
        AND ra.attribute = 'modo_transporte'
        JOIN responses r ON a.respondent_id = r.respondent_id
                WHERE a.question_id = :question_id
                    AND ra.value IS NOT NULL
                    AND r.city_id IS NOT NULL
        {initial_filter}
        GROUP BY
            id_respuesta,
            Respuesta
    """
    return query


DISAGGREGATIONS_MAP = {
    "trabajo_remunerado": lambda initial_only: get_trabajo_remunerado_query(
        initial_only
    ),
    "trabajo_remunerado_por_hombres": lambda initial_only: get_trabajo_remunerado_by_sex_query(
        0, initial_only
    ),
    "trabajo_remunerado_por_mujeres": lambda initial_only: get_trabajo_remunerado_by_sex_query(
        1, initial_only
    ),
    "tipo_trabajo": lambda initial_only: get_tipo_trabajo_query(initial_only),
    "tipo_trabajo_por_hombres": lambda initial_only: get_tipo_trabajo_by_sex_query(
        0, initial_only
    ),
    "tipo_trabajo_por_mujeres": lambda initial_only: get_tipo_trabajo_by_sex_query(
        1, initial_only
    ),
    "afiliacion_servicio_salud": lambda initial_only: get_afiliacion_servicio_salud_query(
        initial_only
    ),
    "nivel_max_estudios": lambda initial_only: get_nivel_max_estudios_query(
        initial_only
    ),
    "servicio_salud_donde_se_atendio": lambda initial_only: get_servicio_salud_donde_se_atendio_query(
        initial_only
    ),
    "tipo_servicio_salud_donde_se_atendio": lambda initial_only: get_tipo_servicio_salud_donde_se_atendio_query(
        initial_only
    ),
    "tipo_escuela": lambda initial_only: get_tipo_escuela_query(initial_only),
    "nivel_actual_estudios": lambda initial_only: get_nivel_actual_estudios_query(
        initial_only
    ),
    "nivel_actual_estudios_por_escuela_privada": lambda initial_only: get_nivel_actual_estudios_by_tipo_escuela_query(
        2, initial_only
    ),
    "nivel_actual_estudios_por_escuela_publica": lambda initial_only: get_nivel_actual_estudios_by_tipo_escuela_query(
        1, initial_only
    ),
    "sexo": lambda initial_only: get_sexo_query(initial_only),
    "municipio": lambda initial_only: get_municipio_query(initial_only),
    "municipio_por_hombres": lambda initial_only: get_municipio_by_sex_query(
        0, initial_only
    ),
    "municipio_por_mujeres": lambda initial_only: get_municipio_by_sex_query(
        1, initial_only
    ),
    "edad": lambda initial_only: get_edad_query(initial_only),
    "totales": lambda initial_only: get_totales_query(initial_only),
    "ingreso": lambda initial_only: get_ingreso_query(initial_only),
    "tipo_consulta": lambda initial_only: get_tipo_consulta_query(initial_only),
    "promedio_modo_transporte_y_municipio": lambda initial_only: get_municipio_by_promedio_modo_transporte_query(
        initial_only
    ),
    "ingreso_por_apodaca": lambda initial_only: get_ingreso_by_municipio_query(
        6, initial_only
    ),
    "ingreso_por_guadalupe": lambda initial_only: get_ingreso_by_municipio_query(
        26, initial_only
    ),
    "ingreso_por_juarez": lambda initial_only: get_ingreso_by_municipio_query(
        31, initial_only
    ),
    "ingreso_por_monterrey": lambda initial_only: get_ingreso_by_municipio_query(
        39, initial_only
    ),
    "ingreso_por_san_nicolas": lambda initial_only: get_ingreso_by_municipio_query(
        46, initial_only
    ),
    "ingreso_por_san_pedro": lambda initial_only: get_ingreso_by_municipio_query(
        19, initial_only
    ),
    "ingreso_por_santiago": lambda initial_only: get_ingreso_by_municipio_query(
        49, initial_only
    ),
    "ingreso_por_cadereyta": lambda initial_only: get_ingreso_by_municipio_query(
        9, initial_only
    ),
    "ingreso_por_santa_catarina": lambda initial_only: get_ingreso_by_municipio_query(
        48, initial_only
    ),
    "ingreso_por_garcia": lambda initial_only: get_ingreso_by_municipio_query(
        18, initial_only
    ),
    "ingreso_por_escobedo": lambda initial_only: get_ingreso_by_municipio_query(
        21, initial_only
    ),
    "ingreso_por_region_amm": lambda initial_only: get_ingreso_by_region(
        1, initial_only
    ),
    "ingreso_por_region_periferia": lambda initial_only: get_ingreso_by_region(
        2, initial_only
    ),
    "ingreso_por_region_resto_nl": lambda initial_only: get_ingreso_by_region(
        3, initial_only
    ),
    "particion_modal_agregada_por_municipio": lambda initial_only: get_particion_modal_agregada_por_region_query(
        initial_only
    ),
}

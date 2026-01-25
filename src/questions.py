def get_question_sections_query() -> str:
    query = """
        SELECT DISTINCT
            q_section AS section
        FROM questions
        WHERE section IS NOT NULL
        ORDER BY section;
    """
    return query


def get_questions_by_section_query(section: str) -> str:
    query = f"""
        SELECT
            q_id AS id,
            q_text,
            q_type AS type,
            q_notes
        FROM questions
        WHERE q_section = '{section}'
        ORDER BY id;
    """
    return query

from src.database import get_connection
from src.builder import build_section_report
from src.repository import get_question_sections


def main():
    conn = get_connection()

    sections = get_question_sections(conn)

    for section in sections:
        build_section_report(conn, section)
        print(f"Report generated for {section} section.")

    conn.close()


if __name__ == "__main__":
    main()

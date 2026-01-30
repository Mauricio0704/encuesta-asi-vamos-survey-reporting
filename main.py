import argparse

from src.database import get_connection
from src.builder import build_section_report, build_topics_workbook
from src.repository import get_question_sections


def main():
    parser = argparse.ArgumentParser(description="Generar reportes de tabulados")
    parser.add_argument(
        "--reporte",
        choices=["temas", "temas_unico"],
        default="temas",
        help="temas: un archivo por tema; temas_unico: un solo archivo con hojas por tema",
    )
    args = parser.parse_args()

    conn = get_connection()

    sections = get_question_sections(conn)

    if args.reporte == "temas_unico":
        build_topics_workbook(conn, sections)
        print("Reporte generado: un solo archivo con hojas por tema.")
    else:
        for section in sections:
            build_section_report(conn, section)
            print(f"Report generated for {section} section.")

    conn.close()


if __name__ == "__main__":
    main()

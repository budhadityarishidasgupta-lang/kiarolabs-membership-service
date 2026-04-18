from fastapi import HTTPException

from app.database import get_connection
from app.ingestion.maths.parser import parse_math_pdf


def ingest_math_pdf(file_path: str, paper_code: str) -> int:
    questions = parse_math_pdf(file_path, paper_code)
    conn = get_connection()
    cur = conn.cursor()
    inserted = 0

    try:
        cur.execute(
            """
            SELECT 1 FROM math_printable_papers
            WHERE paper_code = %s
            """,
            (paper_code,),
        )

        if not cur.fetchone():
            raise HTTPException(
                status_code=400,
                detail="Invalid paper_code - must exist in master table",
            )

        for question in questions:
            cur.execute(
                """
                INSERT INTO math_printable_questions
                (paper_code, question_number, question_text)
                SELECT %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM math_printable_questions
                    WHERE paper_code = %s
                      AND question_number = %s
                )
                ON CONFLICT DO NOTHING
                """,
                (
                    question["paper_code"],
                    question["question_number"],
                    question["question_text"],
                    question["paper_code"],
                    question["question_number"],
                ),
            )
            inserted += cur.rowcount

        conn.commit()
        return inserted
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

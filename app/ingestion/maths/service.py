import logging

from fastapi import HTTPException

from app.database import get_connection
from app.ingestion.maths.parser import parse_math_pdf


logger = logging.getLogger(__name__)


def ingest_math_pdf(file_path: str, paper_code: str) -> int:
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
            logger.warning("Printable maths PDF upload rejected for invalid paper_code")
            raise HTTPException(status_code=400, detail="Invalid paper_code")

        cur.execute(
            """
            SELECT COUNT(*)
            FROM math_printable_questions
            WHERE paper_code = %s
            """,
            (paper_code,),
        )
        existing_questions = cur.fetchone()[0]

        if existing_questions:
            logger.warning("Printable maths PDF upload rejected because questions already exist")
            raise HTTPException(
                status_code=400,
                detail="Questions already exist for this paper. Delete first before re-uploading.",
            )

        questions = parse_math_pdf(file_path, paper_code)

        if not questions:
            logger.warning("Printable maths PDF parse returned no questions")
            raise HTTPException(status_code=400, detail="No questions parsed from PDF")

        question_numbers = [question.get("question_number") for question in questions]
        if len(question_numbers) != len(set(question_numbers)):
            logger.warning("Printable maths PDF has duplicate question numbers")
            raise HTTPException(status_code=400, detail="Duplicate question numbers detected")

        if any(not str(question.get("question_text") or "").strip() for question in questions):
            logger.warning("Printable maths PDF has one or more blank questions")
            raise HTTPException(status_code=400, detail="One or more questions are blank")

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

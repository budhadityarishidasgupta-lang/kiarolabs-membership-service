from fastapi import HTTPException, UploadFile

from app.database import get_connection
from app.ingestion.comprehension.parser import (
    parse_comprehension_csv,
    parse_comprehension_pdf,
)


def _get_or_create_passage(cur, title, passage_text, difficulty):
    cur.execute(
        """
        INSERT INTO comprehension_passages (title, passage_text, difficulty, word_count)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (title) DO NOTHING
        RETURNING passage_id
        """,
        (title, passage_text, difficulty, len((passage_text or "").split())),
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        """
        SELECT passage_id
        FROM comprehension_passages
        WHERE title = %s
        """,
        (title,),
    )
    existing = cur.fetchone()
    return existing[0] if existing else None


def ingest_comprehension_file(file: UploadFile, paper_code: str) -> int:
    filename = (file.filename or "").lower()

    if filename.endswith(".csv"):
        content = file.file.read().decode("utf-8-sig")
        passages = parse_comprehension_csv(content, paper_code)
    elif filename.endswith(".pdf"):
        passages = parse_comprehension_pdf(file.file.name, paper_code)
    else:
        raise HTTPException(status_code=400, detail="Only CSV and PDF files are supported")

    conn = get_connection()
    cur = conn.cursor()
    inserted_questions = 0

    try:
        for passage in passages:
            passage_id = _get_or_create_passage(
                cur,
                passage["title"] or f"{paper_code} Passage",
                passage["passage"],
                passage.get("difficulty"),
            )

            if not passage_id:
                continue

            for question in passage["questions"]:
                cur.execute(
                    """
                    INSERT INTO comprehension_questions
                    (passage_id, question_text, option_a, option_b, option_c, option_d, correct_answer, question_type, sort_order)
                    SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM comprehension_questions
                        WHERE passage_id = %s
                          AND sort_order = %s
                          AND question_text = %s
                    )
                    """,
                    (
                        passage_id,
                        question["question_text"],
                        question.get("option_a"),
                        question.get("option_b"),
                        question.get("option_c"),
                        question.get("option_d"),
                        question.get("correct_answer"),
                        question.get("question_type") or "comprehension",
                        question.get("sort_order") or 0,
                        passage_id,
                        question.get("sort_order") or 0,
                        question["question_text"],
                    ),
                )
                inserted_questions += cur.rowcount

        conn.commit()
        return inserted_questions
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


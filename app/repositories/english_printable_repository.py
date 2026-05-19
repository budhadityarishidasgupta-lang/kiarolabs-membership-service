from __future__ import annotations

from typing import Any

from app.database import get_connection


DEFAULT_ENGLISH_PAPERS = [
    (
        f"ENG-P{index}",
        f"English Practice Paper {str(index).zfill(2)}",
        "Printable English paper with upload-based marking.",
        None,
        index,
    )
    for index in range(1, 11)
]

ENGLISH_UNLOCK_CODES = {"english_printables", "english_single_paper"}
ENGLISH_EXPECTED_QUESTION_COUNT = 50


def normalize_english_paper_code(paper_code: str) -> str:
    value = (paper_code or "").strip().upper()
    if value.startswith("ENG-P"):
        suffix = value[5:]
        if suffix.isdigit():
            return f"ENG-P{int(suffix)}"
    if value.startswith("ENG-"):
        suffix = value[4:]
        if suffix.isdigit():
            return f"ENG-P{int(suffix)}"
    if value.startswith("ENGP") and value[4:].isdigit():
        return f"ENG-P{int(value[4:])}"
    return value


def init_english_printable_tables(conn=None) -> None:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()

    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS english_papers (
                id SERIAL PRIMARY KEY,
                paper_code TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                pdf_url TEXT,
                answer_key_uploaded BOOLEAN NOT NULL DEFAULT FALSE,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS english_questions (
                id SERIAL PRIMARY KEY,
                paper_code TEXT NOT NULL REFERENCES english_papers(paper_code) ON DELETE CASCADE,
                question_number INTEGER NOT NULL,
                question_text TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (paper_code, question_number)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS english_answers (
                id SERIAL PRIMARY KEY,
                paper_code TEXT NOT NULL REFERENCES english_papers(paper_code) ON DELETE CASCADE,
                question_number INTEGER NOT NULL,
                correct_answer TEXT NOT NULL,
                explanation TEXT,
                answer_source TEXT NOT NULL DEFAULT 'admin_csv',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (paper_code, question_number)
            )
            """
        )
        cur.execute("ALTER TABLE english_answers ADD COLUMN IF NOT EXISTS explanation TEXT")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS english_attempts (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                paper_code TEXT NOT NULL REFERENCES english_papers(paper_code) ON DELETE CASCADE,
                question_number INTEGER NOT NULL,
                student_answer TEXT NOT NULL,
                is_correct BOOLEAN NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.executemany(
            """
            INSERT INTO english_papers (paper_code, title, description, pdf_url, sort_order)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (paper_code) DO UPDATE
            SET title = EXCLUDED.title,
                description = EXCLUDED.description,
                sort_order = EXCLUDED.sort_order
            """,
            DEFAULT_ENGLISH_PAPERS,
        )
        conn.commit()
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def bulk_upsert_english_questions(rows: list[dict], conn=None) -> dict:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    inserted = updated = existing = 0
    try:
        for row in rows:
            paper_code = normalize_english_paper_code(row["paper_code"])
            question_number = int(row["question_number"])
            question_text = str(row.get("question_text") or f"Question {question_number}").strip()
            cur.execute(
                """
                SELECT question_text
                FROM english_questions
                WHERE paper_code = %s AND question_number = %s
                """,
                (paper_code, question_number),
            )
            current = cur.fetchone()
            if not current:
                cur.execute(
                    """
                    INSERT INTO english_questions (paper_code, question_number, question_text)
                    VALUES (%s, %s, %s)
                    """,
                    (paper_code, question_number, question_text),
                )
                inserted += 1
            elif str(current[0] or "").strip() == question_text:
                existing += 1
            else:
                cur.execute(
                    """
                    UPDATE english_questions
                    SET question_text = %s
                    WHERE paper_code = %s AND question_number = %s
                    """,
                    (question_text, paper_code, question_number),
                )
                updated += 1
        if owns_connection:
            conn.commit()
        return {"inserted": inserted, "updated": updated, "existing": existing}
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def bulk_upsert_english_answers(rows: list[dict], conn=None) -> dict:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    inserted = updated = unchanged = 0
    try:
        for row in rows:
            paper_code = normalize_english_paper_code(row["paper_code"])
            question_number = int(row["question_number"])
            correct_answer = str(row["correct_answer"]).strip()
            explanation = str(row.get("explanation") or "").strip()
            answer_source = str(row.get("answer_source") or "admin_csv").strip()
            cur.execute(
                """
                SELECT correct_answer, COALESCE(explanation, '')
                FROM english_answers
                WHERE paper_code = %s AND question_number = %s
                """,
                (paper_code, question_number),
            )
            existing_row = cur.fetchone()
            if not existing_row:
                cur.execute(
                    """
                    INSERT INTO english_answers (paper_code, question_number, correct_answer, explanation, answer_source)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (paper_code, question_number, correct_answer, explanation, answer_source),
                )
                inserted += 1
            elif str(existing_row[0] or "").strip().lower() == correct_answer.lower() and str(existing_row[1] or "").strip() == explanation:
                unchanged += 1
            else:
                cur.execute(
                    """
                    UPDATE english_answers
                    SET correct_answer = %s,
                        explanation = %s,
                        answer_source = %s,
                        updated_at = NOW()
                    WHERE paper_code = %s AND question_number = %s
                    """,
                    (correct_answer, explanation, answer_source, paper_code, question_number),
                )
                updated += 1

        cur.execute(
            """
            UPDATE english_papers p
            SET answer_key_uploaded = EXISTS (
                SELECT 1 FROM english_answers a WHERE a.paper_code = p.paper_code
            )
            WHERE p.paper_code = ANY(%s)
            """,
            (sorted({normalize_english_paper_code(row["paper_code"]) for row in rows}),),
        )
        if owns_connection:
            conn.commit()
        return {"inserted": inserted, "updated": updated, "unchanged": unchanged}
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def get_active_english_papers(*, conn=None) -> list[dict]:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                p.id,
                p.paper_code,
                p.title,
                p.description,
                p.pdf_url,
                p.answer_key_uploaded,
                p.is_active,
                p.created_at,
                p.sort_order,
                COALESCE(q.questions_count, 0) AS questions_count,
                COALESCE(a.answers_count, 0) AS answers_count
            FROM english_papers p
            LEFT JOIN (
                SELECT paper_code, COUNT(*) AS questions_count
                FROM english_questions
                GROUP BY paper_code
            ) q ON q.paper_code = p.paper_code
            LEFT JOIN (
                SELECT paper_code, COUNT(*) AS answers_count
                FROM english_answers
                GROUP BY paper_code
            ) a ON a.paper_code = p.paper_code
            WHERE p.is_active = TRUE
            ORDER BY p.sort_order ASC, p.paper_code ASC
            """
        )
        rows = cur.fetchall()
        return [
            {
                "id": row[0],
                "paper_code": row[1],
                "title": row[2],
                "description": row[3],
                "pdf_url": row[4],
                "answer_key_uploaded": row[5],
                "is_active": row[6],
                "created_at": row[7].isoformat() if row[7] else None,
                "sort_order": row[8],
                "questions_count": row[9],
                "answers_count": row[10],
                "effective_question_count": max(row[9], row[10]),
                "ready": max(row[9], row[10]) == ENGLISH_EXPECTED_QUESTION_COUNT and row[10] == ENGLISH_EXPECTED_QUESTION_COUNT,
            }
            for row in rows
        ]
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def get_english_paper_meta(paper_code: str, *, conn=None) -> dict | None:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    try:
        normalized = normalize_english_paper_code(paper_code)
        cur.execute(
            """
            SELECT
                p.paper_code,
                p.title,
                p.description,
                p.pdf_url,
                p.answer_key_uploaded,
                p.is_active,
                p.sort_order,
                COALESCE(q.questions_count, 0) AS questions_count,
                COALESCE(a.answers_count, 0) AS answers_count
            FROM english_papers p
            LEFT JOIN (
                SELECT paper_code, COUNT(*) AS questions_count
                FROM english_questions
                GROUP BY paper_code
            ) q ON q.paper_code = p.paper_code
            LEFT JOIN (
                SELECT paper_code, COUNT(*) AS answers_count
                FROM english_answers
                GROUP BY paper_code
            ) a ON a.paper_code = p.paper_code
            WHERE p.paper_code = %s
            LIMIT 1
            """,
            (normalized,),
        )
        row = cur.fetchone()
        if not row:
            return None
        effective_question_count = max(int(row[7] or 0), int(row[8] or 0))
        return {
            "paper_code": row[0],
            "title": row[1],
            "description": row[2],
            "pdf_url": row[3],
            "answer_key_uploaded": row[4],
            "is_active": row[5],
            "sort_order": row[6],
            "questions_count": int(row[7] or 0),
            "answers_count": int(row[8] or 0),
            "effective_question_count": effective_question_count,
            "ready": effective_question_count == ENGLISH_EXPECTED_QUESTION_COUNT and int(row[8] or 0) == ENGLISH_EXPECTED_QUESTION_COUNT,
        }
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def get_english_questions_for_paper(paper_code: str, *, conn=None) -> list[dict]:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    try:
        normalized = normalize_english_paper_code(paper_code)
        cur.execute(
            """
            SELECT question_number, question_text
            FROM english_questions
            WHERE paper_code = %s
            ORDER BY question_number ASC
            """,
            (normalized,),
        )
        rows = cur.fetchall()
        if not rows:
            cur.execute(
                """
                SELECT question_number
                FROM english_answers
                WHERE paper_code = %s
                ORDER BY question_number ASC
                """,
                (normalized,),
            )
            rows = [(row[0], f"Question {row[0]}") for row in cur.fetchall()]
        return [
            {
                "question_number": int(row[0]),
                "question_text": row[1],
            }
            for row in rows
        ]
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def get_english_answers_for_paper(paper_code: str, *, conn=None) -> list[dict]:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT question_number, correct_answer, COALESCE(explanation, ''), answer_source
            FROM english_answers
            WHERE paper_code = %s
            ORDER BY question_number ASC
            """,
            (normalize_english_paper_code(paper_code),),
        )
        return [
            {"question_number": int(row[0]), "correct_answer": row[1], "explanation": row[2], "answer_source": row[3]}
            for row in cur.fetchall()
        ]
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def record_english_attempt_batch(*, user_id: int, paper_code: str, answers: list[dict], conn=None) -> None:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    try:
        cur.executemany(
            """
            INSERT INTO english_attempts (user_id, paper_code, question_number, student_answer, is_correct)
            VALUES (%s, %s, %s, %s, %s)
            """,
            [
                (
                    user_id,
                    normalize_english_paper_code(paper_code),
                    int(item["question_number"]),
                    str(item["student_answer"]),
                    bool(item["is_correct"]),
                )
                for item in answers
            ],
        )
        if owns_connection:
            conn.commit()
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def user_has_english_printable_access(*, user_email: str, user_role: str | None = None, conn=None) -> bool:
    if (user_role or "").lower() == "admin":
        return True

    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT m.id
            FROM kiaro_membership.members m
            WHERE LOWER(m.email) = LOWER(%s)
            LIMIT 1
            """,
            (user_email,),
        )
        row = cur.fetchone()
        if not row:
            return False
        member_id = int(row[0])
        cur.execute(
            """
            SELECT app_code
            FROM kiaro_membership.member_apps
            WHERE member_id = %s
            """,
            (member_id,),
        )
        codes = {str(item[0]).strip().lower() for item in cur.fetchall()}
        return bool(codes & ENGLISH_UNLOCK_CODES)
    finally:
        cur.close()
        if owns_connection:
            conn.close()

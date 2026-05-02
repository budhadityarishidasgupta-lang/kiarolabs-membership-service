from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.database import get_connection


DEFAULT_VR_PAPERS = [
    ("VR-P1", "Verbal Reasoning Practice Paper 01", "GL-style verbal reasoning printable paper.", None, 1),
    ("VR-P2", "Verbal Reasoning Practice Paper 02", "GL-style verbal reasoning printable paper.", None, 2),
    ("VR-P3", "Verbal Reasoning Practice Paper 03", "GL-style verbal reasoning printable paper.", None, 3),
    ("VR-P4", "Verbal Reasoning Practice Paper 04", "GL-style verbal reasoning printable paper.", None, 4),
    ("VR-P5", "Verbal Reasoning Practice Paper 05", "GL-style verbal reasoning printable paper.", None, 5),
    ("VR-P6", "Verbal Reasoning Practice Paper 06", "GL-style verbal reasoning printable paper.", None, 6),
    ("VR-P7", "Verbal Reasoning Practice Paper 07", "GL-style verbal reasoning printable paper.", None, 7),
    ("VR-P8", "Verbal Reasoning Practice Paper 08", "GL-style verbal reasoning printable paper.", None, 8),
    ("VR-P9", "Verbal Reasoning Practice Paper 09", "GL-style verbal reasoning printable paper.", None, 9),
    ("VR-P10", "Verbal Reasoning Practice Paper 10", "GL-style verbal reasoning printable paper.", None, 10),
]

VR_UNLOCK_CODES = {"practice", "vr_printables", "vr_single_paper", "vr_starter_pack", "vr_complete_pack"}


@dataclass
class VrPaperMeta:
    id: int
    paper_code: str
    title: str
    description: str | None
    pdf_url: str | None
    answer_key_uploaded: bool
    is_active: bool
    created_at: Any
    sort_order: int
    questions_count: int
    answers_count: int

    @property
    def ready(self) -> bool:
        effective_count = max(self.questions_count, self.answers_count)
        return effective_count > 0 and self.answers_count == effective_count


def init_vr_tables(conn=None) -> None:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()

    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS vr_papers (
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
            CREATE TABLE IF NOT EXISTS vr_questions (
                id SERIAL PRIMARY KEY,
                paper_code TEXT NOT NULL REFERENCES vr_papers(paper_code) ON DELETE CASCADE,
                question_number INTEGER NOT NULL,
                question_type TEXT,
                question_text TEXT,
                option_a TEXT,
                option_b TEXT,
                option_c TEXT,
                option_d TEXT,
                option_e TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (paper_code, question_number)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS vr_answers (
                id SERIAL PRIMARY KEY,
                paper_code TEXT NOT NULL REFERENCES vr_papers(paper_code) ON DELETE CASCADE,
                question_number INTEGER NOT NULL,
                correct_answer TEXT NOT NULL,
                answer_source TEXT NOT NULL DEFAULT 'admin_csv',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (paper_code, question_number)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS vr_attempts (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                paper_code TEXT NOT NULL REFERENCES vr_papers(paper_code) ON DELETE CASCADE,
                question_number INTEGER NOT NULL,
                student_answer TEXT NOT NULL,
                is_correct BOOLEAN NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.executemany(
            """
            INSERT INTO vr_papers (paper_code, title, description, pdf_url, sort_order)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (paper_code) DO UPDATE
            SET title = EXCLUDED.title,
                description = EXCLUDED.description,
                sort_order = EXCLUDED.sort_order
            """,
            DEFAULT_VR_PAPERS,
        )
        conn.commit()
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def normalize_vr_paper_code(paper_code: str) -> str:
    value = (paper_code or "").strip().upper()
    if value.startswith("VR-"):
        suffix = value[3:]
        if suffix.isdigit():
            return f"VR-P{int(suffix)}"
    if value.startswith("VRP") and value[3:].isdigit():
        return f"VR-P{int(value[3:])}"
    return value


def create_or_update_vr_paper(*, paper_code: str, title: str, description: str | None = None, pdf_url: str | None = None, answer_key_uploaded: bool | None = None, is_active: bool = True, sort_order: int | None = None, conn=None) -> dict:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    try:
        normalized = normalize_vr_paper_code(paper_code)
        cur.execute(
            """
            INSERT INTO vr_papers (paper_code, title, description, pdf_url, answer_key_uploaded, is_active, sort_order)
            VALUES (%s, %s, %s, %s, COALESCE(%s, FALSE), %s, COALESCE(%s, 0))
            ON CONFLICT (paper_code) DO UPDATE
            SET title = EXCLUDED.title,
                description = EXCLUDED.description,
                pdf_url = COALESCE(EXCLUDED.pdf_url, vr_papers.pdf_url),
                answer_key_uploaded = COALESCE(%s, vr_papers.answer_key_uploaded),
                is_active = EXCLUDED.is_active,
                sort_order = COALESCE(%s, vr_papers.sort_order)
            RETURNING id, paper_code
            """,
            (
                normalized,
                title,
                description,
                pdf_url,
                answer_key_uploaded,
                is_active,
                sort_order,
                answer_key_uploaded,
                sort_order,
            ),
        )
        row = cur.fetchone()
        if owns_connection:
            conn.commit()
        return {"id": row[0], "paper_code": row[1]}
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def bulk_upsert_vr_questions(rows: list[dict], conn=None) -> dict:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    inserted = updated = unchanged = 0
    try:
        for row in rows:
            paper_code = normalize_vr_paper_code(row["paper_code"])
            question_number = int(row["question_number"])
            question_text = row.get("question_text") or f"Question {question_number}"
            cur.execute(
                """
                SELECT question_type, question_text, option_a, option_b, option_c, option_d, option_e
                FROM vr_questions
                WHERE paper_code = %s AND question_number = %s
                """,
                (paper_code, question_number),
            )
            existing = cur.fetchone()

            cur.execute(
                """
                INSERT INTO vr_questions
                (paper_code, question_number, question_type, question_text, option_a, option_b, option_c, option_d, option_e)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (paper_code, question_number) DO UPDATE
                SET question_type = COALESCE(EXCLUDED.question_type, vr_questions.question_type),
                    question_text = COALESCE(EXCLUDED.question_text, vr_questions.question_text),
                    option_a = COALESCE(EXCLUDED.option_a, vr_questions.option_a),
                    option_b = COALESCE(EXCLUDED.option_b, vr_questions.option_b),
                    option_c = COALESCE(EXCLUDED.option_c, vr_questions.option_c),
                    option_d = COALESCE(EXCLUDED.option_d, vr_questions.option_d),
                    option_e = COALESCE(EXCLUDED.option_e, vr_questions.option_e)
                """,
                (
                    paper_code,
                    question_number,
                    row.get("question_type"),
                    question_text,
                    row.get("option_a"),
                    row.get("option_b"),
                    row.get("option_c"),
                    row.get("option_d"),
                    row.get("option_e"),
                ),
            )
            incoming_signature = (
                row.get("question_type"),
                question_text,
                row.get("option_a"),
                row.get("option_b"),
                row.get("option_c"),
                row.get("option_d"),
                row.get("option_e"),
            )
            if not existing:
                inserted += 1
            elif tuple(existing) == incoming_signature:
                unchanged += 1
            else:
                updated += 1
        if owns_connection:
            conn.commit()
        return {"inserted": inserted, "updated": updated, "existing": unchanged}
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def bulk_upsert_vr_answers(rows: list[dict], conn=None) -> dict:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    inserted = updated = unchanged = 0
    try:
        for row in rows:
            paper_code = normalize_vr_paper_code(row["paper_code"])
            question_number = int(row["question_number"])
            correct_answer = str(row["correct_answer"]).strip().upper()
            answer_source = row.get("answer_source") or "admin_csv"
            cur.execute(
                """
                SELECT id, correct_answer
                FROM vr_answers
                WHERE paper_code = %s AND question_number = %s
                """,
                (paper_code, question_number),
            )
            existing = cur.fetchone()
            if not existing:
                cur.execute(
                    """
                    INSERT INTO vr_answers (paper_code, question_number, correct_answer, answer_source)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (paper_code, question_number, correct_answer, answer_source),
                )
                inserted += 1
            elif (existing[1] or "").strip().upper() == correct_answer:
                unchanged += 1
            else:
                cur.execute(
                    """
                    UPDATE vr_answers
                    SET correct_answer = %s,
                        answer_source = %s,
                        updated_at = NOW()
                    WHERE paper_code = %s AND question_number = %s
                    """,
                    (correct_answer, answer_source, paper_code, question_number),
                )
                updated += 1

        cur.execute(
            """
            UPDATE vr_papers p
            SET answer_key_uploaded = EXISTS (
                SELECT 1 FROM vr_answers a WHERE a.paper_code = p.paper_code
            )
            WHERE p.paper_code = ANY(%s)
            """,
            (sorted({normalize_vr_paper_code(row["paper_code"]) for row in rows}),),
        )
        if owns_connection:
            conn.commit()
        return {"inserted": inserted, "updated": updated, "unchanged": unchanged}
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def get_active_vr_papers(*, conn=None) -> list[dict]:
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
            FROM vr_papers p
            LEFT JOIN (
                SELECT paper_code, COUNT(*) AS questions_count
                FROM vr_questions
                GROUP BY paper_code
            ) q ON q.paper_code = p.paper_code
            LEFT JOIN (
                SELECT paper_code, COUNT(*) AS answers_count
                FROM vr_answers
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
                "ready": max(row[9], row[10]) > 0 and row[10] == max(row[9], row[10]),
            }
            for row in rows
        ]
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def get_vr_paper_meta(paper_code: str, *, conn=None) -> dict | None:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    try:
        normalized = normalize_vr_paper_code(paper_code)
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
            FROM vr_papers p
            LEFT JOIN (
                SELECT paper_code, COUNT(*) AS questions_count
                FROM vr_questions
                GROUP BY paper_code
            ) q ON q.paper_code = p.paper_code
            LEFT JOIN (
                SELECT paper_code, COUNT(*) AS answers_count
                FROM vr_answers
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
        return {
            "paper_code": row[0],
            "title": row[1],
            "description": row[2],
            "pdf_url": row[3],
            "answer_key_uploaded": row[4],
            "is_active": row[5],
            "sort_order": row[6],
            "questions_count": row[7],
            "answers_count": row[8],
            "effective_question_count": max(row[7], row[8]),
            "ready": max(row[7], row[8]) > 0 and max(row[7], row[8]) == row[8],
        }
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def get_vr_questions_for_paper(paper_code: str, *, conn=None) -> list[dict]:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    try:
        normalized = normalize_vr_paper_code(paper_code)
        cur.execute(
            """
            SELECT question_number, question_type, question_text, option_a, option_b, option_c, option_d, option_e
            FROM vr_questions
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
                FROM vr_answers
                WHERE paper_code = %s
                ORDER BY question_number ASC
                """,
                (normalized,),
            )
            rows = [(row[0], None, f"Question {row[0]}", None, None, None, None, None) for row in cur.fetchall()]
        return [
            {
                "question_number": row[0],
                "question_type": row[1],
                "question_text": row[2],
                "option_a": row[3],
                "option_b": row[4],
                "option_c": row[5],
                "option_d": row[6],
                "option_e": row[7],
            }
            for row in rows
        ]
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def get_vr_answer(paper_code: str, question_number: int, *, conn=None) -> str | None:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT correct_answer
            FROM vr_answers
            WHERE paper_code = %s AND question_number = %s
            """,
            (normalize_vr_paper_code(paper_code), int(question_number)),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def get_vr_answers_for_paper(paper_code: str, *, conn=None) -> list[dict]:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT question_number, correct_answer, answer_source
            FROM vr_answers
            WHERE paper_code = %s
            ORDER BY question_number ASC
            """,
            (normalize_vr_paper_code(paper_code),),
        )
        return [
            {"question_number": row[0], "correct_answer": row[1], "answer_source": row[2]}
            for row in cur.fetchall()
        ]
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def delete_vr_paper_content(paper_code: str, *, conn=None) -> dict:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    try:
        normalized = normalize_vr_paper_code(paper_code)
        cur.execute("DELETE FROM vr_questions WHERE paper_code = %s", (normalized,))
        questions_deleted = cur.rowcount
        cur.execute("DELETE FROM vr_answers WHERE paper_code = %s", (normalized,))
        answers_deleted = cur.rowcount
        cur.execute(
            """
            UPDATE vr_papers
            SET answer_key_uploaded = FALSE
            WHERE paper_code = %s
            """,
            (normalized,),
        )
        if owns_connection:
            conn.commit()
        return {
            "paper_code": normalized,
            "questions_deleted": questions_deleted,
            "answers_deleted": answers_deleted,
        }
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def record_vr_attempt(*, user_id: int, paper_code: str, question_number: int, student_answer: str, is_correct: bool, conn=None) -> None:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO vr_attempts (user_id, paper_code, question_number, student_answer, is_correct)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (user_id, normalize_vr_paper_code(paper_code), int(question_number), student_answer, bool(is_correct)),
        )
        if owns_connection:
            conn.commit()
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def record_vr_attempt_batch(*, user_id: int, paper_code: str, answers: list[dict], conn=None) -> None:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    try:
        cur.executemany(
            """
            INSERT INTO vr_attempts (user_id, paper_code, question_number, student_answer, is_correct)
            VALUES (%s, %s, %s, %s, %s)
            """,
            [
                (
                    user_id,
                    normalize_vr_paper_code(paper_code),
                    int(item["question_number"]),
                    item["student_answer"],
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


def get_vr_score_for_user(*, user_id: int, paper_code: str, conn=None) -> dict:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                COUNT(*) AS answered,
                COALESCE(SUM(CASE WHEN is_correct THEN 1 ELSE 0 END), 0) AS correct
            FROM vr_attempts
            WHERE user_id = %s
              AND paper_code = %s
            """,
            (user_id, normalize_vr_paper_code(paper_code)),
        )
        row = cur.fetchone()
        answered = row[0] or 0
        correct = row[1] or 0
        percentage = round((correct * 100.0 / answered), 2) if answered else 0.0
        return {"answered": answered, "correct": correct, "percentage": percentage}
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def user_has_vr_access(*, user_email: str, user_role: str | None = None, conn=None) -> bool:
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
            """,
            (user_email,),
        )
        row = cur.fetchone()
        if not row:
            return False
        member_id = row[0]
        cur.execute(
            """
            SELECT app_code
            FROM kiaro_membership.member_apps
            WHERE member_id = %s
            """,
            (member_id,),
        )
        codes = {str(item[0]).strip().lower() for item in cur.fetchall()}
        return bool(codes & VR_UNLOCK_CODES)
    finally:
        cur.close()
        if owns_connection:
            conn.close()

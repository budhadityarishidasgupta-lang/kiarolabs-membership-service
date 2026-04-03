# app/comprehension/repository.py

from app.database import get_connection


# =========================
# Helper: Row → Dict
# =========================
def row_to_dict(row):
    if hasattr(row, "_mapping"):
        return dict(row._mapping)
    if isinstance(row, dict):
        return row
    return {}


# =========================
# PASSAGES
# =========================

def get_active_passages():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT passage_id, title, difficulty
        FROM comprehension_passages
        WHERE is_active = TRUE
        ORDER BY created_at DESC;
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return [row_to_dict(r) for r in rows]


def get_passage_by_id(passage_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM comprehension_passages
        WHERE passage_id = %s;
    """, (passage_id,))

    row = cur.fetchone()

    cur.close()
    conn.close()

    return row_to_dict(row) if row else None


def get_passage_by_title(title):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM comprehension_passages
        WHERE title = %s;
    """, (title,))

    row = cur.fetchone()

    cur.close()
    conn.close()

    return row_to_dict(row) if row else None


def insert_passage(title, passage_text, difficulty=None, word_count=None):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO comprehension_passages (title, passage_text, difficulty, word_count)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (title) DO NOTHING
        RETURNING passage_id;
    """, (title, passage_text, difficulty, word_count))

    result = cur.fetchone()
    conn.commit()

    cur.close()
    conn.close()

    return result[0] if result else None


# =========================
# QUESTIONS
# =========================

def get_questions_for_passage(passage_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM comprehension_questions
        WHERE passage_id = %s
        ORDER BY sort_order ASC;
    """, (passage_id,))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return [row_to_dict(r) for r in rows]


def insert_question(passage_id, question_text, a, b, c, d, correct, qtype, order):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO comprehension_questions
        (passage_id, question_text, option_a, option_b, option_c, option_d, correct_answer, question_type, sort_order)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """, (passage_id, question_text, a, b, c, d, correct, qtype, order))

    conn.commit()

    cur.close()
    conn.close()


# =========================
# ATTEMPTS (Append Only)
# =========================

def insert_attempt(user_id, passage_id, question_id, selected_answer, correct):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO comprehension_attempts
        (user_id, passage_id, question_id, selected_answer, correct)
        VALUES (%s, %s, %s, %s, %s);
    """, (user_id, passage_id, question_id, selected_answer, correct))

    conn.commit()

    cur.close()
    conn.close()

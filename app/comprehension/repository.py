# app/comprehension/repository.py

from app.database import get_connection


# =========================
# Helper: Convert rows to dict
# =========================
def rows_to_dicts(cur, rows):
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in rows]


def row_to_dict(cur, row):
    if not row:
        return None
    columns = [desc[0] for desc in cur.description]
    return dict(zip(columns, row))


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
    result = rows_to_dicts(cur, rows)

    cur.close()
    conn.close()

    return result


def get_passage_by_id(passage_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT passage_id, title, passage_text, difficulty, word_count
        FROM comprehension_passages
        WHERE passage_id = %s;
    """, (passage_id,))

    row = cur.fetchone()
    result = row_to_dict(cur, row)  # ✅ convert BEFORE closing

    cur.close()
    conn.close()

    return result


def get_passage_by_title(title):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT passage_id, title, passage_text, difficulty, word_count
        FROM comprehension_passages
        WHERE title = %s;
    """, (title,))

    row = cur.fetchone()
    result = row_to_dict(cur, row)  # ✅ convert BEFORE closing

    cur.close()
    conn.close()

    return result


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
        SELECT question_id, question_text, option_a, option_b, option_c, option_d, correct_answer
        FROM comprehension_questions
        WHERE passage_id = %s
        ORDER BY sort_order ASC;
    """, (passage_id,))

    rows = cur.fetchall()
    result = rows_to_dicts(cur, rows)

    cur.close()
    conn.close()

    return result


def get_question_by_id(question_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT question_id, passage_id, question_text, option_a, option_b, option_c, option_d, correct_answer
        FROM comprehension_questions
        WHERE question_id = %s;
    """, (question_id,))

    row = cur.fetchone()
    result = row_to_dict(cur, row)  # ✅ FIXED: convert BEFORE closing

    cur.close()
    conn.close()

    return result


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

import json
import logging

from app.database import get_connection

logger = logging.getLogger(__name__)
COMPREHENSION_COOLDOWN_DISTANCE = 3


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


def get_next_comprehension_question(
    user_id,
    passage_id,
    conn=None,
    cooldown_distance: int = COMPREHENSION_COOLDOWN_DISTANCE,
    exclude_question_ids: list[int] | None = None,
):
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()

    cur = conn.cursor()

    try:
        excluded_question_id_set = {
            question_id
            for question_id in (exclude_question_ids or [])
            if question_id is not None
        }

        cur.execute(
            """
            SELECT question_id
            FROM comprehension_questions
            WHERE passage_id = %s
            ORDER BY sort_order ASC, question_id ASC
            """,
            (passage_id,),
        )
        ordered_question_ids = [row[0] for row in cur.fetchall() if row and row[0] is not None]

        if not ordered_question_ids:
            return None

        cur.execute(
            """
            SELECT question_id
            FROM comprehension_attempts
            WHERE user_id = %s
              AND passage_id = %s
            ORDER BY created_at ASC, attempt_id ASC
            """,
            (user_id, passage_id),
        )
        attempt_question_ids = [row[0] for row in cur.fetchall() if row and row[0] is not None]

        recent_question_ids = attempt_question_ids[-cooldown_distance:] if cooldown_distance > 0 else []
        recent_question_id_set = set(recent_question_ids)

        cycle_attempted_ids = set()
        ordered_question_id_set = set(ordered_question_ids)
        for attempted_question_id in attempt_question_ids:
            if attempted_question_id not in ordered_question_id_set:
                continue
            cycle_attempted_ids.add(attempted_question_id)
            if len(cycle_attempted_ids) == len(ordered_question_ids):
                cycle_attempted_ids = set()

        current_cycle_unanswered = [
            question_id for question_id in ordered_question_ids
            if question_id not in cycle_attempted_ids
        ]

        def _available_candidates(question_ids):
            return [
                candidate_question_id
                for candidate_question_id in question_ids
                if candidate_question_id not in excluded_question_id_set
            ]

        def _first_not_recent(question_ids):
            for candidate_question_id in question_ids:
                if candidate_question_id not in recent_question_id_set:
                    return candidate_question_id
            return None

        available_cycle_unanswered = _available_candidates(current_cycle_unanswered)
        selected_question_id = _first_not_recent(available_cycle_unanswered)
        cycle_restarted = False

        if selected_question_id is None and available_cycle_unanswered:
            blocked_question_id = available_cycle_unanswered[0]
            logger.info(
                "[COMPREHENSION_REPEAT_BLOCKED] %s",
                json.dumps(
                    {
                        "user_id": user_id,
                        "passage_id": passage_id,
                        "blocked_question_id": blocked_question_id,
                        "recent_question_ids": recent_question_ids,
                        "cooldown_distance": cooldown_distance,
                        "reason": "progression_recent_cooldown",
                    },
                    sort_keys=True,
                ),
            )
            selected_question_id = blocked_question_id

        if selected_question_id is None:
            cycle_restarted = True
            restart_candidates = _available_candidates(ordered_question_ids)
            selected_question_id = _first_not_recent(restart_candidates)
            if selected_question_id is None and restart_candidates:
                logger.info(
                    "[COMPREHENSION_REPEAT_BLOCKED] %s",
                    json.dumps(
                        {
                            "user_id": user_id,
                            "passage_id": passage_id,
                            "blocked_question_id": restart_candidates[0],
                            "recent_question_ids": recent_question_ids,
                            "cooldown_distance": cooldown_distance,
                            "reason": "cycle_restart_recent_cooldown",
                        },
                        sort_keys=True,
                    ),
                )
                selected_question_id = restart_candidates[0]

        if selected_question_id is None:
            if ordered_question_ids:
                selected_question_id = ordered_question_ids[0]
            else:
                return None

        if selected_question_id is None:
            return None

        return {
            "question_id": selected_question_id,
            "attempt_count": len(attempt_question_ids),
            "recent_question_ids": recent_question_ids,
            "cycle_restarted": cycle_restarted,
        }
    finally:
        cur.close()
        if owns_connection:
            conn.close()


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

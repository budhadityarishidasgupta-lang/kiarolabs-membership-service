from app.database import get_connection
from app.repositories.math_stats_repository import ensure_math_stats_table, update_math_stats_from_attempt


def get_math_lessons_list():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                id,
                lesson_name,
                display_name,
                topic,
                difficulty
            FROM math_lessons
            WHERE is_active = TRUE
            ORDER BY id;
            """
        )

        rows = cur.fetchall()
        return [
            {
                "lesson_id": row[0],
                "lesson_name": row[1],
                "display_name": row[2],
                "topic": row[3],
                "difficulty": row[4],
            }
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()


def _fetch_lesson_questions(cur, user_id: int, lesson_id: int) -> list[dict]:
    cur.execute(
        """
        SELECT
            q.id,
            q.stem,
            q.option_a,
            q.option_b,
            q.option_c,
            q.option_d,
            q.correct_option,
            COALESCE(s.times_seen, 0) AS times_seen,
            COALESCE(s.times_correct, 0) AS times_correct,
            COALESCE(s.times_wrong, 0) AS times_wrong,
            COALESCE(s.accuracy, 0) AS accuracy,
            s.last_seen_at,
            COALESCE(s.is_weak, FALSE) AS is_weak
        FROM math_questions q
        LEFT JOIN math_question_stats s
            ON s.question_id = q.id
           AND s.user_id = %s
        WHERE q.lesson_id = %s
        ORDER BY q.id ASC
        """,
        (user_id, lesson_id),
    )

    rows = cur.fetchall()
    return [
        {
            "question_id": row[0],
            "stem": row[1],
            "option_a": row[2],
            "option_b": row[3],
            "option_c": row[4],
            "option_d": row[5],
            "correct_option": row[6],
            "times_seen": row[7],
            "times_correct": row[8],
            "times_wrong": row[9],
            "accuracy": float(row[10] or 0),
            "last_seen_at": row[11],
            "is_weak": bool(row[12]),
        }
        for row in rows
    ]


def _get_latest_question_id(cur, user_id: int, lesson_id: int):
    cur.execute(
        """
        SELECT question_id
        FROM math_attempts
        WHERE student_id = %s
          AND lesson_id = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (user_id, lesson_id),
    )
    row = cur.fetchone()
    return row[0] if row else None


def _avoid_immediate_repeat(items: list[dict], last_question_id):
    if len(items) <= 1 or not last_question_id:
        return items
    filtered = [item for item in items if item["question_id"] != last_question_id]
    return filtered or items


def get_math_next_question(user_id: int, lesson_id_or_scope: int):
    ensure_math_stats_table()
    conn = get_connection()
    cur = conn.cursor()

    try:
        items = _fetch_lesson_questions(cur, user_id, lesson_id_or_scope)
        if not items:
            return None

        last_question_id = _get_latest_question_id(cur, user_id, lesson_id_or_scope)

        unseen = [item for item in items if item["times_seen"] == 0]
        weak = [item for item in items if item["is_weak"] or item["times_wrong"] > 0]
        review = [item for item in items if item["times_seen"] > 0 and item not in weak]

        unseen = sorted(unseen, key=lambda item: item["question_id"])
        weak = sorted(weak, key=lambda item: (item["accuracy"], item["last_seen_at"] or 0, item["question_id"]))
        review = sorted(review, key=lambda item: (item["accuracy"], item["last_seen_at"] or 0, item["question_id"]))

        for pool in (unseen, weak, review):
            candidate_pool = _avoid_immediate_repeat(pool, last_question_id)
            if candidate_pool:
                return candidate_pool[0]

        return items[0]
    finally:
        cur.close()
        conn.close()


def get_math_question_record(question_id: int):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                id,
                lesson_id,
                stem,
                option_a,
                option_b,
                option_c,
                option_d,
                option_e,
                correct_option
            FROM math_questions
            WHERE id = %s
            LIMIT 1
            """,
            (question_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "question_id": row[0],
            "lesson_id": row[1],
            "stem": row[2],
            "option_a": row[3],
            "option_b": row[4],
            "option_c": row[5],
            "option_d": row[6],
            "option_e": row[7],
            "correct_option": row[8],
        }
    finally:
        cur.close()
        conn.close()


def record_math_attempt(
    user_id: int,
    question_id: int,
    correct: bool,
    selected_option: str,
    session_id: str | None = None,
):
    question = get_math_question_record(question_id)
    if not question:
        return

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO math_attempts
            (
                student_id,
                question_id,
                lesson_id,
                selected_option,
                is_correct,
                created_at,
                attempted_at,
                session_id,
                submitted_at,
                contract_version
            )
            VALUES (%s, %s, %s, %s, %s, NOW(), NOW(), %s, NOW(), %s)
            """,
            (
                user_id,
                question_id,
                question["lesson_id"],
                selected_option,
                correct,
                session_id,
                "v1",
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    update_math_stats_from_attempt(user_id, question_id, correct)

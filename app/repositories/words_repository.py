from datetime import datetime

from app.database import get_connection
from app.repositories.words_stats_repository import update_words_stats_from_attempt


def get_words_courses_tree():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                c.id AS course_id,
                c.name AS course_name,
                l.id AS lesson_id,
                l.name AS lesson_name
            FROM words_courses c
            JOIN words_lessons l
                ON l.course_id = c.id
            ORDER BY c.id, l.id;
            """
        )

        rows = cur.fetchall()
        courses = {}

        for course_id, course_name, lesson_id, lesson_name in rows:
            if course_id not in courses:
                courses[course_id] = {
                    "course_id": course_id,
                    "course_name": course_name,
                    "lessons": [],
                }

            courses[course_id]["lessons"].append(
                {
                    "lesson_id": lesson_id,
                    "lesson_name": lesson_name,
                }
            )

        return list(courses.values())
    finally:
        cur.close()
        conn.close()


def _fetch_lesson_words(cur, user_id: int, lesson_id: int) -> list[dict]:
    cur.execute(
        """
        SELECT
            w.id,
            w.word,
            COALESCE(w.hint, '') AS hint,
            COALESCE(w.example, '') AS example,
            COALESCE(s.attempts_count, 0) AS times_seen,
            COALESCE(s.correct_count, 0) AS times_correct,
            COALESCE(s.wrong_count, 0) AS times_wrong,
            COALESCE(s.accuracy, 0) AS accuracy,
            s.last_attempt_at
        FROM words_lesson_words lw
        JOIN words_words w
            ON lw.word_id = w.id
        LEFT JOIN words_word_stats s
            ON s.word_id = w.id
           AND s.user_id = %s
        WHERE lw.lesson_id = %s
        ORDER BY w.id ASC
        """,
        (user_id, lesson_id),
    )

    rows = cur.fetchall()
    return [
        {
            "word_id": row[0],
            "word": row[1],
            "hint": row[2],
            "example": row[3],
            "times_seen": row[4],
            "times_correct": row[5],
            "times_wrong": row[6],
            "accuracy": float(row[7] or 0),
            "last_seen_at": row[8],
            "is_weak": row[4] >= 2 and float(row[7] or 0) < 0.7,
        }
        for row in rows
    ]


def _get_latest_word_id(cur, user_id: int, lesson_id: int):
    cur.execute(
        """
        SELECT wa.word_id
        FROM words_attempts wa
        JOIN words_lesson_words lw
            ON lw.word_id = wa.word_id
        WHERE wa.user_id = %s
          AND lw.lesson_id = %s
        ORDER BY wa.created_at DESC
        LIMIT 1
        """,
        (user_id, lesson_id),
    )
    row = cur.fetchone()
    return row[0] if row else None


def _avoid_immediate_repeat(items: list[dict], last_word_id):
    if len(items) <= 1 or not last_word_id:
        return items
    filtered = [item for item in items if item["word_id"] != last_word_id]
    return filtered or items


def _sort_last_seen(value):
    return value if value is not None else datetime.min


def get_words_next_item(user_id: int, lesson_id: int):
    conn = get_connection()
    cur = conn.cursor()

    try:
        items = _fetch_lesson_words(cur, user_id, lesson_id)
        if not items:
            return None

        last_word_id = _get_latest_word_id(cur, user_id, lesson_id)

        unseen = [item for item in items if item["times_seen"] == 0]
        weak = [item for item in items if item["is_weak"]]
        review = [item for item in items if item["times_seen"] > 0 and not item["is_weak"]]

        unseen = sorted(unseen, key=lambda item: item["word_id"])
        weak = sorted(weak, key=lambda item: (item["accuracy"], _sort_last_seen(item["last_seen_at"]), item["word_id"]))
        review = sorted(review, key=lambda item: (item["accuracy"], _sort_last_seen(item["last_seen_at"]), item["word_id"]))

        for pool in (unseen, weak, review):
            candidate_pool = _avoid_immediate_repeat(pool, last_word_id)
            if candidate_pool:
                return candidate_pool[0]

        return items[0]
    finally:
        cur.close()
        conn.close()


def get_word_details(word_id: int):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT id, word, COALESCE(hint, ''), COALESCE(example, '')
            FROM words_words
            WHERE id = %s
            LIMIT 1
            """,
            (word_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "word_id": row[0],
            "word": row[1],
            "hint": row[2],
            "example": row[3],
        }
    finally:
        cur.close()
        conn.close()


def get_words_micro_challenge_data(word_id: int):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT word, option_1, option_2, option_3, option_4, correct_option_index
            FROM words
            WHERE id = %s
            """,
            (word_id,),
        )
        row = cur.fetchone()
        if not row:
            return None

        return {
            "word": row[0],
            "options": [row[1], row[2], row[3], row[4]],
            "correct_index": row[5],
        }
    finally:
        cur.close()
        conn.close()


def record_words_attempt(user_id: int, lesson_id: int, word_id: int, correct: bool, response_ms: int = 0):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO words_attempts
            (user_id, word_id, correct, time_taken, blanks_count, wrong_letters_count, course_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (user_id, word_id, correct, response_ms or 0, 0, 0, 0),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    update_words_stats_from_attempt(user_id, word_id, correct)

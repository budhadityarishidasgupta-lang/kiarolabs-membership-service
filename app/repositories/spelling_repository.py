from datetime import datetime, timezone

from app.database import get_connection
from app.repositories.spelling_stats_repository import update_spelling_stats_from_attempt


def get_spelling_micro_challenge_data(word_id: int):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT word, COALESCE(hint, ''), COALESCE(example_sentence, '')
            FROM spelling_words
            WHERE word_id = %s
            LIMIT 1
            """,
            (word_id,),
        )
        row = cur.fetchone()
        if not row:
            return None

        return {
            "word": row[0],
            "hint": row[1],
            "example_sentence": row[2],
        }
    finally:
        cur.close()
        conn.close()


def get_resume_word_id(user_id, lesson_id, conn):
    """
    Returns next word_id based on last correct attempt.
    Uses spelling_lesson_words (correct namespaced table).
    """

    query = """
        WITH last_correct AS (
            SELECT word_id
            FROM spelling_attempts
            WHERE user_id = %s
              AND lesson_id = %s
              AND correct = TRUE
            ORDER BY created_at DESC
            LIMIT 1
        )
        SELECT slw.word_id
        FROM spelling_lesson_words slw
        JOIN last_correct lc ON TRUE
        WHERE slw.lesson_id = %s
          AND slw.word_id > lc.word_id
        ORDER BY slw.word_id ASC
        LIMIT 1
    """

    with conn.cursor() as cur:
        cur.execute(query, (user_id, lesson_id, lesson_id))
        result = cur.fetchone()

    return result[0] if result else None


def get_weak_word_id(user_id, lesson_id, conn):
    """
    Returns a weak word_id for a user within a lesson.
    Weak = more incorrect attempts than correct attempts.
    Lesson-scoped (platform compliant).
    """

    query = """
        SELECT word_id
        FROM spelling_attempts
        WHERE user_id = %s
          AND lesson_id = %s
        GROUP BY word_id
        HAVING SUM(CASE WHEN correct = FALSE THEN 1 ELSE 0 END) >
               SUM(CASE WHEN correct = TRUE THEN 1 ELSE 0 END)
        ORDER BY MAX(created_at) DESC
        LIMIT 1
    """

    with conn.cursor() as cur:
        cur.execute(query, (user_id, lesson_id))
        result = cur.fetchone()

    return result[0] if result else None


def get_lesson_id_for_word(word_id: int, conn=None):
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()

    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT lesson_id
            FROM spelling_lesson_words
            WHERE word_id = %s
            ORDER BY lesson_id ASC
            LIMIT 1
            """,
            (word_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def is_word_mastered(user_id, lesson_id, word_id, conn):
    """
    Returns True if the word is mastered for this user in this lesson.
    Mastery is lesson-scoped and derived only from attempts.
    """

    query = """
        WITH recent_attempts AS (
            SELECT correct
            FROM spelling_attempts
            WHERE user_id = %s
              AND lesson_id = %s
              AND word_id = %s
            ORDER BY created_at DESC
            LIMIT 2
        )
        SELECT
            COUNT(*) AS total_recent,
            SUM(CASE WHEN correct = TRUE THEN 1 ELSE 0 END) AS correct_recent,
            SUM(CASE WHEN correct = FALSE THEN 1 ELSE 0 END) AS wrong_recent
        FROM recent_attempts
    """

    with conn.cursor() as cur:
        cur.execute(query, (user_id, lesson_id, word_id))
        row = cur.fetchone()

    if not row:
        return False

    total_recent = row[0] or 0
    correct_recent = row[1] or 0
    wrong_recent = row[2] or 0

    return total_recent == 2 and correct_recent == 2 and wrong_recent == 0


def get_next_unmastered_word(user_id, lesson_id, conn):
    """
    Returns next unmastered word in lesson.
    Skips mastered words.
    """

    query = """
        SELECT slw.word_id
        FROM spelling_lesson_words slw
        WHERE slw.lesson_id = %s
          AND slw.word_id NOT IN (
              SELECT word_id
              FROM spelling_attempts
              WHERE user_id = %s
                AND lesson_id = %s
              GROUP BY word_id
              HAVING COUNT(*) FILTER (WHERE correct = TRUE) >= 2
                 AND COUNT(*) FILTER (WHERE correct = FALSE) = 0
          )
        ORDER BY slw.word_id ASC
        LIMIT 1
    """

    with conn.cursor() as cur:
        cur.execute(query, (lesson_id, user_id, lesson_id))
        result = cur.fetchone()

    return result[0] if result else None


def _fetch_lesson_words(cur, user_id: int, lesson_id: int) -> list[dict]:
    cur.execute(
        """
        SELECT
            w.word_id,
            w.word,
            COALESCE(w.hint, '') AS hint,
            COALESCE(w.example_sentence, '') AS example_sentence,
            COALESCE(s.attempts_count, 0) AS times_seen,
            COALESCE(s.correct_count, 0) AS times_correct,
            COALESCE(s.wrong_count, 0) AS times_wrong,
            COALESCE(s.accuracy, 0) AS accuracy,
            s.last_attempt_at
        FROM spelling_lesson_words lw
        JOIN spelling_words w
            ON lw.word_id = w.word_id
        LEFT JOIN spelling_word_stats s
            ON s.word_id = w.word_id
           AND s.user_id = %s
        WHERE lw.lesson_id = %s
        ORDER BY w.word_id ASC
        """,
        (user_id, lesson_id),
    )

    rows = cur.fetchall()
    return [
        {
            "word_id": row[0],
            "word": row[1],
            "hint": row[2],
            "example_sentence": row[3],
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
        SELECT sa.word_id
        FROM spelling_attempts sa
        JOIN spelling_lesson_words lw
            ON lw.word_id = sa.word_id
        WHERE sa.user_id = %s
          AND lw.lesson_id = %s
        ORDER BY sa.created_at DESC
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


def get_spelling_next_item(user_id: int, lesson_id: int):
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
        weak = sorted(weak, key=lambda item: (item["accuracy"], item["last_seen_at"] or 0, item["word_id"]))
        review = sorted(review, key=lambda item: (item["accuracy"], item["last_seen_at"] or 0, item["word_id"]))

        for pool in (unseen, weak, review):
            candidate_pool = _avoid_immediate_repeat(pool, last_word_id)
            if candidate_pool:
                return candidate_pool[0]

        return items[0]
    finally:
        cur.close()
        conn.close()


def get_spelling_word_details(word_id: int, conn=None):
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()

    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT word_id, word, COALESCE(hint, ''), COALESCE(example_sentence, '')
            FROM spelling_words
            WHERE word_id = %s
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
            "example_sentence": row[3],
        }
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def record_spelling_attempt(
    user_id: int,
    word_id: int,
    submitted_text: str,
    correct: bool,
    session_id: str | None = None,
    question_id: str | None = None,
    lesson_id: int | None = None,
):
    conn = get_connection()
    cur = conn.cursor()

    try:
        time_taken = 0
        blanks_count = 0
        wrong_letters_count = 0
        created_at = datetime.now(timezone.utc)
        course_id = 0
        member_id = None

        lesson_id = lesson_id if lesson_id else None
        question_id = question_id if question_id else None
        session_id = session_id if session_id else None
        contract_version = "v1"

        query = """
            INSERT INTO spelling_attempts (
                user_id,
                word_id,
                correct,
                time_taken,
                blanks_count,
                wrong_letters_count,
                created_at,
                course_id,
                member_id,

                -- NEW FIELDS (SAFE ADDITIVE)
                lesson_id,
                question_id,
                session_id,
                submitted_at,
                contract_version
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, NOW(), %s
            )
        """

        values = (
            user_id,
            word_id,
            correct,
            time_taken,
            blanks_count,
            wrong_letters_count,
            created_at,
            course_id,
            member_id,

            # NEW FIELDS
            lesson_id,
            question_id,
            session_id,
            contract_version,
        )
        cur.execute(query, values)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    update_spelling_stats_from_attempt(user_id, word_id, correct)

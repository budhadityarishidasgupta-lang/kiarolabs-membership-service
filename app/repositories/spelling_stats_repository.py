from app.database import get_connection


def get_spelling_weak_items(user_id: int, lesson_id: int) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
        SELECT
            w.word_id,
            COALESCE(s.attempts_count, 0) AS times_seen,
                COALESCE(s.correct_count, 0) AS times_correct,
                COALESCE(s.wrong_count, 0) AS times_wrong,
                COALESCE(s.accuracy, 0) AS accuracy,
                s.last_attempt_at
            FROM spelling_lesson_items li
            JOIN spelling_lessons l
                ON l.lesson_id = li.lesson_id
            JOIN spelling_words w
                ON li.word_id = w.word_id
            JOIN spelling_word_stats s
                ON s.word_id = w.word_id
               AND s.user_id = %s
            WHERE li.lesson_id = %s
              AND l.is_active = true
              AND s.attempts_count >= 2
              AND s.accuracy < 0.7
            ORDER BY s.accuracy ASC, s.last_attempt_at ASC NULLS FIRST, w.word_id ASC
            """,
            (user_id, lesson_id),
        )

        rows = cur.fetchall()
        return [
            {
                "word_id": row[0],
                "times_seen": row[1],
                "times_correct": row[2],
                "times_wrong": row[3],
                "accuracy": float(row[4] or 0),
                "last_seen_at": row[5],
                "is_weak": True,
            }
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()


def get_spelling_weak_pattern(user_id: int):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT pattern
            FROM spelling_pattern_stats
            WHERE user_id = %s
            ORDER BY accuracy ASC, last_attempt_at ASC NULLS FIRST
            LIMIT 1
            """,
            (user_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()
        conn.close()


def update_spelling_stats_from_attempt(user_id: int, word_id: int, correct: bool):
    conn = get_connection()
    cur = conn.cursor()

    correct_count = 1 if correct else 0
    wrong_count = 0 if correct else 1

    try:
        cur.execute(
            """
            INSERT INTO spelling_word_stats (
                user_id,
                word_id,
                attempts_count,
                correct_count,
                wrong_count,
                last_attempt_at,
                last_correct_at,
                accuracy
            )
            VALUES (
                %s,
                %s,
                1,
                %s,
                %s,
                NOW(),
                CASE WHEN %s = 1 THEN NOW() ELSE NULL END,
                %s
            )
            ON CONFLICT (user_id, word_id)
            DO UPDATE SET
                attempts_count = spelling_word_stats.attempts_count + 1,
                correct_count = spelling_word_stats.correct_count + EXCLUDED.correct_count,
                wrong_count = spelling_word_stats.wrong_count + EXCLUDED.wrong_count,
                last_attempt_at = NOW(),
                last_correct_at = CASE
                    WHEN EXCLUDED.correct_count = 1 THEN NOW()
                    ELSE spelling_word_stats.last_correct_at
                END,
                accuracy = (
                    (spelling_word_stats.correct_count + EXCLUDED.correct_count)::float /
                    (spelling_word_stats.attempts_count + 1)
                )
            """,
            (
                user_id,
                word_id,
                correct_count,
                wrong_count,
                correct_count,
                1.0 if correct else 0.0,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def update_spelling_pattern_stats(user_id: int, patterns: list[str], correct: bool):
    if not patterns:
        return

    conn = get_connection()
    cur = conn.cursor()

    correct_count = 1 if correct else 0
    wrong_count = 0 if correct else 1

    try:
        for pattern in patterns:
            cur.execute(
                """
                INSERT INTO spelling_pattern_stats (
                    user_id,
                    pattern,
                    attempts_count,
                    correct_count,
                    wrong_count,
                    accuracy,
                    last_attempt_at
                )
                VALUES (%s, %s, 1, %s, %s, %s, NOW())
                ON CONFLICT (user_id, pattern)
                DO UPDATE SET
                    attempts_count = spelling_pattern_stats.attempts_count + 1,
                    correct_count = spelling_pattern_stats.correct_count + EXCLUDED.correct_count,
                    wrong_count = spelling_pattern_stats.wrong_count + EXCLUDED.wrong_count,
                    last_attempt_at = NOW(),
                    accuracy = (
                        (spelling_pattern_stats.correct_count + EXCLUDED.correct_count)::float /
                        (spelling_pattern_stats.attempts_count + 1)
                    )
                """,
                (
                    user_id,
                    pattern,
                    correct_count,
                    wrong_count,
                    1.0 if correct else 0.0,
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

from app.database import get_connection


def get_words_weak_items(user_id: int, lesson_id: int) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                w.id,
                COALESCE(s.attempts_count, 0) AS times_seen,
                COALESCE(s.correct_count, 0) AS times_correct,
                COALESCE(s.wrong_count, 0) AS times_wrong,
                COALESCE(s.accuracy, 0) AS accuracy,
                s.last_attempt_at
            FROM words_lesson_words lw
            JOIN words_words w
                ON lw.word_id = w.id
            JOIN words_word_stats s
                ON s.word_id = w.id
               AND s.user_id = %s
            WHERE lw.lesson_id = %s
              AND s.attempts_count >= 2
              AND s.accuracy < 0.7
            ORDER BY s.accuracy ASC, s.last_attempt_at ASC NULLS FIRST, w.id ASC
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


def update_words_stats_from_attempt(user_id: int, word_id: int, correct: bool):
    conn = get_connection()
    cur = conn.cursor()

    correct_count = 1 if correct else 0
    wrong_count = 0 if correct else 1

    try:
        cur.execute(
            """
            INSERT INTO words_word_stats (
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
                attempts_count = words_word_stats.attempts_count + 1,
                correct_count = words_word_stats.correct_count + EXCLUDED.correct_count,
                wrong_count = words_word_stats.wrong_count + EXCLUDED.wrong_count,
                last_attempt_at = NOW(),
                last_correct_at = CASE
                    WHEN EXCLUDED.correct_count = 1 THEN NOW()
                    ELSE words_word_stats.last_correct_at
                END,
                accuracy = (
                    (words_word_stats.correct_count + EXCLUDED.correct_count)::float /
                    (words_word_stats.attempts_count + 1)
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


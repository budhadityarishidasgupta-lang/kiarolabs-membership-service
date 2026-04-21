from app.database import get_connection


def ensure_math_stats_table():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS math_question_stats (
                id SERIAL PRIMARY KEY,
                user_id INT NOT NULL,
                question_id INT NOT NULL,
                times_seen INT DEFAULT 0,
                times_correct INT DEFAULT 0,
                times_wrong INT DEFAULT 0,
                accuracy FLOAT DEFAULT 0,
                last_seen_at TIMESTAMP,
                is_weak BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE (user_id, question_id)
            )
            """
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def get_math_weak_questions(user_id: int, lesson_id: int) -> list[dict]:
    ensure_math_stats_table()
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                q.id,
                s.times_seen,
                s.times_correct,
                s.times_wrong,
                s.accuracy,
                s.last_seen_at,
                s.is_weak
            FROM math_questions q
            JOIN math_question_stats s
                ON s.question_id = q.id
               AND s.user_id = %s
            WHERE q.lesson_id = %s
              AND s.is_weak = TRUE
            ORDER BY s.accuracy ASC, s.last_seen_at ASC NULLS FIRST, q.id ASC
            """,
            (user_id, lesson_id),
        )
        rows = cur.fetchall()
        return [
            {
                "question_id": row[0],
                "times_seen": row[1],
                "times_correct": row[2],
                "times_wrong": row[3],
                "accuracy": float(row[4] or 0),
                "last_seen_at": row[5],
                "is_weak": bool(row[6]),
            }
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()


def update_math_stats_from_attempt(user_id: int, question_id: int, correct: bool):
    ensure_math_stats_table()
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO math_question_stats (
                user_id,
                question_id,
                times_seen,
                times_correct,
                times_wrong,
                accuracy,
                last_seen_at,
                is_weak,
                updated_at
            )
            VALUES (
                %s,
                %s,
                1,
                %s,
                %s,
                %s,
                NOW(),
                %s,
                NOW()
            )
            ON CONFLICT (user_id, question_id)
            DO UPDATE SET
                times_seen = math_question_stats.times_seen + 1,
                times_correct = math_question_stats.times_correct + EXCLUDED.times_correct,
                times_wrong = math_question_stats.times_wrong + EXCLUDED.times_wrong,
                accuracy = (
                    (math_question_stats.times_correct + EXCLUDED.times_correct)::float /
                    (math_question_stats.times_seen + 1)
                ),
                last_seen_at = NOW(),
                is_weak = (
                    (math_question_stats.times_seen + 1) >= 2
                    AND (
                        (math_question_stats.times_correct + EXCLUDED.times_correct)::float /
                        (math_question_stats.times_seen + 1)
                    ) < 0.7
                ),
                updated_at = NOW()
            """,
            (
                user_id,
                question_id,
                1 if correct else 0,
                0 if correct else 1,
                1.0 if correct else 0.0,
                False,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


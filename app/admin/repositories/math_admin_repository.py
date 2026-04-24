from app.database import get_connection


def get_math_overview():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT COUNT(*) FROM math_lessons")
        lesson_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM math_questions")
        item_count = cur.fetchone()[0]

        return {
            "module": "maths",
            "label": "Maths",
            "supports_courses": False,
            "course_count": 0,
            "lesson_count": lesson_count,
            "item_count": item_count,
        }
    finally:
        cur.close()
        conn.close()


def list_math_courses():
    return []


def list_math_lessons():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                id,
                lesson_name,
                COALESCE(display_name, lesson_name) AS display_name,
                COALESCE(topic, 'General') AS topic,
                COALESCE(difficulty, 'unspecified') AS difficulty,
                COALESCE(is_active, TRUE) AS is_active
            FROM math_lessons
            ORDER BY id ASC
            """
        )
        rows = cur.fetchall()

        lessons = []
        for row in rows:
            lessons.append(
                {
                    "lesson_id": row[0],
                    "lesson_name": row[1],
                    "display_name": row[2],
                    "course_name": row[3],
                    "topic": row[3],
                    "difficulty": row[4],
                    "is_active": row[5],
                }
            )

        return lessons
    finally:
        cur.close()
        conn.close()


def create_math_lesson(
    lesson_name: str,
    display_name: str | None = None,
    topic: str | None = None,
    difficulty: str | None = None,
    is_active: bool = True,
):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO math_lessons (lesson_name, display_name, topic, difficulty, is_active)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, lesson_name, COALESCE(display_name, lesson_name), COALESCE(topic, 'General'), COALESCE(difficulty, 'unspecified'), is_active
            """,
            (
                lesson_name.strip(),
                (display_name or "").strip() or None,
                (topic or "").strip() or None,
                (difficulty or "").strip() or None,
                is_active,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        return {
            "lesson_id": row[0],
            "lesson_name": row[1],
            "display_name": row[2],
            "topic": row[3],
            "difficulty": row[4],
            "is_active": row[5],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


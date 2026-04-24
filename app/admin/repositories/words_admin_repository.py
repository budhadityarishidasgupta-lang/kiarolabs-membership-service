from app.database import get_connection


def get_words_overview():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT COUNT(*) FROM words_courses")
        course_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM words_lessons")
        lesson_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM words_words")
        item_count = cur.fetchone()[0]

        return {
            "module": "words",
            "label": "Words",
            "supports_courses": True,
            "course_count": course_count,
            "lesson_count": lesson_count,
            "item_count": item_count,
        }
    finally:
        cur.close()
        conn.close()


def list_words_courses():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                c.id,
                c.name,
                COUNT(l.id) AS lesson_count
            FROM words_courses c
            LEFT JOIN words_lessons l
                ON l.course_id = c.id
            GROUP BY c.id, c.name
            ORDER BY c.id ASC
            """
        )
        rows = cur.fetchall()

        return [
            {
                "course_id": row[0],
                "course_name": row[1],
                "lesson_count": row[2],
            }
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()


def create_words_course(name: str):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO words_courses (name)
            VALUES (%s)
            RETURNING id, name
            """,
            (name.strip(),),
        )
        row = cur.fetchone()
        conn.commit()
        return {
            "course_id": row[0],
            "course_name": row[1],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def list_words_lessons(course_id: int | None = None):
    conn = get_connection()
    cur = conn.cursor()

    try:
        params = []
        where_sql = ""
        if course_id is not None:
            where_sql = "WHERE l.course_id = %s"
            params.append(course_id)

        cur.execute(
            f"""
            SELECT
                l.id,
                l.course_id,
                c.name AS course_name,
                l.name,
                COUNT(lw.word_id) AS item_count
            FROM words_lessons l
            JOIN words_courses c
                ON c.id = l.course_id
            LEFT JOIN words_lesson_words lw
                ON lw.lesson_id = l.id
            {where_sql}
            GROUP BY l.id, l.course_id, c.name, l.name
            ORDER BY l.course_id ASC, l.id ASC
            """,
            tuple(params),
        )
        rows = cur.fetchall()

        return [
            {
                "lesson_id": row[0],
                "course_id": row[1],
                "course_name": row[2],
                "lesson_name": row[3],
                "display_name": row[3],
                "item_count": row[4],
            }
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()


def create_words_lesson(course_id: int, lesson_name: str):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO words_lessons (course_id, name)
            VALUES (%s, %s)
            RETURNING id, course_id, name
            """,
            (course_id, lesson_name.strip()),
        )
        row = cur.fetchone()
        conn.commit()
        return {
            "lesson_id": row[0],
            "course_id": row[1],
            "lesson_name": row[2],
            "display_name": row[2],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


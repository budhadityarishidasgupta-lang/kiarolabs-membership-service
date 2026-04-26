from app.database import get_connection


def get_spelling_overview():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT COUNT(DISTINCT c.course_id)
            FROM spelling_courses c
            JOIN spelling_lessons l
                ON l.course_id = c.course_id
            WHERE COALESCE(l.is_active, TRUE) = TRUE
            """
        )
        course_count = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COUNT(*)
            FROM spelling_lessons
            WHERE COALESCE(is_active, TRUE) = TRUE
            """
        )
        lesson_count = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COUNT(DISTINCT lw.word_id)
            FROM spelling_lesson_words lw
            JOIN spelling_lessons l
                ON l.lesson_id = lw.lesson_id
            WHERE COALESCE(l.is_active, TRUE) = TRUE
            """
        )
        item_count = cur.fetchone()[0]

        return {
            "module": "spelling",
            "label": "Spelling",
            "supports_courses": True,
            "course_count": course_count,
            "lesson_count": lesson_count,
            "item_count": item_count,
        }
    finally:
        cur.close()
        conn.close()


def list_spelling_courses():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                c.course_id,
                c.course_name,
                COUNT(DISTINCT l.lesson_id) AS lesson_count
            FROM spelling_courses c
            LEFT JOIN spelling_lessons l
                ON l.course_id = c.course_id
               AND COALESCE(l.is_active, TRUE) = TRUE
            GROUP BY c.course_id, c.course_name
            ORDER BY c.course_id ASC
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


def create_spelling_course(course_name: str):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO spelling_courses (course_name)
            VALUES (%s)
            RETURNING course_id, course_name
            """,
            (course_name.strip(),),
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


def list_spelling_lessons(course_id: int | None = None):
    conn = get_connection()
    cur = conn.cursor()

    try:
        params = []
        where_sql = ""
        if course_id is not None:
            where_sql = "WHERE l.course_id = %s AND COALESCE(l.is_active, TRUE) = TRUE"
            params.append(course_id)
        else:
            where_sql = "WHERE COALESCE(l.is_active, TRUE) = TRUE"

        cur.execute(
            f"""
            SELECT
                l.lesson_id,
                l.course_id,
                c.course_name,
                l.lesson_name,
                COALESCE(l.display_name, l.lesson_name) AS display_name,
                COALESCE(l.sort_order, 0) AS sort_order,
                COALESCE(l.is_active, TRUE) AS is_active,
                COUNT(DISTINCT lw.word_id) AS item_count
            FROM spelling_lessons l
            JOIN spelling_courses c
                ON c.course_id = l.course_id
            LEFT JOIN spelling_lesson_words lw
                ON lw.lesson_id = l.lesson_id
            {where_sql}
            GROUP BY
                l.lesson_id,
                l.course_id,
                c.course_name,
                l.lesson_name,
                l.display_name,
                l.sort_order,
                l.is_active
            ORDER BY l.course_id ASC, COALESCE(l.sort_order, 0) ASC, l.lesson_id ASC
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
                "display_name": row[4],
                "sort_order": row[5],
                "is_active": row[6],
                "item_count": row[7],
            }
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()


def create_spelling_lesson(
    course_id: int,
    lesson_name: str,
    display_name: str | None = None,
    is_active: bool = True,
):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT COALESCE(MAX(sort_order), 0) + 1
            FROM spelling_lessons
            WHERE course_id = %s
            """,
            (course_id,),
        )
        sort_order = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO spelling_lessons (course_id, lesson_name, display_name, sort_order, is_active)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING lesson_id, course_id, lesson_name, COALESCE(display_name, lesson_name), sort_order, is_active
            """,
            (course_id, lesson_name.strip(), (display_name or "").strip() or None, sort_order, is_active),
        )
        row = cur.fetchone()
        conn.commit()
        return {
            "lesson_id": row[0],
            "course_id": row[1],
            "lesson_name": row[2],
            "display_name": row[3],
            "sort_order": row[4],
            "is_active": row[5],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def update_spelling_lesson(
    lesson_id: int,
    *,
    lesson_name: str,
    display_name: str | None = None,
):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cleaned_lesson_name = (lesson_name or "").strip()
        cleaned_display_name = (display_name or "").strip() or None

        if not cleaned_lesson_name:
            raise ValueError("lesson_name is required")

        cur.execute(
            """
            UPDATE spelling_lessons
            SET lesson_name = %s,
                display_name = %s
            WHERE lesson_id = %s
            RETURNING lesson_id, course_id, lesson_name, COALESCE(display_name, lesson_name), COALESCE(sort_order, 0), COALESCE(is_active, TRUE)
            """,
            (cleaned_lesson_name, cleaned_display_name, lesson_id),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None

        cur.execute(
            """
            SELECT course_name
            FROM spelling_courses
            WHERE course_id = %s
            """,
            (row[1],),
        )
        course_row = cur.fetchone()

        cur.execute(
            """
            SELECT COUNT(DISTINCT word_id)
            FROM spelling_lesson_words
            WHERE lesson_id = %s
            """,
            (lesson_id,),
        )
        item_count = cur.fetchone()[0]

        conn.commit()
        return {
            "lesson_id": row[0],
            "course_id": row[1],
            "course_name": course_row[0] if course_row else None,
            "lesson_name": row[2],
            "display_name": row[3],
            "sort_order": row[4],
            "is_active": row[5],
            "item_count": item_count,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def list_spelling_lesson_content(lesson_id: int):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                l.lesson_id,
                l.lesson_name,
                COALESCE(l.display_name, l.lesson_name) AS display_name
            FROM spelling_lessons l
            WHERE l.lesson_id = %s
            LIMIT 1
            """,
            (lesson_id,),
        )
        lesson_row = cur.fetchone()
        if not lesson_row:
            return None

        cur.execute(
            """
            SELECT
                w.word_id,
                COALESCE(w.example_sentence, '') AS prompt,
                w.word
            FROM spelling_lesson_words lw
            JOIN spelling_words w
                ON w.word_id = lw.word_id
            WHERE lw.lesson_id = %s
            ORDER BY w.word_id ASC
            """,
            (lesson_id,),
        )
        rows = cur.fetchall()

        items = [
            {
                "item_id": row[0],
                "prompt": row[1].strip() or f"Word #{row[0]}",
                "answer": row[2],
            }
            for row in rows
        ]

        return {
            "lesson_id": lesson_row[0],
            "lesson_name": lesson_row[1],
            "display_name": lesson_row[2],
            "items": items,
        }
    finally:
        cur.close()
        conn.close()


def update_spelling_content_answer(item_id: int, answer: str):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cleaned_answer = (answer or "").strip()
        if not cleaned_answer:
            raise ValueError("answer is required")

        cur.execute(
            """
            UPDATE spelling_words
            SET word = %s
            WHERE word_id = %s
            RETURNING word_id, COALESCE(example_sentence, ''), word
            """,
            (cleaned_answer, item_id),
        )
        row = cur.fetchone()
        if not row:
            return None
        conn.commit()
        return {
            "item_id": row[0],
            "prompt": row[1].strip() or f"Word #{row[0]}",
            "answer": row[2],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

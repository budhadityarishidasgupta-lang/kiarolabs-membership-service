from app.database import get_connection


def _table_has_column(cur, table_schema: str, table_name: str, column_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (table_schema, table_name, column_name),
    )
    return cur.fetchone() is not None


def get_words_overview():
    conn = get_connection()
    cur = conn.cursor()

    try:
        lessons_have_is_active = _table_has_column(cur, "public", "lessons", "is_active")
        active_lesson_predicate = (
            "COALESCE(l.is_active, TRUE) = TRUE" if lessons_have_is_active else "TRUE"
        )

        cur.execute(
            """
            SELECT COUNT(*)
            FROM public.courses
            WHERE course_type = 'synonym'
            """
        )
        course_count = cur.fetchone()[0]

        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM public.lessons l
            JOIN public.courses c
                ON c.course_id = l.course_id
            WHERE c.course_type = 'synonym'
              AND {active_lesson_predicate}
            """
        )
        lesson_count = cur.fetchone()[0]

        cur.execute(
            f"""
            SELECT COUNT(DISTINCT lw.word_id)
            FROM public.lesson_words lw
            JOIN public.lessons l
                ON l.lesson_id = lw.lesson_id
            JOIN public.courses c
                ON c.course_id = l.course_id
            WHERE c.course_type = 'synonym'
              AND {active_lesson_predicate}
            """
        )
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
        lessons_have_is_active = _table_has_column(cur, "public", "lessons", "is_active")
        active_join_clause = (
            "AND COALESCE(l.is_active, TRUE) = TRUE" if lessons_have_is_active else ""
        )

        cur.execute(
            f"""
            SELECT
                c.course_id,
                c.title,
                COUNT(DISTINCT l.lesson_id) AS lesson_count
            FROM public.courses c
            LEFT JOIN public.lessons l
                ON l.course_id = c.course_id
               {active_join_clause}
            WHERE c.course_type = 'synonym'
            GROUP BY c.course_id, c.title
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


def create_words_course(name: str):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO public.courses (title, course_type)
            VALUES (%s, 'synonym')
            RETURNING course_id, title
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
        lessons_have_is_active = _table_has_column(cur, "public", "lessons", "is_active")
        params = []
        where_clauses = ["c.course_type = 'synonym'"]
        is_active_select = "TRUE AS is_active"
        active_group_field = ""
        if lessons_have_is_active:
            where_clauses.append("COALESCE(l.is_active, TRUE) = TRUE")
            is_active_select = "COALESCE(l.is_active, TRUE) AS is_active"
            active_group_field = ", l.is_active"
        if course_id is not None:
            where_clauses.append("l.course_id = %s")
            params.append(course_id)
        where_sql = f"WHERE {' AND '.join(where_clauses)}"

        cur.execute(
            f"""
            SELECT
                l.lesson_id,
                l.course_id,
                c.title AS course_name,
                l.title,
                COALESCE(l.sort_order, 0) AS sort_order,
                {is_active_select},
                COUNT(DISTINCT lw.word_id) AS item_count
            FROM public.lessons l
            JOIN public.courses c
                ON c.course_id = l.course_id
            LEFT JOIN public.lesson_words lw
                ON lw.lesson_id = l.lesson_id
            {where_sql}
            GROUP BY l.lesson_id, l.course_id, c.title, l.title, l.sort_order{active_group_field}
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
                "display_name": row[3],
                "sort_order": row[4],
                "is_active": row[5],
                "item_count": row[6],
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
        lessons_have_is_active = _table_has_column(cur, "public", "lessons", "is_active")

        cur.execute(
            """
            SELECT COALESCE(MAX(sort_order), 0) + 1
            FROM public.lessons
            WHERE course_id = %s
            """,
            (course_id,),
        )
        sort_order = cur.fetchone()[0]

        if lessons_have_is_active:
            cur.execute(
                """
                INSERT INTO public.lessons (course_id, title, sort_order, is_active)
                VALUES (%s, %s, %s, TRUE)
                RETURNING lesson_id, course_id, title, sort_order, COALESCE(is_active, TRUE)
                """,
                (course_id, lesson_name.strip(), sort_order),
            )
        else:
            cur.execute(
                """
                INSERT INTO public.lessons (course_id, title, sort_order)
                VALUES (%s, %s, %s)
                RETURNING lesson_id, course_id, title, sort_order, TRUE
                """,
                (course_id, lesson_name.strip(), sort_order),
            )
        row = cur.fetchone()
        conn.commit()
        return {
            "lesson_id": row[0],
            "course_id": row[1],
            "lesson_name": row[2],
            "display_name": row[2],
            "sort_order": row[3],
            "is_active": row[4],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

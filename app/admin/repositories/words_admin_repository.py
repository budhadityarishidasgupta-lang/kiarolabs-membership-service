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


def update_words_lesson(
    lesson_id: int,
    *,
    lesson_name: str,
    display_name: str | None = None,
):
    conn = get_connection()
    cur = conn.cursor()

    try:
        next_title = ((display_name or "").strip() or (lesson_name or "").strip())
        if not next_title:
            raise ValueError("lesson_name is required")

        lessons_have_is_active = _table_has_column(cur, "public", "lessons", "is_active")
        is_active_select = "COALESCE(is_active, TRUE)" if lessons_have_is_active else "TRUE"

        cur.execute(
            f"""
            UPDATE public.lessons
            SET title = %s
            WHERE lesson_id = %s
            RETURNING lesson_id, course_id, title, COALESCE(sort_order, 0), {is_active_select}
            """,
            (next_title, lesson_id),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None

        cur.execute(
            """
            SELECT title
            FROM public.courses
            WHERE course_id = %s
            """,
            (row[1],),
        )
        course_row = cur.fetchone()

        cur.execute(
            """
            SELECT COUNT(DISTINCT word_id)
            FROM public.lesson_words
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
            "display_name": row[2],
            "sort_order": row[3],
            "is_active": row[4],
            "item_count": item_count,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def list_words_lesson_content(lesson_id: int):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                l.lesson_id,
                l.title,
                l.title
            FROM public.lessons l
            JOIN public.courses c
                ON c.course_id = l.course_id
            WHERE l.lesson_id = %s
              AND c.course_type = 'synonym'
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
                w.headword,
                COALESCE(w.synonyms, '')
            FROM public.lesson_words lw
            JOIN public.words w
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
                "prompt": row[1],
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


def update_words_content_answer(item_id: int, answer: str):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cleaned_answer = (answer or "").strip()
        if not cleaned_answer:
            raise ValueError("answer is required")

        cur.execute(
            """
            UPDATE public.words
            SET synonyms = %s
            WHERE word_id = %s
            RETURNING word_id, headword, COALESCE(synonyms, '')
            """,
            (cleaned_answer, item_id),
        )
        row = cur.fetchone()
        if not row:
            return None
        conn.commit()
        return {
            "item_id": row[0],
            "prompt": row[1],
            "answer": row[2],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

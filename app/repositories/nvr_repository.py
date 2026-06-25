"""
NVR practice repository — lesson listing and question fetch.
Mirrors math_repository.py patterns.
"""
from app.database import get_connection
from app.adaptive_difficulty import get_student_mastery_nvr, target_difficulty


def get_nvr_lessons():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                l.id,
                l.lesson_name,
                l.display_name,
                l.topic,
                l.difficulty,
                l.description,
                COUNT(lq.question_id) AS question_count
            FROM nvr_lessons l
            LEFT JOIN nvr_lesson_questions lq ON lq.lesson_id = l.id
            WHERE l.is_active = TRUE
            GROUP BY l.id, l.lesson_name, l.display_name, l.topic, l.difficulty, l.description
            HAVING COUNT(lq.question_id) > 0
            ORDER BY l.id;
            """
        )
        rows = cur.fetchall()
        return [
            {
                "lesson_id": row[0],
                "lesson_name": row[1],
                "display_name": row[2] or row[1],
                "topic": row[3] or "General",
                "difficulty": row[4] or "unspecified",
                "description": row[5] or "",
                "question_count": row[6],
            }
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()


def get_nvr_question(lesson_id: int, seen_ids: list[int] | None = None, user_id: int | None = None):
    """Return one unseen question from the lesson, biased by student mastery difficulty."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Adaptive difficulty: determine preferred difficulty band
        difficulty_filter = ""
        diff_params: list = []
        if user_id:
            from app.adaptive_difficulty import filter_by_difficulty
            mastery = get_student_mastery_nvr(cur, user_id, lesson_id)
            preferred = target_difficulty(mastery)
            # Try preferred difficulty first, fall back if no results
            for diff in preferred:
                exclusion_inner = ""
                p_inner: list = [lesson_id, diff]
                if seen_ids:
                    exclusion_inner = "AND q.id != ALL(%s)"
                    p_inner.append(seen_ids)
                cur.execute(
                    f"""
                    SELECT q.id, q.question_id, q.stem, q.option_a, q.option_b, q.option_c,
                           q.option_d, q.option_e, q.correct_option, q.topic, q.difficulty,
                           q.explanation, q.hint, q.geometry_schema::text
                    FROM nvr_questions q
                    JOIN nvr_lesson_questions lq ON lq.question_id = q.id
                    WHERE lq.lesson_id = %s AND LOWER(COALESCE(q.difficulty,'')) = LOWER(%s)
                      {exclusion_inner}
                    ORDER BY RANDOM() LIMIT 1;
                    """,
                    p_inner,
                )
                row = cur.fetchone()
                if row:
                    import json
                    geo = None
                    if row[13]:
                        try: geo = json.loads(row[13])
                        except Exception: geo = None
                    return {"db_id": row[0], "question_id": row[1], "stem": row[2],
                            "option_a": row[3], "option_b": row[4], "option_c": row[5],
                            "option_d": row[6], "option_e": row[7], "correct_option": row[8],
                            "topic": row[9], "difficulty": row[10], "explanation": row[11],
                            "hint": row[12], "geometry_schema": geo}
        exclusion = ""
        params: list = [lesson_id]
        if seen_ids:
            exclusion = "AND q.id != ALL(%s)"
            params.append(seen_ids)

        cur.execute(
            f"""
            SELECT
                q.id,
                q.question_id,
                q.stem,
                q.option_a,
                q.option_b,
                q.option_c,
                q.option_d,
                q.option_e,
                q.correct_option,
                q.topic,
                q.difficulty,
                q.explanation,
                q.hint,
                q.geometry_schema::text
            FROM nvr_questions q
            JOIN nvr_lesson_questions lq ON lq.question_id = q.id
            WHERE lq.lesson_id = %s
              {exclusion}
            ORDER BY RANDOM()
            LIMIT 1;
            """,
            params,
        )
        row = cur.fetchone()
        if not row:
            return None
        import json
        geo = None
        if row[13]:
            try:
                geo = json.loads(row[13])
            except Exception:
                geo = None
        return {
            "db_id": row[0],
            "question_id": row[1],
            "stem": row[2],
            "option_a": row[3],
            "option_b": row[4],
            "option_c": row[5],
            "option_d": row[6],
            "option_e": row[7],
            "correct_option": row[8],
            "topic": row[9],
            "difficulty": row[10],
            "explanation": row[11],
            "hint": row[12],
            "geometry_schema": geo,
        }
    finally:
        cur.close()
        conn.close()


def submit_nvr_answer(user_id: int, lesson_id: int, question_id: str, selected: str, correct: str):
    """Record a student answer attempt."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO nvr_attempts (user_id, lesson_id, question_id, selected_option, correct_option, is_correct, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT DO NOTHING;
            """,
            (user_id, lesson_id, question_id, selected, correct, selected.upper() == correct.upper()),
        )
        conn.commit()
        return {"ok": True}
    except Exception:
        conn.rollback()
        return {"ok": False}
    finally:
        cur.close()
        conn.close()

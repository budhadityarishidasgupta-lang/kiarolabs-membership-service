"""
NVRSprint Admin Repository
Mirrors math_admin_repository.py for the nvr_* tables.
"""
import re
from app.database import get_connection


def _get_nvr_lesson_item_count(cur, lesson_id: int) -> int:
    cur.execute(
        "SELECT COUNT(*) FROM nvr_lesson_questions WHERE lesson_id = %s",
        (lesson_id,),
    )
    row = cur.fetchone()
    return row[0] if row else 0


def get_nvr_overview() -> dict:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM nvr_lessons WHERE is_active = TRUE")
        lesson_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM nvr_questions")
        question_count = cur.fetchone()[0]
        return {
            "module": "nvr",
            "display_name": "NVRSprint",
            "lesson_count": lesson_count,
            "question_count": question_count,
        }
    finally:
        cur.close()
        conn.close()


def list_nvr_lessons() -> list:
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
                COALESCE(is_active, TRUE) AS is_active,
                COALESCE(description, '') AS description
            FROM nvr_lessons
            ORDER BY id ASC
            """
        )
        rows = cur.fetchall()
        lessons = []
        for row in rows:
            item_count = _get_nvr_lesson_item_count(cur, row[0])
            lessons.append({
                "lesson_id": row[0],
                "lesson_name": row[1],
                "display_name": row[2],
                "topic": row[3],
                "difficulty": row[4],
                "is_active": row[5],
                "item_count": item_count,
                "description": row[6],
            })
        return lessons
    finally:
        cur.close()
        conn.close()


def list_nvr_lesson_question_answers(lesson_id: int) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id, lesson_name, COALESCE(display_name, lesson_name) FROM nvr_lessons WHERE id = %s",
            (lesson_id,),
        )
        lesson_row = cur.fetchone()
        if not lesson_row:
            return None

        cur.execute(
            """
            SELECT
                q.id,
                q.question_id,
                q.stem,
                q.option_a, q.option_b, q.option_c, q.option_d,
                COALESCE(q.option_e, '') AS option_e,
                q.correct_option,
                COALESCE(q.explanation, '') AS explanation,
                COALESCE(q.hint, '') AS hint,
                COALESCE(q.geometry_schema::text, '') AS geometry_schema
            FROM nvr_questions q
            JOIN nvr_lesson_questions nlq ON nlq.question_id = q.id
            WHERE nlq.lesson_id = %s
            ORDER BY nlq.position, q.id
            """,
            (lesson_id,),
        )
        q_rows = cur.fetchall()
        questions = []
        for r in q_rows:
            options = [r[3], r[4], r[5], r[6]]
            if r[7]:
                options.append(r[7])
            questions.append({
                "question_id": r[0],
                "question_code": r[1],
                "stem": r[2],
                "options": options,
                "correct_option": r[8],
                "correct_answer": r[8],
                "explanation": r[9],
                "hint": r[10],
                "geometry_schema": r[11],
            })

        return {
            "lesson_id": lesson_row[0],
            "lesson_name": lesson_row[1],
            "display_name": lesson_row[2],
            "questions": questions,
        }
    finally:
        cur.close()
        conn.close()


def create_nvr_lesson(
    *,
    lesson_name: str,
    display_name: str | None = None,
    topic: str | None = None,
    difficulty: str | None = None,
    description: str | None = None,
    is_active: bool = True,
) -> dict:
    cleaned_lesson_name = lesson_name.strip()
    cleaned_display_name = (display_name or cleaned_lesson_name).strip()
    cleaned_topic = (topic or "General").strip()
    cleaned_difficulty = (difficulty or "Core").strip()
    lesson_code = re.sub(r"[^a-z0-9]+", "_", cleaned_lesson_name.lower()).strip("_")

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO nvr_lessons (lesson_code, lesson_name, display_name, topic, difficulty, is_active, description)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id, lesson_code, lesson_name,
                COALESCE(display_name, lesson_name),
                COALESCE(topic, 'General'),
                COALESCE(difficulty, 'unspecified'),
                is_active,
                COALESCE(description, '')
            """,
            (
                lesson_code, cleaned_lesson_name, cleaned_display_name,
                cleaned_topic, cleaned_difficulty, is_active,
                (description or "").strip() or None,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        return {
            "lesson_id": row[0],
            "lesson_code": row[1],
            "lesson_name": row[2],
            "display_name": row[3],
            "topic": row[4],
            "difficulty": row[5],
            "is_active": row[6],
            "description": row[7],
            "item_count": 0,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def update_nvr_lesson(
    lesson_id: int,
    *,
    lesson_name: str,
    display_name: str | None = None,
) -> dict | None:
    cleaned = lesson_name.strip()
    if not cleaned:
        raise ValueError("lesson_name cannot be empty")
    cleaned_display = (display_name or cleaned).strip()

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE nvr_lessons
            SET lesson_name = %s, display_name = %s
            WHERE id = %s
            RETURNING id, lesson_name, COALESCE(display_name, lesson_name), is_active
            """,
            (cleaned, cleaned_display, lesson_id),
        )
        row = cur.fetchone()
        conn.commit()
        if not row:
            return None
        return {
            "lesson_id": row[0],
            "lesson_name": row[1],
            "display_name": row[2],
            "is_active": row[3],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def delete_nvr_lesson(lesson_id: int) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM nvr_lessons WHERE id = %s RETURNING id, lesson_name",
            (lesson_id,),
        )
        row = cur.fetchone()
        conn.commit()
        if not row:
            return None
        return {"lesson_id": row[0], "lesson_name": row[1]}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

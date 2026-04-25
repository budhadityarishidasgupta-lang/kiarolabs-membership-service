import hashlib
import re

from app.database import get_connection


def _clean_optional_text(value: str | None, max_length: int = 50) -> str | None:
    cleaned = (value or "").strip()
    if not cleaned:
        return None
    return cleaned[:max_length]


def _build_math_lesson_code_seed(lesson_name: str, display_name: str | None = None, topic: str | None = None) -> str:
    raw = " ".join(part for part in [display_name, lesson_name, topic] if part and part.strip())
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", raw.upper()).strip("_")
    if not cleaned:
        cleaned = "LESSON"
    prefix = cleaned[:6]
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:6].upper()
    return f"MATH_{prefix}_{digest}"[:50]


def _generate_unique_math_lesson_code(cur, lesson_name: str, display_name: str | None = None, topic: str | None = None) -> str:
    base_code = _build_math_lesson_code_seed(
        lesson_name=lesson_name,
        display_name=display_name,
        topic=topic,
    )

    cur.execute(
        """
        SELECT lesson_code
        FROM math_lessons
        WHERE lesson_code = %s OR lesson_code LIKE %s
        ORDER BY lesson_code ASC
        """,
        (base_code, f"{base_code}_%"),
    )
    existing_codes = {row[0] for row in cur.fetchall() if row and row[0]}

    if base_code not in existing_codes:
        return base_code

    suffix = 2
    while True:
        candidate = f"{base_code}_{suffix}"
        if candidate not in existing_codes:
            return candidate
        suffix += 1


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
        cleaned_lesson_name = _clean_optional_text(lesson_name, max_length=50)
        cleaned_display_name = _clean_optional_text(display_name, max_length=50)
        cleaned_topic = _clean_optional_text(topic, max_length=50)
        cleaned_difficulty = _clean_optional_text(difficulty, max_length=50)
        if not cleaned_lesson_name:
            raise ValueError("lesson_name is required")
        lesson_code = _generate_unique_math_lesson_code(
            cur,
            lesson_name=cleaned_lesson_name,
            display_name=cleaned_display_name,
            topic=cleaned_topic,
        )

        cur.execute(
            """
            INSERT INTO math_lessons (lesson_code, lesson_name, display_name, topic, difficulty, is_active)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING
                id,
                lesson_code,
                lesson_name,
                COALESCE(display_name, lesson_name),
                COALESCE(topic, 'General'),
                COALESCE(difficulty, 'unspecified'),
                is_active
            """,
            (
                lesson_code,
                cleaned_lesson_name,
                cleaned_display_name,
                cleaned_topic,
                cleaned_difficulty,
                is_active,
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
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

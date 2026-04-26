import hashlib
import re

from app.database import get_connection
from app.repositories.math_repository import _get_lesson_details, get_math_question_record


_TEST_FIXTURE_RE = re.compile(r"^\s*e2e\b", re.IGNORECASE)


def _is_test_fixture_text(value: str | None) -> bool:
    return bool(value and _TEST_FIXTURE_RE.search(value))


def _math_lesson_visibility_sql(table_alias: str = "") -> str:
    prefix = f"{table_alias}." if table_alias else ""
    return (
        f"NOT ("
        f"COALESCE({prefix}lesson_name, '') ~* '^\\s*e2e\\b' OR "
        f"COALESCE({prefix}display_name, '') ~* '^\\s*e2e\\b' OR "
        f"COALESCE({prefix}topic, '') ~* '^\\s*e2e\\b'"
        f")"
    )


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


def _extract_lesson_keywords(lesson_name: str, display_name: str | None = None, topic: str | None = None) -> list[str]:
    raw_parts = [display_name or "", lesson_name or "", topic or ""]
    raw_text = " ".join(part for part in raw_parts if part)
    tokens = re.findall(r"[A-Za-z0-9]+", raw_text.lower())
    stop_words = {"with", "and", "the", "same", "into", "than"}
    keywords: list[str] = []

    for token in tokens:
        if token in stop_words:
            continue
        if len(token) < 3:
            continue
        if token not in keywords:
            keywords.append(token)

    return keywords


def _count_questions_for_filter(cur, where_sql: str, params: tuple) -> int:
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM math_questions q
        WHERE {where_sql}
        """,
        params,
    )
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def _get_math_lesson_item_count(
    cur,
    lesson_name: str,
    display_name: str | None = None,
    topic: str | None = None,
    difficulty: str | None = None,
) -> int:
    topic_value = (topic or "").strip()
    difficulty_value = (difficulty or "").strip()

    if topic_value and difficulty_value:
        count = _count_questions_for_filter(
            cur,
            "LOWER(COALESCE(q.topic, '')) = LOWER(%s) AND LOWER(COALESCE(q.difficulty, '')) = LOWER(%s)",
            (topic_value, difficulty_value),
        )
        if count:
            return count

    if topic_value:
        count = _count_questions_for_filter(
            cur,
            "LOWER(COALESCE(q.topic, '')) = LOWER(%s)",
            (topic_value,),
        )
        if count:
            return count

    if difficulty_value:
        count = _count_questions_for_filter(
            cur,
            "LOWER(COALESCE(q.difficulty, '')) = LOWER(%s)",
            (difficulty_value,),
        )
        if count:
            return count

    keywords = _extract_lesson_keywords(
        lesson_name=lesson_name,
        display_name=display_name,
        topic=topic_value,
    )
    if keywords:
        keyword_clauses = []
        keyword_params: list[str] = []
        for keyword in keywords:
            like_value = f"%{keyword}%"
            keyword_clauses.append(
                "(LOWER(COALESCE(q.topic, '')) LIKE LOWER(%s) OR LOWER(COALESCE(q.stem, '')) LIKE LOWER(%s))"
            )
            keyword_params.extend([like_value, like_value])

        count = _count_questions_for_filter(
            cur,
            " OR ".join(keyword_clauses),
            tuple(keyword_params),
        )
        if count:
            return count

    return 0


def get_math_overview():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM math_lessons
            WHERE {_math_lesson_visibility_sql()}
            """
        )
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
            f"""
            SELECT
                id,
                lesson_name,
                COALESCE(display_name, lesson_name) AS display_name,
                COALESCE(topic, 'General') AS topic,
                COALESCE(difficulty, 'unspecified') AS difficulty,
                COALESCE(is_active, TRUE) AS is_active
            FROM math_lessons
            WHERE {_math_lesson_visibility_sql()}
            ORDER BY id ASC
            """
        )
        rows = cur.fetchall()

        lessons = []
        for row in rows:
            item_count = _get_math_lesson_item_count(
                cur,
                lesson_name=row[1],
                display_name=row[2],
                topic=row[3],
                difficulty=row[4],
            )
            lessons.append(
                {
                    "lesson_id": row[0],
                    "lesson_name": row[1],
                    "display_name": row[2],
                    "topic": row[3],
                    "difficulty": row[4],
                    "is_active": row[5],
                    "item_count": item_count,
                }
            )

        return lessons
    finally:
        cur.close()
        conn.close()


def list_math_lesson_question_answers(lesson_id: int):
    conn = get_connection()
    cur = conn.cursor()

    try:
        lesson = _get_lesson_details(cur, lesson_id)
        if not lesson:
            return None

        topic = (lesson.get("topic") or "").strip()
        difficulty = (lesson.get("difficulty") or "").strip()

        def _fetch(where_sql: str, params: tuple):
            cur.execute(
                f"""
                SELECT
                    q.id,
                    q.stem,
                    q.correct_option,
                    q.option_a,
                    q.option_b,
                    q.option_c,
                    q.option_d,
                    q.option_e
                FROM math_questions q
                WHERE {where_sql}
                ORDER BY q.id ASC
                """,
                params,
            )
            return cur.fetchall()

        rows = []
        if topic and difficulty:
            rows = _fetch(
                "LOWER(COALESCE(q.topic, '')) = LOWER(%s) AND LOWER(COALESCE(q.difficulty, '')) = LOWER(%s)",
                (topic, difficulty),
            )

        if not rows and topic:
            rows = _fetch(
                "LOWER(COALESCE(q.topic, '')) = LOWER(%s)",
                (topic,),
            )

        if not rows and difficulty:
            rows = _fetch(
                "LOWER(COALESCE(q.difficulty, '')) = LOWER(%s)",
                (difficulty,),
            )

        if not rows:
            keywords = _extract_lesson_keywords(
                lesson_name=lesson.get("lesson_name") or "",
                display_name=lesson.get("display_name"),
                topic=topic,
            )
            if keywords:
                keyword_clauses = []
                keyword_params: list[str] = []
                for keyword in keywords:
                    like_value = f"%{keyword}%"
                    keyword_clauses.append(
                        "(LOWER(COALESCE(q.topic, '')) LIKE LOWER(%s) OR LOWER(COALESCE(q.stem, '')) LIKE LOWER(%s))"
                    )
                    keyword_params.extend([like_value, like_value])

                rows = _fetch(" OR ".join(keyword_clauses), tuple(keyword_params))

        questions = [_format_math_question_admin_payload(row) for row in rows]

        return {
            "lesson_id": lesson["lesson_id"],
            "lesson_name": lesson["lesson_name"],
            "display_name": lesson.get("display_name") or lesson["lesson_name"],
            "questions": questions,
        }
    finally:
        cur.close()
        conn.close()


def _format_math_question_admin_payload(row):
    options_map = {
        "A": row[3] or "",
        "B": row[4] or "",
        "C": row[5] or "",
        "D": row[6] or "",
        "E": row[7] or "",
    }
    correct_option = row[2] or "A"
    return {
        "question_id": row[0],
        "stem": row[1],
        "correct_option": correct_option,
        "correct_answer": options_map.get(correct_option) or "",
        "options": options_map,
    }


def update_math_question_correct_answer(question_id: int, correct_answer: str):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cleaned_answer = (correct_answer or "").strip()
        if not cleaned_answer:
            raise ValueError("correct_answer is required")

        question = get_math_question_record(question_id)
        if not question:
            return None

        option_column_map = {
            "A": "option_a",
            "B": "option_b",
            "C": "option_c",
            "D": "option_d",
            "E": "option_e",
        }
        target_column = option_column_map.get(question.get("correct_option"))
        if not target_column:
            raise ValueError("Question is missing a valid correct option")

        cur.execute(
            f"""
            UPDATE math_questions
            SET {target_column} = %s
            WHERE id = %s
            RETURNING id, stem, correct_option, option_a, option_b, option_c, option_d, option_e
            """,
            (cleaned_answer, question_id),
        )
        row = cur.fetchone()
        conn.commit()
        if not row:
            return None

        return _format_math_question_admin_payload(row)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def update_math_question_content(
    question_id: int,
    *,
    option_a: str | None,
    option_b: str | None,
    option_c: str | None,
    option_d: str | None,
    option_e: str | None,
    correct_option: str,
):
    conn = get_connection()
    cur = conn.cursor()

    try:
        normalized_correct_option = (correct_option or "").strip().upper()
        if normalized_correct_option not in {"A", "B", "C", "D", "E"}:
            raise ValueError("correct_option must be one of A, B, C, D or E")

        normalized_options = {
            "A": (option_a or "").strip(),
            "B": (option_b or "").strip(),
            "C": (option_c or "").strip(),
            "D": (option_d or "").strip(),
            "E": (option_e or "").strip(),
        }

        if not normalized_options[normalized_correct_option]:
            raise ValueError("The selected correct option must have an answer value")

        cur.execute(
            """
            UPDATE math_questions
            SET
                option_a = %s,
                option_b = %s,
                option_c = %s,
                option_d = %s,
                option_e = %s,
                correct_option = %s
            WHERE id = %s
            RETURNING id, stem, correct_option, option_a, option_b, option_c, option_d, option_e
            """,
            (
                normalized_options["A"] or None,
                normalized_options["B"] or None,
                normalized_options["C"] or None,
                normalized_options["D"] or None,
                normalized_options["E"] or None,
                normalized_correct_option,
                question_id,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        if not row:
            return None

        return _format_math_question_admin_payload(row)
    except Exception:
        conn.rollback()
        raise
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
        if (
            _is_test_fixture_text(cleaned_lesson_name)
            or _is_test_fixture_text(cleaned_display_name)
            or _is_test_fixture_text(cleaned_topic)
        ):
            raise ValueError("Reserved test fixture names are not allowed in production lessons")
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
            "item_count": 0,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def update_math_lesson(
    lesson_id: int,
    *,
    lesson_name: str,
    display_name: str | None = None,
):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cleaned_lesson_name = _clean_optional_text(lesson_name, max_length=50)
        cleaned_display_name = _clean_optional_text(display_name, max_length=50)

        if not cleaned_lesson_name:
            raise ValueError("lesson_name is required")

        if _is_test_fixture_text(cleaned_lesson_name) or _is_test_fixture_text(cleaned_display_name):
            raise ValueError("Reserved test fixture names are not allowed in production lessons")

        cur.execute(
            """
            UPDATE math_lessons
            SET lesson_name = %s,
                display_name = %s
            WHERE id = %s
            RETURNING
                id,
                lesson_name,
                COALESCE(display_name, lesson_name) AS display_name,
                COALESCE(topic, 'General') AS topic,
                COALESCE(difficulty, 'unspecified') AS difficulty,
                COALESCE(is_active, TRUE) AS is_active
            """,
            (cleaned_lesson_name, cleaned_display_name, lesson_id),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None

        conn.commit()
        return {
            "lesson_id": row[0],
            "lesson_name": row[1],
            "display_name": row[2],
            "topic": row[3],
            "difficulty": row[4],
            "is_active": row[5],
            "item_count": _get_math_lesson_item_count(
                cur,
                lesson_name=row[1],
                display_name=row[2],
                topic=row[3],
                difficulty=row[4],
            ),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def delete_math_lesson(lesson_id: int):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            DELETE FROM math_lessons
            WHERE id = %s
            RETURNING id, lesson_code, COALESCE(display_name, lesson_name)
            """,
            (lesson_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        conn.commit()
        return {
            "lesson_id": row[0],
            "lesson_code": row[1],
            "display_name": row[2],
            "deleted": True,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def delete_e2e_math_lessons():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            DELETE FROM math_lessons
            WHERE COALESCE(lesson_name, '') ~* '^\\s*e2e\\b'
               OR COALESCE(display_name, '') ~* '^\\s*e2e\\b'
               OR COALESCE(topic, '') ~* '^\\s*e2e\\b'
            RETURNING id
            """
        )
        rows = cur.fetchall()
        conn.commit()
        return {
            "deleted_count": len(rows),
            "lesson_ids": [row[0] for row in rows],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

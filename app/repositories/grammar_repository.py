from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.database import get_connection

DEFAULT_GRAMMAR_COURSE_NAME = "GrammarSprint v1"
REVIEW_COOLDOWN_WINDOW = 4


def _first_matching_column(columns: set[str], candidates: list[str]) -> str | None:
    return next((c for c in candidates if c in columns), None)


def _get_table_columns(cur, table_name: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        """,
        (table_name,),
    )
    return {str(row[0]).strip().lower() for row in cur.fetchall() if row and row[0]}


def _get_table_column_type(cur, table_name: str, column_name: str) -> str:
    cur.execute(
        """
        SELECT LOWER(COALESCE(data_type, '')), LOWER(COALESCE(udt_name, ''))
        FROM information_schema.columns
        WHERE table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (table_name, column_name),
    )
    row = cur.fetchone()
    if not row:
        return ""
    return ":".join(str(part or "").strip().lower() for part in row if part is not None)


def _order_by_existing_columns(*, columns: set[str], preferred: list[str], alias: str | None = None) -> str:
    selected = [column for column in preferred if column in columns]
    if not selected:
        return ""
    prefix = f"{alias}." if alias else ""
    if len(selected) == 1:
        return f"ORDER BY COALESCE({prefix}{selected[0]}, 0)"
    return "ORDER BY " + ", ".join(f"COALESCE({prefix}{column}, 0)" for column in selected)


def _rows_as_dicts(cur) -> list[dict[str, Any]]:
    columns = [str(column[0]).strip().lower() for column in (cur.description or [])]
    return [dict(zip(columns, row)) for row in cur.fetchall() or []]


def _first_value(row: dict[str, Any], *keys: str, default=None):
    for key in keys:
        value = row.get(key)
        if value is not None and value != "":
            return value
    return default


def _clean_text(value: Any, default: str = "") -> str:
    return str(_first_value({"value": value}, "value", default=default) or default).strip()


def _clean_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError, AttributeError):
        return default


def _slugify(value: str) -> str:
    token = str(value or "").strip().lower()
    token = "".join(ch if ch.isalnum() else "-" for ch in token)
    while "--" in token:
        token = token.replace("--", "-")
    return token.strip("-") or "grammar"


def _normalize_option_key(value: Any) -> str:
    token = str(value or "").strip().upper()
    if token in {"A", "B", "C", "D"}:
        return token
    return ""


def _difficulty_to_storage_value(cur, value: Any) -> Any:
    raw = _clean_text(value)
    if not raw:
        return 1

    column_type = _get_table_column_type(cur, "grammar_questions", "difficulty")
    is_numeric = any(token in column_type for token in ("int", "numeric", "decimal", "real", "double"))
    if not is_numeric:
        return raw

    if raw.isdigit():
        return int(raw)

    lowered = raw.lower()
    difficulty_map = {
        "easy": 1,
        "foundation": 1,
        "beginner": 1,
        "medium": 2,
        "intermediate": 2,
        "hard": 3,
        "advanced": 3,
    }
    return difficulty_map.get(lowered, 1)


def _difficulty_to_display_value(value: Any) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""

    if raw.isdigit():
        label_map = {
            "1": "easy",
            "2": "medium",
            "3": "hard",
        }
        return label_map.get(raw, raw)
    return raw


def _normalize_selected_option(selected_option: Any, question: dict[str, Any]) -> str:
    key = _normalize_option_key(selected_option)
    if key:
        return key

    option_map = {
        "A": str(_first_value(question, "option_a", "a", default="") or "").strip(),
        "B": str(_first_value(question, "option_b", "b", default="") or "").strip(),
        "C": str(_first_value(question, "option_c", "c", default="") or "").strip(),
        "D": str(_first_value(question, "option_d", "d", default="") or "").strip(),
    }
    selected_text = str(selected_option or "").strip()
    for option_key, option_text in option_map.items():
        if option_text and option_text == selected_text:
            return option_key
    return ""


def _question_payload(question: dict[str, Any], *, include_answer: bool = False) -> dict[str, Any]:
    payload = {
        "question_id": _first_value(question, "question_id", "id"),
        "lesson_id": _first_value(question, "lesson_id"),
        "question_type": _first_value(question, "question_type", default="mcq"),
        "question_text": _first_value(question, "question_text", "stem", "prompt", default=""),
        "options": [
            _first_value(question, "option_a", "a", default=""),
            _first_value(question, "option_b", "b", default=""),
            _first_value(question, "option_c", "c", default=""),
            _first_value(question, "option_d", "d", default=""),
        ],
        "difficulty": _difficulty_to_display_value(_first_value(question, "difficulty", default="")),
        "skill_tag": _first_value(question, "skill_tag", default=""),
        "source_ref": _first_value(question, "source_ref", default=""),
        "explanation": _first_value(question, "explanation", default="") if include_answer else "",
    }
    if include_answer:
        payload["correct_option"] = _first_value(question, "correct_option", "answer_key", default="")
    return payload


def _ensure_course(cur, course_name: str, sort_order: int) -> tuple[int, bool]:
    course_name = course_name.strip() or DEFAULT_GRAMMAR_COURSE_NAME
    course_columns = _get_table_columns(cur, "grammar_courses")
    cur.execute(
        """
        SELECT *
        FROM grammar_courses
        WHERE LOWER(COALESCE(course_name, '')) = LOWER(%s)
        LIMIT 1
        """,
        (course_name,),
    )
    row = cur.fetchone()
    if row:
        columns = [str(column[0]).strip().lower() for column in (cur.description or [])]
        course = dict(zip(columns, row))
        course_id = int(_first_value(course, "course_id", "id", default=0) or 0)
        if course_id:
            updates = []
            params: list[Any] = []
            if "sort_order" in course_columns and _clean_int(_first_value(course, "sort_order", default=0)) != sort_order:
                updates.append("sort_order = %s")
                params.append(sort_order)
            if "course_code" in course_columns and not _clean_text(_first_value(course, "course_code", default="")):
                updates.append("course_code = %s")
                params.append(_slugify(course_name))
            if updates:
                params.append(course_id)
                cur.execute(
                    f"""
                    UPDATE grammar_courses
                    SET {', '.join(updates)}
                    WHERE course_id = %s
                    """,
                    tuple(params),
                )
        return course_id, False

    insert_columns = ["course_name"]
    insert_values = [course_name]
    if "course_code" in course_columns:
        insert_columns.append("course_code")
        insert_values.append(_slugify(course_name))
    if "sort_order" in course_columns:
        insert_columns.append("sort_order")
        insert_values.append(sort_order)

    cur.execute(
        f"""
        INSERT INTO grammar_courses ({', '.join(insert_columns)})
        VALUES ({', '.join(['%s'] * len(insert_columns))})
        RETURNING course_id
        """,
        tuple(insert_values),
    )
    course_row = cur.fetchone()
    course_id = int(course_row[0]) if course_row else 0
    return course_id, True


def _ensure_lesson(
    cur,
    *,
    course_id: int,
    lesson_code: str,
    lesson_name: str,
    sort_order: int,
) -> tuple[int, bool]:
    lesson_code = lesson_code.strip() or _slugify(lesson_name)
    lesson_name = lesson_name.strip()
    cur.execute(
        """
        SELECT *
        FROM grammar_lessons
        WHERE course_id = %s
          AND LOWER(COALESCE(lesson_code, '')) = LOWER(%s)
        LIMIT 1
        """,
        (course_id, lesson_code),
    )
    row = cur.fetchone()
    if row:
        columns = [str(column[0]).strip().lower() for column in (cur.description or [])]
        lesson = dict(zip(columns, row))
        lesson_id = int(_first_value(lesson, "lesson_id", "id", default=0) or 0)
        if lesson_id:
            updates = []
            params: list[Any] = []
            if _clean_text(_first_value(lesson, "lesson_name", default="")) != lesson_name:
                updates.append("lesson_name = %s")
                params.append(lesson_name)
            if _clean_int(_first_value(lesson, "sort_order", default=0)) != sort_order:
                updates.append("sort_order = %s")
                params.append(sort_order)
            if _clean_int(_first_value(lesson, "course_id", default=0)) != course_id:
                updates.append("course_id = %s")
                params.append(course_id)
            if updates:
                params.append(lesson_id)
                cur.execute(
                    f"""
                    UPDATE grammar_lessons
                    SET {', '.join(updates)}
                    WHERE lesson_id = %s
                    """,
                    tuple(params),
                )
        return lesson_id, False

    cur.execute(
        """
        INSERT INTO grammar_lessons (course_id, lesson_code, lesson_name, sort_order)
        VALUES (%s, %s, %s, %s)
        RETURNING lesson_id
        """,
        (course_id, lesson_code, lesson_name, sort_order),
    )
    lesson_row = cur.fetchone()
    lesson_id = int(lesson_row[0]) if lesson_row else 0
    return lesson_id, True


def _ensure_question(cur, row: dict[str, Any], lesson_id: int, course_id: int) -> tuple[int, bool]:
    question_text = _clean_text(_first_value(row, "question_text", default=""))
    question_type = _clean_text(_first_value(row, "question_type", default="mcq")) or "mcq"
    question_columns = _get_table_columns(cur, "grammar_questions")
    insert_fields = [
        ("course_id", course_id),
        ("lesson_id", lesson_id),
        ("question_type", question_type),
        ("question_text", question_text),
        ("option_a", _clean_text(_first_value(row, "option_a", default=""))),
        ("option_b", _clean_text(_first_value(row, "option_b", default=""))),
        ("option_c", _clean_text(_first_value(row, "option_c", default=""))),
        ("option_d", _clean_text(_first_value(row, "option_d", default=""))),
        ("correct_option", _normalize_option_key(_first_value(row, "correct_option", default=""))),
        ("explanation", _clean_text(_first_value(row, "explanation", default=""))),
        ("difficulty", _difficulty_to_storage_value(cur, _first_value(row, "difficulty", default=""))),
        ("skill_tag", _clean_text(_first_value(row, "skill_tag", default=""))),
        ("source_ref", _clean_text(_first_value(row, "source_ref", default=""))),
    ]
    if "course_id" in question_columns:
        cur.execute(
            """
            SELECT *
            FROM grammar_questions
            WHERE course_id = %s
              AND LOWER(COALESCE(question_text, '')) = LOWER(%s)
            LIMIT 1
            """,
            (course_id, question_text),
        )
    else:
        cur.execute(
            """
            SELECT q.*
            FROM grammar_questions q
            JOIN grammar_lesson_items li
              ON li.question_id = q.question_id
            WHERE li.lesson_id = %s
              AND LOWER(COALESCE(q.question_text, '')) = LOWER(%s)
            LIMIT 1
            """,
            (lesson_id, question_text),
        )
    found = cur.fetchone()
    if found:
        columns = [str(column[0]).strip().lower() for column in (cur.description or [])]
        question = dict(zip(columns, found))
        question_id = int(_first_value(question, "question_id", "id", default=0) or 0)
        if question_id:
            updates = []
            params: list[Any] = []
            for column, new_value in insert_fields:
                if column not in question_columns:
                    continue
                if _clean_text(_first_value(question, column, default="")) != _clean_text(new_value):
                    updates.append(f"{column} = %s")
                    params.append(new_value)
            if updates:
                params.append(question_id)
                cur.execute(
                    f"""
                    UPDATE grammar_questions
                    SET {', '.join(updates)}
                    WHERE question_id = %s
                    """,
                    tuple(params),
                )
        return question_id, False

    insert_columns = [column for column, _ in insert_fields if column in question_columns]
    insert_values = [value for column, value in insert_fields if column in question_columns]
    if not insert_columns:
        raise ValueError("grammar_questions table has no writable columns")

    cur.execute(
        f"""
        INSERT INTO grammar_questions ({', '.join(insert_columns)})
        VALUES ({', '.join(['%s'] * len(insert_columns))})
        RETURNING question_id
        """,
        tuple(insert_values),
    )
    question_row = cur.fetchone()
    question_id = int(question_row[0]) if question_row else 0
    return question_id, True


def _ensure_lesson_item(cur, lesson_id: int, question_id: int, sort_order: int) -> bool:
    cur.execute(
        """
        SELECT *
        FROM grammar_lesson_items
        WHERE lesson_id = %s
          AND question_id = %s
        LIMIT 1
        """,
        (lesson_id, question_id),
    )
    row = cur.fetchone()
    if row:
        columns = [str(column[0]).strip().lower() for column in (cur.description or [])]
        item = dict(zip(columns, row))
        lesson_item_id = int(_first_value(item, "lesson_item_id", "id", default=0) or 0)
        if lesson_item_id and _clean_int(_first_value(item, "sort_order", default=0)) != sort_order:
            cur.execute(
                """
                UPDATE grammar_lesson_items
                SET sort_order = %s
                WHERE lesson_item_id = %s
                """,
                (sort_order, lesson_item_id),
            )
        return False

    cur.execute(
        """
        INSERT INTO grammar_lesson_items (lesson_id, question_id, sort_order)
        VALUES (%s, %s, %s)
        """,
        (lesson_id, question_id, sort_order),
    )
    return True


def import_grammar_csv_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    conn = get_connection()
    cur = conn.cursor()
    stats = {
        "rows_processed": 0,
        "rows_skipped": 0,
        "courses_created": 0,
        "courses_updated": 0,
        "lessons_created": 0,
        "lessons_updated": 0,
        "questions_created": 0,
        "questions_updated": 0,
        "lesson_items_created": 0,
        "lesson_items_updated": 0,
    }

    try:
        for index, row in enumerate(rows, start=1):
            clean_row = {str(key).strip().lower(): value for key, value in (row or {}).items() if key}
            if not any(str(value or "").strip() for value in clean_row.values()):
                stats["rows_skipped"] += 1
                continue

            course_name = _clean_text(_first_value(clean_row, "course_name", default="")) or DEFAULT_GRAMMAR_COURSE_NAME
            lesson_code = _clean_text(_first_value(clean_row, "lesson_code", default="")) or _slugify(_clean_text(_first_value(clean_row, "lesson_name", default="")))
            lesson_name = _clean_text(_first_value(clean_row, "lesson_name", default=""))
            question_text = _clean_text(_first_value(clean_row, "question_text", default=""))
            if not lesson_name or not question_text:
                stats["rows_skipped"] += 1
                continue

            sort_order = _clean_int(_first_value(clean_row, "sort_order", default=index), default=index)
            course_id, course_created = _ensure_course(cur, course_name, sort_order)
            lesson_id, lesson_created = _ensure_lesson(
                cur,
                course_id=course_id,
                lesson_code=lesson_code,
                lesson_name=lesson_name,
                sort_order=sort_order,
            )
            question_id, question_created = _ensure_question(cur, clean_row, lesson_id, course_id)
            lesson_item_created = _ensure_lesson_item(cur, lesson_id, question_id, sort_order)

            stats["rows_processed"] += 1
            stats["courses_created"] += int(course_created)
            stats["lessons_created"] += int(lesson_created)
            stats["questions_created"] += int(question_created)
            stats["lesson_items_created"] += int(lesson_item_created)
            stats["courses_updated"] += int(not course_created)
            stats["lessons_updated"] += int(not lesson_created)
            stats["questions_updated"] += int(not question_created)
            stats["lesson_items_updated"] += int(not lesson_item_created)

        conn.commit()
        stats["course_name"] = DEFAULT_GRAMMAR_COURSE_NAME
        stats["total_rows"] = len(rows)
        return stats
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def _lesson_payload(lesson: dict[str, Any], progress: dict[str, Any] | None = None) -> dict[str, Any]:
    progress = progress or {}
    total_questions = int(progress.get("total_questions") or lesson.get("question_count") or lesson.get("item_count") or 0)
    attempts_count = int(progress.get("attempts_count") or progress.get("attempts") or 0)
    correct_count = int(progress.get("correct_count") or 0)
    wrong_count = int(progress.get("wrong_count") or 0)
    accuracy = float(progress.get("accuracy") or 0)
    completed = bool(progress.get("completed"))
    completed = completed or (total_questions > 0 and attempts_count >= total_questions)
    if attempts_count and not progress.get("accuracy"):
        accuracy = round((correct_count * 100.0 / attempts_count), 2) if attempts_count else 0.0

    return {
        "lesson_id": _first_value(lesson, "lesson_id", "id"),
        "lesson_code": _first_value(lesson, "lesson_code", default=""),
        "lesson_name": _first_value(lesson, "lesson_name", "title", "name", default=""),
        "sort_order": int(_first_value(lesson, "sort_order", "lesson_order", default=0) or 0),
        "question_count": total_questions,
        "attempts_count": attempts_count,
        "correct_count": correct_count,
        "wrong_count": wrong_count,
        "accuracy": round(accuracy, 2),
        "completed": completed,
        "progress_percent": min(100, round((attempts_count / total_questions) * 100)) if total_questions else 0,
    }


def get_grammar_courses(user_id: int | None = None) -> list[dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()
    try:
        course_columns = _get_table_columns(cur, "grammar_courses")
        lesson_columns = _get_table_columns(cur, "grammar_lessons")
        course_order_by = _order_by_existing_columns(columns=course_columns, preferred=["sort_order", "course_id"])
        cur.execute(
            "SELECT * FROM grammar_courses "
            + (course_order_by or "")
        )
        courses = _rows_as_dicts(cur)

        lesson_order_by = _order_by_existing_columns(columns=lesson_columns, preferred=["sort_order", "lesson_id"])
        cur.execute(
            "SELECT * FROM grammar_lessons "
            + (lesson_order_by or "")
        )
        lessons = _rows_as_dicts(cur)

        progress_rows: dict[int, dict[str, Any]] = {}
        if user_id is not None:
            try:
                cur.execute(
                    """
                    SELECT *
                    FROM grammar_lesson_progress
                    WHERE user_id = %s
                    """,
                    (user_id,),
                )
                for row in _rows_as_dicts(cur):
                    lesson_id = _first_value(row, "lesson_id")
                    if lesson_id is not None:
                        progress_rows[int(lesson_id)] = row
            except Exception:
                progress_rows = {}

        valid_course_ids = {
            int(_first_value(course, "course_id", "id", default=0) or 0)
            for course in courses
            if _first_value(course, "course_id", "id") is not None
        }
        primary_course_id = next(
            (
                int(_first_value(course, "course_id", "id", default=0) or 0)
                for course in courses
                if str(_first_value(course, "course_name", "title", "name", default="")).strip().lower()
                == DEFAULT_GRAMMAR_COURSE_NAME.lower()
            ),
            int(_first_value(courses[0], "course_id", "id", default=0) or 0) if courses else 0,
        )

        lesson_buckets: dict[int, list[dict[str, Any]]] = {}
        for lesson in lessons:
            course_id = _first_value(lesson, "course_id")
            bucket_id = int(course_id or 0)
            if bucket_id not in valid_course_ids:
                bucket_id = primary_course_id
            lesson_payload = _lesson_payload(lesson, progress_rows.get(int(_first_value(lesson, "lesson_id", "id", default=0) or 0), {}))
            lesson_buckets.setdefault(bucket_id, []).append(lesson_payload)

        if not courses:
            # Fall back to a single inferred course wrapper when the database only has lessons.
            return [
                {
                    "course_id": 0,
                    "course_name": DEFAULT_GRAMMAR_COURSE_NAME,
                    "lessons": sorted(
                        [
                            _lesson_payload(lesson, progress_rows.get(int(_first_value(lesson, "lesson_id", "id", default=0) or 0), {}))
                            for lesson in lessons
                        ],
                        key=lambda item: (item["sort_order"], item["lesson_id"] or 0),
                    ),
                }
            ]

        result = []
        for course in courses:
            course_id = int(_first_value(course, "course_id", "id", default=0) or 0)
            result.append(
                {
                    "course_id": course_id,
                    "course_code": _first_value(course, "course_code", default=""),
                    "course_name": _first_value(course, "course_name", "title", "name", default=DEFAULT_GRAMMAR_COURSE_NAME),
                    "lessons": sorted(
                        lesson_buckets.get(course_id, []),
                        key=lambda item: (item["sort_order"], item["lesson_id"] or 0),
                    ),
                }
            )
        return result
    finally:
        cur.close()
        conn.close()


def get_grammar_lessons(user_id: int | None = None, course_name: str = DEFAULT_GRAMMAR_COURSE_NAME) -> dict[str, Any]:
    courses = get_grammar_courses(user_id=user_id)
    target_course = next(
        (
            course
            for course in courses
            if str(course.get("course_name") or "").strip().lower() == str(course_name or "").strip().lower()
            or not course_name
        ),
        courses[0] if courses else {"course_name": DEFAULT_GRAMMAR_COURSE_NAME, "lessons": []},
    )
    lessons = list(target_course.get("lessons") or [])
    next_incomplete_lesson_id = next((lesson["lesson_id"] for lesson in lessons if not lesson.get("completed")), None)
    return {
        "course_id": target_course.get("course_id"),
        "course_name": target_course.get("course_name") or DEFAULT_GRAMMAR_COURSE_NAME,
        "lessons": lessons,
        "next_incomplete_lesson_id": next_incomplete_lesson_id,
    }


def _fetch_grammar_lessons_map(cur) -> dict[int, dict[str, Any]]:
    lesson_columns = _get_table_columns(cur, "grammar_lessons")
    lesson_order_by = _order_by_existing_columns(columns=lesson_columns, preferred=["sort_order", "lesson_id"])
    cur.execute(
        "SELECT * FROM grammar_lessons " + (lesson_order_by or "")
    )
    lessons = _rows_as_dicts(cur)
    return {
        int(_first_value(lesson, "lesson_id", "id", default=0) or 0): lesson
        for lesson in lessons
        if _first_value(lesson, "lesson_id", "id") is not None
    }


def _fetch_grammar_question_map(cur, lesson_id: int) -> list[dict[str, Any]]:
    lesson_item_columns = _get_table_columns(cur, "grammar_lesson_items")
    question_order_by = _order_by_existing_columns(columns=lesson_item_columns, preferred=["sort_order", "question_id"], alias="li")
    cur.execute(
        """
        SELECT q.*
        FROM grammar_lesson_items li
        JOIN grammar_questions q
          ON q.question_id = li.question_id
        WHERE li.lesson_id = %s
        """
        + (question_order_by or "ORDER BY li.question_id")
        ,
        (lesson_id,),
    )
    return _rows_as_dicts(cur)


def _fetch_question_stats(cur, user_id: int) -> dict[int, dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT *
            FROM grammar_question_stats
            WHERE user_id = %s
            """,
            (user_id,),
        )
        stats = {}
        for row in _rows_as_dicts(cur):
            question_id = _first_value(row, "question_id")
            if question_id is None:
                continue
            stats[int(question_id)] = row
        return stats
    except Exception:
        return {}


def _fetch_attempt_history(cur, user_id: int, lesson_id: int) -> list[dict[str, Any]]:
    attempt_columns = _get_table_columns(cur, "grammar_attempts")
    timestamp_column = _first_matching_column(attempt_columns, ["created_at", "submitted_at", "attempted_at"])
    attempt_id_column = _first_matching_column(attempt_columns, ["attempt_id", "id"])
    order_parts = []
    if timestamp_column:
        order_parts.append(f"COALESCE({timestamp_column}, NOW()) DESC")
    if attempt_id_column:
        order_parts.append(f"COALESCE({attempt_id_column}, 0) DESC")
    order_clause = "ORDER BY " + ", ".join(order_parts) if order_parts else ""
    cur.execute(
        "SELECT * FROM grammar_attempts WHERE user_id = %s AND lesson_id = %s "
        + (order_clause + " " if order_clause else ""),
        (user_id, lesson_id),
    )
    return _rows_as_dicts(cur)


def _question_attempt_count(stats_row: dict[str, Any] | None) -> int:
    if not stats_row:
        return 0
    return int(_first_value(stats_row, "attempts_count", "attempts", default=0) or 0)


def _question_correct_count(stats_row: dict[str, Any] | None) -> int:
    if not stats_row:
        return 0
    return int(_first_value(stats_row, "correct_count", "correct", default=0) or 0)


def _question_wrong_count(stats_row: dict[str, Any] | None) -> int:
    if not stats_row:
        return 0
    return int(_first_value(stats_row, "wrong_count", "wrong", default=0) or 0)


def _question_accuracy(stats_row: dict[str, Any] | None) -> float:
    if not stats_row:
        return 0.0
    value = _first_value(stats_row, "accuracy", default=0)
    if value is None:
        attempts = _question_attempt_count(stats_row)
        correct = _question_correct_count(stats_row)
        return round((correct * 100.0 / attempts), 2) if attempts else 0.0
    return float(value or 0)


def _choose_next_grammar_question(questions: list[dict[str, Any]], stats_rows: dict[int, dict[str, Any]], recent_question_ids: set[int]) -> dict[str, Any] | None:
    if not questions:
        return None

    unanswered = [question for question in questions if _question_attempt_count(stats_rows.get(int(_first_value(question, "question_id", "id", default=0) or 0))) == 0]
    if unanswered:
        return unanswered[0]

    ranked = sorted(
        questions,
        key=lambda question: (
            _question_accuracy(stats_rows.get(int(_first_value(question, "question_id", "id", default=0) or 0))),
            -_question_attempt_count(stats_rows.get(int(_first_value(question, "question_id", "id", default=0) or 0))),
            int(_first_value(question, "question_id", "id", default=0) or 0),
        ),
    )
    for question in ranked:
        question_id = int(_first_value(question, "question_id", "id", default=0) or 0)
        if question_id and question_id not in recent_question_ids:
            return question
    return ranked[0]


def get_grammar_question(lesson_id: int, user_id: int, session_id: str | None = None) -> dict[str, Any] | None:
    conn = get_connection()
    cur = conn.cursor()
    try:
        lesson = None
        lesson_map = _fetch_grammar_lessons_map(cur)
        lesson = lesson_map.get(int(lesson_id))
        if not lesson:
            return None

        questions = _fetch_grammar_question_map(cur, lesson_id)
        if not questions:
            return None

        stats_rows = _fetch_question_stats(cur, user_id)
        attempts = _fetch_attempt_history(cur, user_id, lesson_id)
        recent_question_ids = {
            int(_first_value(row, "question_id", default=0) or 0)
            for row in attempts[:REVIEW_COOLDOWN_WINDOW]
            if _first_value(row, "question_id") is not None
        }
        selected_question = _choose_next_grammar_question(questions, stats_rows, recent_question_ids)
        if not selected_question:
            return None

        question_id = int(_first_value(selected_question, "question_id", "id", default=0) or 0)
        stats = stats_rows.get(question_id)
        question_position = len(attempts) + 1
        review_reason = None
        if stats and _question_attempt_count(stats) > 0 and question_id in recent_question_ids:
            review_reason = "review_question"
        elif stats and _question_accuracy(stats) < 70 and _question_attempt_count(stats) >= 2:
            review_reason = "weak_question"

        payload = _question_payload(selected_question, include_answer=False)
        if not payload.get("lesson_id"):
            payload["lesson_id"] = lesson_id
        payload.update(
            {
                "lesson_name": _first_value(lesson, "lesson_name", "title", "name", default=""),
                "lesson_code": _first_value(lesson, "lesson_code", default=""),
                "session_id": session_id,
                "question_position": question_position,
                "lesson_item_count": len(questions),
                "review_reason": review_reason,
                "is_review": bool(review_reason),
                "session_state": {
                    "is_review": bool(review_reason),
                    "review_reason": review_reason,
                    "question_position": question_position,
                    "cooldown_distance": REVIEW_COOLDOWN_WINDOW if review_reason else None,
                },
            }
        )
        if review_reason:
            payload["encouragement_message"] = "Let's practise this one again - you were close last time."
        return payload
    finally:
        cur.close()
        conn.close()


def _upsert_question_stats(cur, user_id: int, question_id: int, correct: bool) -> None:
    # DB schema: grammar_question_stats(id, user_id, question_id, attempts_count, correct_count,
    #   wrong_count, accuracy, last_attempt_at, last_correct_at, created_at, updated_at)
    accuracy = 100.0 if correct else 0.0
    cur.execute(
        """
        INSERT INTO grammar_question_stats
            (user_id, question_id, attempts_count, correct_count, wrong_count, accuracy,
             last_attempt_at, updated_at)
        VALUES (%s, %s, 1, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (user_id, question_id)
        DO UPDATE SET
            attempts_count = grammar_question_stats.attempts_count + 1,
            correct_count  = grammar_question_stats.correct_count + EXCLUDED.correct_count,
            wrong_count    = grammar_question_stats.wrong_count + EXCLUDED.wrong_count,
            accuracy = CASE
                WHEN (grammar_question_stats.attempts_count + 1) > 0 THEN
                    ROUND(
                        ((grammar_question_stats.correct_count + EXCLUDED.correct_count) * 100.0)
                        / (grammar_question_stats.attempts_count + 1), 2)
                ELSE 0
            END,
            last_attempt_at = NOW(),
            updated_at = NOW()
        """,
        (user_id, question_id, 1 if correct else 0, 0 if correct else 1, accuracy),
    )


def _upsert_lesson_progress(cur, user_id: int, lesson_id: int) -> dict[str, Any]:
    # DB schema: grammar_lesson_progress(id, user_id, course_id, lesson_id, questions_attempted,
    #   correct_count, wrong_count, accuracy, completed, last_attempt_at, completed_at,
    #   created_at, updated_at)
    cur.execute(
        """
        SELECT
            COUNT(DISTINCT question_id) AS questions_attempted,
            COALESCE(SUM(CASE WHEN correct THEN 1 ELSE 0 END), 0) AS correct_count,
            COALESCE(SUM(CASE WHEN correct THEN 0 ELSE 1 END), 0) AS wrong_count,
            COUNT(*) AS total_attempts
        FROM grammar_attempts
        WHERE user_id = %s AND lesson_id = %s
        """,
        (user_id, lesson_id),
    )
    row = cur.fetchone() or (0, 0, 0, 0)
    questions_attempted = int(row[0] or 0)
    correct_count = int(row[1] or 0)
    wrong_count = int(row[2] or 0)
    total_attempts = int(row[3] or 0)

    cur.execute(
        "SELECT COUNT(*) FROM grammar_lesson_items WHERE lesson_id = %s",
        (lesson_id,),
    )
    total_questions = int((cur.fetchone() or (0,))[0] or 0)
    accuracy = round((correct_count * 100.0 / total_attempts), 2) if total_attempts else 0.0
    completed = bool(total_questions and questions_attempted >= total_questions)

    # Get course_id for this lesson
    cur.execute("SELECT course_id FROM grammar_lessons WHERE lesson_id = %s", (lesson_id,))
    lesson_row = cur.fetchone()
    course_id = lesson_row[0] if lesson_row else None

    cur.execute(
        """
        INSERT INTO grammar_lesson_progress
            (user_id, course_id, lesson_id, questions_attempted, correct_count, wrong_count,
             accuracy, completed, last_attempt_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (user_id, lesson_id)
        DO UPDATE SET
            questions_attempted = EXCLUDED.questions_attempted,
            correct_count       = EXCLUDED.correct_count,
            wrong_count         = EXCLUDED.wrong_count,
            accuracy            = EXCLUDED.accuracy,
            completed           = EXCLUDED.completed,
            last_attempt_at     = NOW(),
            updated_at          = NOW()
        """,
        (user_id, course_id, lesson_id, questions_attempted, correct_count, wrong_count,
         accuracy, completed),
    )
    return {
        "user_id": user_id,
        "lesson_id": lesson_id,
        "questions_attempted": questions_attempted,
        "correct_count": correct_count,
        "wrong_count": wrong_count,
        "accuracy": accuracy,
        "completed": completed,
        "progress_percent": round(questions_attempted * 100.0 / total_questions, 1) if total_questions else 0,
    }


def record_grammar_attempt(
    user_id: int,
    lesson_id: int,
    question_id: int,
    selected_option: Any,
    correct: bool,
    session_id: str | None = None,
) -> dict[str, Any]:
    conn = get_connection()
    cur = conn.cursor()
    try:
        # DB schema: grammar_attempts(attempt_id, user_id, course_id, lesson_id, question_id,
        #   selected_option, correct, time_taken, session_id uuid, contract_version,
        #   created_at, submitted_at)
        # Get course_id for this lesson
        cur.execute("SELECT course_id FROM grammar_lessons WHERE lesson_id = %s", (lesson_id,))
        _lesson_row = cur.fetchone()
        _course_id = _lesson_row[0] if _lesson_row else None
        # Pass session_id as plain string — psycopg2 handles casting to uuid column
        _session_uuid = str(session_id) if session_id else None
        cur.execute(
            """
            INSERT INTO grammar_attempts
                (user_id, course_id, lesson_id, question_id, selected_option, correct,
                 session_id, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            RETURNING *
            """,
            (user_id, _course_id, lesson_id, question_id, selected_option, correct, _session_uuid),
        )
        attempt_row = cur.fetchone()
        _upsert_question_stats(cur, user_id, question_id, correct)
        progress_row = _upsert_lesson_progress(cur, user_id, lesson_id)
        conn.commit()
        return {
            "attempt": attempt_row,
            "progress": progress_row,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def submit_grammar_answer(
    user_id: int,
    lesson_id: int,
    question_id: int,
    selected_option: Any,
    session_id: str | None = None,
) -> dict[str, Any]:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT *
            FROM grammar_questions
            WHERE question_id = %s
            LIMIT 1
            """,
            (question_id,),
        )
        row = cur.fetchone()
        if not row:
            return {"error": "Question not found"}
        columns = [str(column[0]).strip().lower() for column in (cur.description or [])]
        question = dict(zip(columns, row))
        correct_option = str(_first_value(question, "correct_option", default="")).strip().upper()
        normalized_selected = _normalize_selected_option(selected_option, question)
        if not normalized_selected:
            return {"error": "Invalid selected option"}

        is_correct = normalized_selected == correct_option
        attempt_result = record_grammar_attempt(
            user_id=user_id,
            lesson_id=lesson_id,
            question_id=question_id,
            selected_option=normalized_selected,
            correct=is_correct,
            session_id=session_id,
        )

        lesson_progress = attempt_result.get("progress") or _upsert_lesson_progress(cur, user_id, lesson_id)
        next_question = get_grammar_question(lesson_id=lesson_id, user_id=user_id, session_id=session_id)
        return {
            "correct": is_correct,
            "selected_option": normalized_selected,
            "correct_option": correct_option,
            "question_id": question_id,
            "lesson_id": lesson_id,
            "explanation": _first_value(question, "explanation", default=""),
            "difficulty": _first_value(question, "difficulty", default=""),
            "skill_tag": _first_value(question, "skill_tag", default=""),
            "lesson_progress": lesson_progress,
            "next_question": next_question,
        }
    finally:
        cur.close()
        conn.close()


def get_grammar_resume(user_id: int) -> dict[str, Any] | None:
    conn = get_connection()
    cur = conn.cursor()
    try:
        attempt_columns = _get_table_columns(cur, "grammar_attempts")
        timestamp_column = _first_matching_column(attempt_columns, ["created_at", "submitted_at", "attempted_at"])
        attempt_id_column = _first_matching_column(attempt_columns, ["attempt_id", "id"])
        order_parts = []
        if timestamp_column:
            order_parts.append(f"COALESCE({timestamp_column}, NOW()) DESC")
        if attempt_id_column:
            order_parts.append(f"COALESCE({attempt_id_column}, 0) DESC")
        order_clause = "ORDER BY " + ", ".join(order_parts) if order_parts else ""
        cur.execute(
            "SELECT * FROM grammar_attempts WHERE user_id = %s "
            + (order_clause + " " if order_clause else "")
            + "LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        columns = [str(column[0]).strip().lower() for column in (cur.description or [])]
        attempt = dict(zip(columns, row))
        lesson_id = int(_first_value(attempt, "lesson_id", default=0) or 0)
        question_id = int(_first_value(attempt, "question_id", default=0) or 0)
        return {
            "lesson_id": lesson_id,
            "question_id": question_id,
            "session_id": _first_value(attempt, "session_id", default=""),
            "created_at": _first_value(attempt, "created_at", "submitted_at", "attempted_at", default=None),
        }
    finally:
        cur.close()
        conn.close()

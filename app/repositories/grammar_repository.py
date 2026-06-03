from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.database import get_connection

DEFAULT_GRAMMAR_COURSE_NAME = "GrammarSprint v1"
REVIEW_COOLDOWN_WINDOW = 4


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


def _rows_as_dicts(cur) -> list[dict[str, Any]]:
    columns = [str(column[0]).strip().lower() for column in (cur.description or [])]
    return [dict(zip(columns, row)) for row in cur.fetchall() or []]


def _first_value(row: dict[str, Any], *keys: str, default=None):
    for key in keys:
        value = row.get(key)
        if value is not None and value != "":
            return value
    return default


def _normalize_option_key(value: Any) -> str:
    token = str(value or "").strip().upper()
    if token in {"A", "B", "C", "D"}:
        return token
    return ""


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
        "difficulty": _first_value(question, "difficulty", default=""),
        "skill_tag": _first_value(question, "skill_tag", default=""),
        "source_ref": _first_value(question, "source_ref", default=""),
        "explanation": _first_value(question, "explanation", default="") if include_answer else "",
    }
    if include_answer:
        payload["correct_option"] = _first_value(question, "correct_option", "answer_key", default="")
    return payload


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
        cur.execute(
            """
            SELECT *
            FROM grammar_courses
            ORDER BY COALESCE(sort_order, 0), COALESCE(course_id, id)
            """
        )
        courses = _rows_as_dicts(cur)

        cur.execute(
            """
            SELECT *
            FROM grammar_lessons
            ORDER BY COALESCE(sort_order, 0), COALESCE(lesson_id, id)
            """
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

        lesson_buckets: dict[int, list[dict[str, Any]]] = {}
        for lesson in lessons:
            course_id = _first_value(lesson, "course_id")
            if course_id is None:
                continue
            lesson_payload = _lesson_payload(lesson, progress_rows.get(int(_first_value(lesson, "lesson_id", "id", default=0) or 0), {}))
            lesson_buckets.setdefault(int(course_id), []).append(lesson_payload)

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
    cur.execute("SELECT * FROM grammar_lessons ORDER BY COALESCE(sort_order, 0), COALESCE(lesson_id, id)")
    lessons = _rows_as_dicts(cur)
    return {
        int(_first_value(lesson, "lesson_id", "id", default=0) or 0): lesson
        for lesson in lessons
        if _first_value(lesson, "lesson_id", "id") is not None
    }


def _fetch_grammar_question_map(cur, lesson_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT q.*
        FROM grammar_lesson_items li
        JOIN grammar_questions q
          ON q.question_id = li.question_id
        WHERE li.lesson_id = %s
        ORDER BY COALESCE(li.sort_order, 0), COALESCE(li.id, li.lesson_item_id, li.question_id)
        """,
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
    cur.execute(
        """
        SELECT *
        FROM grammar_attempts
        WHERE user_id = %s
          AND lesson_id = %s
        ORDER BY COALESCE(created_at, attempted_at, submitted_at, NOW()) DESC, COALESCE(attempt_id, id) DESC
        """,
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
    attempts_count = 1
    correct_count = 1 if correct else 0
    wrong_count = 0 if correct else 1
    cur.execute(
        """
        INSERT INTO grammar_question_stats (
            user_id,
            question_id,
            attempts_count,
            correct_count,
            wrong_count,
            accuracy,
            is_weak,
            last_attempt_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (user_id, question_id)
        DO UPDATE SET
            attempts_count = grammar_question_stats.attempts_count + EXCLUDED.attempts_count,
            correct_count = grammar_question_stats.correct_count + EXCLUDED.correct_count,
            wrong_count = grammar_question_stats.wrong_count + EXCLUDED.wrong_count,
            accuracy = CASE
                WHEN (grammar_question_stats.attempts_count + EXCLUDED.attempts_count) > 0 THEN
                    ROUND(
                        ((grammar_question_stats.correct_count + EXCLUDED.correct_count) * 100.0)
                        / (grammar_question_stats.attempts_count + EXCLUDED.attempts_count),
                        2
                    )
                ELSE 0
            END,
            is_weak = CASE
                WHEN (grammar_question_stats.attempts_count + EXCLUDED.attempts_count) >= 2
                 AND ((grammar_question_stats.correct_count + EXCLUDED.correct_count) * 100.0)
                     / NULLIF(grammar_question_stats.attempts_count + EXCLUDED.attempts_count, 0) < 70
                THEN TRUE
                ELSE FALSE
            END,
            last_attempt_at = NOW(),
            updated_at = NOW()
        """,
        (
            user_id,
            question_id,
            attempts_count,
            correct_count,
            wrong_count,
            100.0 if correct else 0.0,
            not correct,
        ),
    )


def _upsert_lesson_progress(cur, user_id: int, lesson_id: int) -> dict[str, Any]:
    cur.execute(
        """
        SELECT
            COUNT(*) AS attempts_count,
            COALESCE(SUM(CASE WHEN correct THEN 1 ELSE 0 END), 0) AS correct_count,
            COALESCE(SUM(CASE WHEN correct THEN 0 ELSE 1 END), 0) AS wrong_count,
            COUNT(DISTINCT question_id) AS attempted_questions
        FROM grammar_attempts
        WHERE user_id = %s
          AND lesson_id = %s
        """,
        (user_id, lesson_id),
    )
    row = cur.fetchone() or (0, 0, 0, 0)
    attempts_count = int(row[0] or 0)
    correct_count = int(row[1] or 0)
    wrong_count = int(row[2] or 0)
    attempted_questions = int(row[3] or 0)

    cur.execute(
        """
        SELECT COUNT(*)
        FROM grammar_lesson_items
        WHERE lesson_id = %s
        """,
        (lesson_id,),
    )
    total_questions = int((cur.fetchone() or (0,))[0] or 0)
    accuracy = round((correct_count * 100.0 / attempts_count), 2) if attempts_count else 0.0
    completed = bool(total_questions and attempted_questions >= total_questions)

    cur.execute(
        """
        INSERT INTO grammar_lesson_progress (
            user_id,
            lesson_id,
            total_questions,
            attempts_count,
            correct_count,
            wrong_count,
            accuracy,
            completed,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (user_id, lesson_id)
        DO UPDATE SET
            total_questions = EXCLUDED.total_questions,
            attempts_count = EXCLUDED.attempts_count,
            correct_count = EXCLUDED.correct_count,
            wrong_count = EXCLUDED.wrong_count,
            accuracy = EXCLUDED.accuracy,
            completed = EXCLUDED.completed,
            updated_at = NOW()
        RETURNING *
        """,
        (
            user_id,
            lesson_id,
            total_questions,
            attempts_count,
            correct_count,
            wrong_count,
            accuracy,
            completed,
        ),
    )
    progress_row = cur.fetchone()
    if isinstance(progress_row, tuple):
        return {
            "user_id": user_id,
            "lesson_id": lesson_id,
            "total_questions": total_questions,
            "attempts_count": attempts_count,
            "correct_count": correct_count,
            "wrong_count": wrong_count,
            "accuracy": accuracy,
            "completed": completed,
        }
    return progress_row or {
        "user_id": user_id,
        "lesson_id": lesson_id,
        "total_questions": total_questions,
        "attempts_count": attempts_count,
        "correct_count": correct_count,
        "wrong_count": wrong_count,
        "accuracy": accuracy,
        "completed": completed,
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
        cur.execute(
            """
            INSERT INTO grammar_attempts (
                user_id,
                lesson_id,
                question_id,
                selected_option,
                correct,
                session_id,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            RETURNING *
            """,
            (
                user_id,
                lesson_id,
                question_id,
                selected_option,
                correct,
                session_id,
            ),
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
        cur.execute(
            """
            SELECT *
            FROM grammar_attempts
            WHERE user_id = %s
            ORDER BY COALESCE(created_at, submitted_at, attempted_at, NOW()) DESC,
                     COALESCE(attempt_id, id) DESC
            LIMIT 1
            """,
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

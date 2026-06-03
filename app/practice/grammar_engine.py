from __future__ import annotations

import uuid
from typing import Any

from app.repositories.grammar_repository import (
    DEFAULT_GRAMMAR_COURSE_NAME,
    get_grammar_lessons,
    get_grammar_question,
    get_grammar_resume,
    submit_grammar_answer,
)


def _normalize_session_id(session_id: str | None) -> str:
    token = str(session_id or "").strip()
    return token or str(uuid.uuid4())


def get_grammar_courses(user_id: int | None = None):
    return get_grammar_lessons(user_id=user_id, course_name=DEFAULT_GRAMMAR_COURSE_NAME)


def get_grammar_question_for_lesson(
    lesson_id: int,
    user_id: int,
    session_id: str | None = None,
) -> dict[str, Any] | None:
    normalized_session_id = _normalize_session_id(session_id)
    question = get_grammar_question(
        lesson_id=lesson_id,
        user_id=user_id,
        session_id=normalized_session_id,
    )
    if not question:
        return None
    question["session_id"] = normalized_session_id
    return question


def submit_grammar_answer_for_lesson(
    *,
    user_id: int,
    lesson_id: int,
    question_id: int,
    selected_option: Any,
    session_id: str | None = None,
) -> dict[str, Any]:
    normalized_session_id = _normalize_session_id(session_id)
    result = submit_grammar_answer(
        user_id=user_id,
        lesson_id=lesson_id,
        question_id=question_id,
        selected_option=selected_option,
        session_id=normalized_session_id,
    )
    if isinstance(result, dict):
        result["session_id"] = normalized_session_id
    return result


def get_grammar_resume_for_user(user_id: int):
    return get_grammar_resume(user_id)

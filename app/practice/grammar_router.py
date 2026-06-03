from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from app.auth import get_current_user, require_member_app_access, resolve_verified_learning_user_id
from app.database import get_connection
from app.practice.grammar_engine import (
    get_grammar_courses,
    get_grammar_question_for_lesson,
    get_grammar_resume_for_user,
    submit_grammar_answer_for_lesson,
)

router = APIRouter(prefix="/practice/grammar", tags=["grammar"])
logger = logging.getLogger(__name__)


def _require_grammar_access(user: dict[str, Any] | None) -> None:
    require_member_app_access(user, "grammar")


def _resolve_user_id(user: dict[str, Any]) -> int:
    user_id = user.get("user_id")
    if user_id:
        try:
            return int(user_id)
        except (TypeError, ValueError):
            pass

    conn = get_connection()
    cur = conn.cursor()
    try:
        resolved = resolve_verified_learning_user_id(cur, user)
        if not resolved:
            raise HTTPException(status_code=400, detail="User not provisioned in learning system")
        return int(resolved)
    finally:
        cur.close()
        conn.close()


@router.get("/courses")
def grammar_courses(user=Depends(get_current_user)):
    _require_grammar_access(user)
    user_id = _resolve_user_id(user)
    return get_grammar_courses(user_id=user_id)


@router.get("/lessons")
def grammar_lessons(user=Depends(get_current_user)):
    _require_grammar_access(user)
    user_id = _resolve_user_id(user)
    return get_grammar_courses(user_id=user_id)


@router.get("/question")
def grammar_question(
    lesson_id: int | None = None,
    session_id: str | None = None,
    user=Depends(get_current_user),
):
    _require_grammar_access(user)
    if lesson_id is None:
        raise HTTPException(status_code=400, detail="Missing required parameter: lesson_id")

    user_id = _resolve_user_id(user)
    result = get_grammar_question_for_lesson(
        lesson_id=lesson_id,
        user_id=user_id,
        session_id=session_id,
    )
    if not result:
        raise HTTPException(status_code=404, detail="Question not found")
    return result


@router.post("/submit")
def grammar_submit(payload: dict, user=Depends(get_current_user)):
    _require_grammar_access(user)
    user_id = _resolve_user_id(user)

    lesson_id = payload.get("lesson_id")
    question_id = payload.get("question_id")
    selected_option = payload.get("selected_option")
    session_id = payload.get("session_id")

    if lesson_id is None:
        raise HTTPException(status_code=400, detail="Missing required parameter: lesson_id")
    if question_id is None:
        raise HTTPException(status_code=400, detail="Missing required parameter: question_id")
    if selected_option is None:
        raise HTTPException(status_code=400, detail="Missing required parameter: selected_option")

    result = submit_grammar_answer_for_lesson(
        user_id=user_id,
        lesson_id=int(lesson_id),
        question_id=int(question_id),
        selected_option=selected_option,
        session_id=session_id,
    )
    if isinstance(result, dict) and result.get("error"):
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.get("/resume")
def grammar_resume(user=Depends(get_current_user)):
    _require_grammar_access(user)
    user_id = _resolve_user_id(user)
    return get_grammar_resume_for_user(user_id)

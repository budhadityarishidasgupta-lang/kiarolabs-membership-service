from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from typing import Optional
from pydantic import BaseModel

from app.auth import get_current_user

# Engines
from app.practice.math_engine import get_math_question
from app.practice.spelling_engine import get_spelling_question
from app.practice.synonym_engine import (
    get_synonym_question,
    submit_synonym_answer,
    get_synonym_progress,
    get_next_synonym_question,
    get_dashboard_stats,
    get_practice_session,
)

router = APIRouter(prefix="/practice", tags=["practice"])


# -----------------------------
# Request Models
# -----------------------------

class SynonymAnswerRequest(BaseModel):
    word_id: int
    chosen: str
    response_ms: int


class SessionAnswerRequest(BaseModel):
    word_id: int
    chosen: str
    response_ms: int


# -----------------------------
# MathSprint Endpoints
# -----------------------------

@router.get("/math/question")
def math_question(user=Depends(get_current_user)):
    return get_math_question(user["id"])


# -----------------------------
# SpellingSprint Endpoints
# -----------------------------

@router.get("/spelling/question")
def spelling_question(user=Depends(get_current_user)):
    return get_spelling_question(user["id"])


# -----------------------------
# WordSprint (Synonym) Endpoints
# -----------------------------

@router.get("/synonym/question")
def synonym_question(user=Depends(get_current_user)):
    return get_synonym_question(user["sub"])


@router.post("/synonym/answer")
def synonym_answer(req: SynonymAnswerRequest, user=Depends(get_current_user)):
    return submit_synonym_answer(
        user_id=user["id"],
        user_email=user["sub"],
        word_id=req.word_id,
        chosen=req.chosen,
        response_ms=req.response_ms,
    )


@router.get("/synonym/progress")
def synonym_progress(user=Depends(get_current_user)):
    return get_synonym_progress(user["sub"])


@router.get("/synonym/next-question")
def synonym_next(user=Depends(get_current_user)):
    return get_next_synonym_question(user["sub"])


# -----------------------------
# Unified Practice Session API
# -----------------------------

@router.get("/session/start")
def start_session(lesson_id: Optional[int] = 7, user=Depends(get_current_user)):
    """
    Starts a learning session and returns:
    - user progress
    - first question
    - session metadata
    """

    # Default fallback lesson for legacy frontend
    if lesson_id is None:
        lesson_id = 7

    return get_practice_session(user["sub"], lesson_id)


@router.post("/session/answer")
def session_answer(req: SessionAnswerRequest, user=Depends(get_current_user)):
    """
    Unified answer endpoint for frontend session handling.
    Currently routes to synonym engine.
    """
    return submit_synonym_answer(
        user_id=user["id"],
        user_email=user["sub"],
        word_id=req.word_id,
        chosen=req.chosen,
        response_ms=req.response_ms,
    )


@router.get("/session/next")
def session_next(user=Depends(get_current_user)):
    """
    Returns next question in session.
    """
    return get_next_synonym_question(user["sub"])


# -----------------------------
# Dashboard
# -----------------------------

@router.get("/dashboard")
def dashboard(user=Depends(get_current_user)):
    """
    Returns student progress summary
    across learning modules.
    """
    return get_dashboard_stats(user["sub"])

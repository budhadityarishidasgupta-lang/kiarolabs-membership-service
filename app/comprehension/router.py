import logging

from fastapi import APIRouter, Depends, HTTPException
from app.auth import get_current_user

from app.comprehension.service import (
    list_passages,
    start_passage,
    submit_answer
)
from app.comprehension.repository import get_question_by_id

router = APIRouter(
    prefix="/practice/comprehension",
    tags=["comprehension"]
)
logger = logging.getLogger(__name__)


def _missing_param(name: str):
    logger.warning("Comprehension endpoint missing required parameter: %s", name)
    raise HTTPException(status_code=400, detail=f"Missing required parameter: {name}")


def _require_user_id(user):
    user_id = user.get("user_id")
    if not user_id:
        logger.warning("Comprehension endpoint rejected request because user_id is missing")
        raise HTTPException(status_code=400, detail="User not provisioned in learning system")
    return user_id


def _require_payload_param(payload: dict, name: str):
    if not isinstance(payload, dict):
        _missing_param(name)
    value = payload.get(name)
    if value is None:
        _missing_param(name)
    return value


def _safe_execute(label: str, func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected comprehension endpoint failure in %s", label)
        raise HTTPException(status_code=500, detail="Internal error. Please try again.")


@router.get("/passages")
def get_passages(user=Depends(get_current_user)):
    return _safe_execute("get_passages", list_passages)


@router.get("/start")
def start(passage_id: int | None = None, user=Depends(get_current_user)):
    if passage_id is None:
        _missing_param("passage_id")

    _require_user_id(user)
    result = _safe_execute("start", start_passage, passage_id)

    if not result:
        logger.warning("Comprehension start invalid passage_id: %s", passage_id)
        raise HTTPException(status_code=404, detail="Passage not found")

    return result


@router.post("/answer")
def answer(payload: dict, user=Depends(get_current_user)):
    user_id = _require_user_id(user)
    passage_id = _require_payload_param(payload, "passage_id")
    question_id = _require_payload_param(payload, "question_id")
    selected_answer = _require_payload_param(payload, "selected_answer")

    question = _safe_execute("answer.lookup", get_question_by_id, question_id)
    if not question or question.get("passage_id") != passage_id:
        logger.warning("Comprehension answer invalid question_id or passage mismatch")
        raise HTTPException(status_code=404, detail="Question not found")

    return _safe_execute(
        "answer",
        submit_answer,
        user_id=user_id,
        passage_id=passage_id,
        question_id=question_id,
        selected_answer=selected_answer,
    )

from fastapi import APIRouter, Depends, HTTPException
from app.auth import get_current_user

from app.comprehension.service import (
    list_passages,
    start_passage,
    submit_answer
)

router = APIRouter(
    prefix="/practice/comprehension",
    tags=["comprehension"]
)


@router.get("/passages")
def get_passages(user=Depends(get_current_user)):
    return list_passages()


@router.get("/start")
def start(passage_id: int, user=Depends(get_current_user)):
    result = start_passage(passage_id)

    if not result:
        raise HTTPException(status_code=404, detail="Passage not found")

    return result


@router.post("/answer")
def answer(payload: dict, user=Depends(get_current_user)):
    return submit_answer(
        user_id=user["user_id"],
        passage_id=payload["passage_id"],
        question_id=payload["question_id"],
        selected_answer=payload["selected_answer"]
    )
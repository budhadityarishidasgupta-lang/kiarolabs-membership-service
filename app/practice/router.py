from fastapi import APIRouter, Depends
from app.auth import get_current_user
from app.practice.math_engine import get_math_question
from app.practice.spelling_engine import get_spelling_question
from app.practice.synonym_engine import get_synonym_question

router = APIRouter(prefix="/practice", tags=["practice"])

@router.get("/math/question")
def math_question(user=Depends(get_current_user)):
    return get_math_question(user["id"])


@router.get("/spelling/question")
def spelling_question(user=Depends(get_current_user)):
    return get_spelling_question(user["id"])


@router.get("/synonym/question")
def synonym_question(user=Depends(get_current_user)):
    return get_synonym_question(user["id"])

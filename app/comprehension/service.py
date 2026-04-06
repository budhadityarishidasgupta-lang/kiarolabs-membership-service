# app/comprehension/service.py

from app.comprehension.repository import (
    get_active_passages,
    get_passage_by_id,
    get_question_by_id,
    get_questions_for_passage,
    insert_attempt
)


# =========================
# PASSAGE LIST
# =========================

def list_passages():
    return get_active_passages()


# =========================
# START PASSAGE SESSION
# =========================

def start_passage(passage_id):
    passage = get_passage_by_id(passage_id)

    if not passage:
        return None

    questions = get_questions_for_passage(passage_id)

    return {
        "passage": passage,
        "questions": questions
    }


# =========================
# SUBMIT ANSWER
# =========================

def submit_answer(user_id, passage_id, question_id, selected_answer):
    question = get_question_by_id(question_id)

    if not question:
        return {"correct": False}

    # safety check: ensure question belongs to passage
    if question.get("passage_id") != passage_id:
        return {"correct": False}

    correct = (question["correct_answer"] == selected_answer)

    insert_attempt(
        user_id=user_id,
        passage_id=passage_id,
        question_id=question_id,
        selected_answer=selected_answer,
        correct=correct
    )

    return {
        "correct": correct
    }

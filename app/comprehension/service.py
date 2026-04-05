# app/comprehension/service.py

from app.comprehension.repository import (
    get_active_passages,
    get_passage_by_id,
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
    questions = get_questions_for_passage(passage_id)

    # find correct answer
    correct = False

    for q in questions:
        if q["question_id"] == question_id:
            correct = (q["correct_answer"] == selected_answer)
            break

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

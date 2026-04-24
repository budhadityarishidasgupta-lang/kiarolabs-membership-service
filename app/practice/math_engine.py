import uuid

from app.repositories.math_repository import (
    get_math_lessons_list,
    get_math_next_question,
    get_math_question_record,
    record_math_attempt,
)


def get_math_lessons():
    return get_math_lessons_list()


def get_math_question(lesson_id, user_id=None, session_id: str | None = None):
    item = get_math_next_question(user_id or 0, lesson_id)
    if not item:
        return {
            "status": "no_questions",
            "lesson_id": lesson_id
        }

    session_id = session_id or str(uuid.uuid4())

    return {
        "session_id": session_id,
        "lesson_id": lesson_id,
        "question_id": item["question_id"],
        "stem": item["stem"],
        "options": [
            item["option_a"],
            item["option_b"],
            item["option_c"],
            item["option_d"]
        ],
        "correct_option": item["correct_option"]
    }


def submit_math_answer(student_id, lesson_id, question_id, selected_option, session_id: str | None = None):
    question = get_math_question_record(question_id)

    if not question:
        return {"error": "Question not found"}

    options_map = {
        "A": question["option_a"],
        "B": question["option_b"],
        "C": question["option_c"],
        "D": question["option_d"],
        "E": question["option_e"],
    }

    normalized_selected = None

    if isinstance(selected_option, str):
        raw = selected_option.strip()

        # Case 1: frontend already sends A/B/C/D/E
        if raw in options_map:
            normalized_selected = raw
        else:
            # Case 2: frontend sends full option text
            for key, value in options_map.items():
                if value is not None and str(value).strip() == raw:
                    normalized_selected = key
                    break

    if normalized_selected is None:
        return {
            "error": "Invalid selected option"
        }

    is_correct = (normalized_selected == question["correct_option"])
    record_math_attempt(
        user_id=student_id,
        lesson_id=lesson_id,
        question_id=question_id,
        correct=is_correct,
        selected_option=normalized_selected,
        session_id=session_id,
    )

    return {
        "correct": is_correct,
        "correct_option": question["correct_option"],
        "lesson_id": lesson_id,
        "question_id": question_id,
        "session_id": session_id,
    }

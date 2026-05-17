import uuid

from app.repositories.math_repository import (
    get_math_lessons_list,
    get_math_next_question,
    get_math_question_record,
    record_math_attempt,
)


REVIEW_ENCOURAGEMENT_MESSAGE = "Let's practise this one again - you were close last time."
REVIEW_COOLDOWN_WINDOW = 4


def _build_session_state(*, is_review: bool, review_reason: str | None, question_position: int, cooldown_distance: int | None):
    return {
        "is_review": bool(is_review),
        "review_reason": review_reason,
        "question_position": max(int(question_position or 1), 1),
        "cooldown_distance": cooldown_distance,
    }


def _add_review_metadata(payload, review_reason):
    is_review = bool(review_reason)
    payload["encouragement_message"] = REVIEW_ENCOURAGEMENT_MESSAGE if is_review else None
    payload["review_reason"] = review_reason
    payload["is_review"] = is_review
    payload["session_state"] = _build_session_state(
        is_review=is_review,
        review_reason=review_reason,
        question_position=(payload.get("question_position") or 1),
        cooldown_distance=REVIEW_COOLDOWN_WINDOW if is_review else None,
    )
    payload.pop("question_position", None)
    return payload


def get_math_lessons():
    return get_math_lessons_list()


def _normalize_practice_session_id(session_id):
    if session_id is None or session_id == "":
        return uuid.uuid4().int % 2147483647

    try:
        return int(session_id)
    except (TypeError, ValueError):
        return uuid.uuid4().int % 2147483647


def get_math_question(lesson_id, user_id=None, session_id: str | None = None):
    item = get_math_next_question(user_id or 0, lesson_id)
    if not item:
        return _add_review_metadata({
            "status": "no_questions",
            "lesson_id": lesson_id
        }, None)

    session_id = _normalize_practice_session_id(session_id)
    review_reason = None
    recent_question_ids = item.get("_recent_question_ids") or []
    is_spaced_review_return = (
        item.get("_selection_strategy") in {"weak", "review"}
        and item.get("_has_prior_incorrect_attempt")
        and item["question_id"] not in recent_question_ids
    )
    if is_spaced_review_return:
        review_reason = "review_question"

    payload = {
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
        "correct_option": item["correct_option"],
        "question_position": (item.get("_attempt_count") or 0) + 1,
        "lesson_item_count": int(item.get("_lesson_item_count") or 0),
    }
    return _add_review_metadata(payload, review_reason)


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
    normalized_session_id = _normalize_practice_session_id(session_id)
    record_math_attempt(
        user_id=student_id,
        lesson_id=lesson_id,
        question_id=question_id,
        correct=is_correct,
        selected_option=normalized_selected,
        session_id=normalized_session_id,
    )

    return {
        "correct": is_correct,
        "correct_option": question["correct_option"],
        "lesson_id": lesson_id,
        "question_id": question_id,
        "session_id": normalized_session_id,
    }

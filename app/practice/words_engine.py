import random
from app.repositories.words_repository import (
    get_word_details,
    get_words_courses_tree,
    get_words_micro_challenge_data,
    get_words_next_item,
    record_words_attempt,
)


def clean_text(value):
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def mask_word(word: str, blanks_count: int = 2):
    """
    Replace internal letters with underscores while keeping first/last letters.
    """
    try:
        if not word or len(word) <= 3:
            return word

        chars = list(word)
        candidates = [
            i for i in range(1, len(chars) - 1)
            if chars[i].isalpha()
        ]

        if not candidates:
            return word

        blanks_count = min(blanks_count, len(candidates))
        hidden_positions = random.sample(candidates, blanks_count)

        for pos in hidden_positions:
            chars[pos] = "_"

        return "".join(chars)
    except Exception:
        return word


def get_words_courses():
    return get_words_courses_tree()


def get_words_question(lesson_id: int, user_id: int):
    item = get_words_next_item(user_id, lesson_id)
    if not item:
        return {
            "word_id": None,
            "masked_word": "",
            "hint": "",
            "example": "",
        }

    return {
        "word_id": item["word_id"],
        "masked_word": mask_word(item["word"]),
        "hint": clean_text(item["hint"]),
        "example": clean_text(item["example"]),
    }


def submit_words_answer(word_id: int, answer: str, user_id: int):
    details = get_word_details(word_id)
    if not details:
        return {
            "correct": False,
            "correct_answer": "",
            "example": "",
            "xp": 0,
        }

    correct_word = clean_text(details["word"])
    correct = answer.strip().lower() == correct_word.lower()

    record_words_attempt(
        user_id=user_id,
        lesson_id=0,
        word_id=word_id,
        correct=correct,
        response_ms=0,
    )

    return {
        "correct": correct,
        "correct_answer": correct_word,
        "example": clean_text(details["example"]),
        "xp": 5 if correct else 0,
    }


def build_words_micro_challenge(user_id: int, word_id: int):
    data = get_words_micro_challenge_data(word_id)
    if not data:
        return {"error": "Word not found"}

    word = data["word"]
    options = data["options"]
    correct_index = data["correct_index"]

    # Q1 — recognition
    q1 = {
        "type": "mcq",
        "question": f"Select the correct synonym of '{word}'",
        "options": options,
        "correct_index": correct_index
    }

    # Q2 — variation (shuffle options)
    shuffled = options.copy()
    random.shuffle(shuffled)
    new_correct_index = shuffled.index(options[correct_index])

    q2 = {
        "type": "mcq",
        "question": f"Which word is closest in meaning to '{word}'?",
        "options": shuffled,
        "correct_index": new_correct_index
    }

    return {
        "word_id": word_id,
        "word": word,
        "questions": [q1, q2],
        "total": 2
    }

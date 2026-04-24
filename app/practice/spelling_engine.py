import random
import uuid

from app.database import get_connection
from app.repositories.spelling_repository import (
    get_resume_word_id,
    get_weak_word_id,
    get_lesson_id_for_word,
    is_word_mastered,
    get_word_timing_stats,
    get_next_unmastered_word,
    get_spelling_next_item,
    get_spelling_micro_challenge_data,
    get_spelling_word_details,
    record_spelling_attempt,
)
from app.repositories.spelling_stats_repository import (
    get_spelling_weak_pattern,
    update_spelling_pattern_stats,
)


def clean_text(value):
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def mask_word(word: str, patterns: list = None, blanks_count: int = 2):
    """
    Replace internal letters with underscores.
    Keeps first and last letters visible.
    """
    try:
        if not word or len(word) <= 3:
            return word

        chars = list(word)

        if patterns:
            masked = False
            lower_word = word.lower()

            for pattern in patterns:
                if not pattern:
                    continue

                pattern_lower = str(pattern).lower()
                start = lower_word.find(pattern_lower)

                if start == -1:
                    continue

                end = start + len(pattern_lower)

                for pos in range(start, min(end, len(chars))):
                    if chars[pos].isalpha():
                        chars[pos] = "_"
                        masked = True

            if masked:
                return "".join(chars)

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


def extract_patterns(word: str):
    patterns = ["ph", "gh", "tion", "sion", "ough", "dge", "tch", "ck", "wr", "kn"]
    found = []

    for pattern in patterns:
        if pattern in word.lower():
            found.append(pattern)

    return found


def compute_priority_score(is_weak, is_mastered, is_slow):
    score = 0

    if is_weak:
        score += 100

    if not is_mastered:
        score += 50

    if is_slow:
        score += 20

    if score == 0:
        score = 10

    return score


def get_spelling_question(lesson_id: int, user_id: int, session_id: str | None = None):
    try:
        conn = get_connection()
        try:
            # STEP 10: Weak word prioritization
            weak_word_id = get_weak_word_id(
                user_id=user_id,
                lesson_id=lesson_id,
                conn=conn,
            )

            weak_word = None

            if weak_word_id:
                weak_word = get_spelling_word_details(
                    word_id=weak_word_id,
                    conn=conn,
                )

            resume_word_id = get_resume_word_id(
                user_id=user_id,
                lesson_id=lesson_id,
                conn=conn,
            )
            resume_word = None
            if resume_word_id:
                resume_word = get_spelling_word_details(
                    word_id=resume_word_id,
                    conn=conn,
                )

            next_unmastered_word_id = get_next_unmastered_word(
                user_id=user_id,
                lesson_id=lesson_id,
                conn=conn,
            )
            next_unmastered_word = None
            if next_unmastered_word_id:
                next_unmastered_word = get_spelling_word_details(
                    word_id=next_unmastered_word_id,
                    conn=conn,
                )

            candidates = []

            if weak_word:
                candidates.append(("weak", weak_word))

            if resume_word:
                candidates.append(("resume", resume_word))

            if next_unmastered_word:
                candidates.append(("next", next_unmastered_word))

            scored_candidates = []

            for label, candidate in candidates:
                word_id = candidate["word_id"] if "word_id" in candidate else candidate["id"]

                is_weak = label == "weak"
                is_mastered = is_word_mastered(
                    user_id=user_id,
                    lesson_id=lesson_id,
                    word_id=word_id,
                    conn=conn,
                )
                timing_stats = get_word_timing_stats(
                    user_id=user_id,
                    lesson_id=lesson_id,
                    word_id=word_id,
                    conn=conn,
                )
                is_slow = timing_stats["is_slow"]
                score = compute_priority_score(is_weak, is_mastered, is_slow)

                scored_candidates.append((score, candidate, label, timing_stats, is_mastered))

            if scored_candidates:
                scored_candidates.sort(key=lambda entry: entry[0], reverse=True)
                best_score, item, selected_strategy, selected_timing_stats, mastered = scored_candidates[0]
            else:
                item = get_spelling_next_item(user_id, lesson_id)
                selected_strategy = "fallback"
                best_score = 0
                selected_timing_stats = {
                    "attempt_count": 0,
                    "avg_time_ms": 0,
                    "is_slow": False,
                }
                mastered = False

            if not item:
                return {
                    "word_id": None,
                    "word_audio": "",
                    "masked_word": "",
                    "hint": "",
                    "example_sentence": "",
                    "weak_word_id": weak_word_id,
                    "resume_from_word_id": resume_word_id,
                    "next_unmastered_word_id": next_unmastered_word_id,
                    "resumed": False,
                    "resume_strategy": "last_correct_next",
                    "adaptive_strategy": "deterministic_priority_scoring",
                    "selection_strategy": selected_strategy,
                    "selection_score": best_score,
                }

            if selected_strategy == "fallback":
                mastered = is_word_mastered(
                    user_id=user_id,
                    lesson_id=lesson_id,
                    word_id=item["word_id"],
                    conn=conn,
                )
                selected_timing_stats = get_word_timing_stats(
                    user_id=user_id,
                    lesson_id=lesson_id,
                    word_id=item["word_id"],
                    conn=conn,
                )

            weak_pattern = get_spelling_weak_pattern(user_id)
            patterns = [weak_pattern] if weak_pattern else None
            question_id = str(uuid.uuid4())
            session_id = session_id or str(uuid.uuid4())

            return {
                "question_id": question_id,
                "session_id": session_id,
                "lesson_id": lesson_id,
                "word_id": item["word_id"],
                "word_audio": "",
                "masked_word": mask_word(item["word"], patterns, blanks_count=3),
                "hint": clean_text(item["hint"]),
                "example_sentence": clean_text(item["example_sentence"]),
                "weak_word_id": weak_word_id,
                "resume_from_word_id": resume_word_id,
                "next_unmastered_word_id": next_unmastered_word_id,
                "resumed": selected_strategy == "resume",
                "resume_strategy": "last_correct_next",
                "adaptive_strategy": "deterministic_priority_scoring",
                "selection_strategy": selected_strategy,
                "selection_score": best_score,
                "mastered": mastered,
                "timing": selected_timing_stats,
            }
        finally:
            conn.close()

    except Exception as e:
        print("SPELLING QUESTION ERROR:", str(e))
        return {
            "word_id": None,
            "word_audio": "",
            "masked_word": "",
            "hint": "",
            "example_sentence": "",
        }


def get_word_by_id(user_id: int, word_id: int):
    details = get_spelling_word_details(word_id)
    if not details:
        return {"error": "Word not found"}

    def mask_word_simple(word):
        return word[0] + "_" * (len(word) - 2) + word[-1] if len(word) > 2 else word

    return {
        "word_id": word_id,
        "masked_word": mask_word_simple(details["word"]),
        "hint": details["hint"] or "",
        "example_sentence": details["example_sentence"] or "",
    }


def build_micro_challenge(user_id: int, word_id: int):
    data = get_spelling_micro_challenge_data(word_id)
    if not data:
        return {"error": "Word not found"}

    word = data["word"]
    hint = data["hint"]
    example = data["example_sentence"]

    def mask_variation(source_word, level):
        if level == 1:
            return source_word[0] + "_" * (len(source_word) - 2) + source_word[-1]
        if level == 2:
            return "_" + source_word[1:-1] + "_"
        return source_word[0:2] + "_" * (len(source_word) - 3) + source_word[-1]

    questions = [
        {
            "attempt": 1,
            "masked_word": mask_variation(word, 1),
            "hint": hint or "",
            "example": example or "",
        },
        {
            "attempt": 2,
            "masked_word": mask_variation(word, 2),
            "hint": hint or "",
            "example": example or "",
        },
        {
            "attempt": 3,
            "masked_word": mask_variation(word, 3),
            "hint": hint or "",
            "example": example or "",
        },
    ]

    return {
        "word_id": word_id,
        "word": word,
        "questions": questions,
        "total": 3,
    }


def submit_spelling_answer(
    word_id: int,
    answer: str,
    user_id: int,
    response_ms: int = 0,
    session_id: str | None = None,
    question_id: str | None = None,
    lesson_id: int | None = None,
):
    try:
        details = get_spelling_word_details(word_id)
        if not details:
            return {
                "correct": False,
                "correct_word": "",
                "hint": "",
                "example_sentence": "",
            }

        clean_correct_word = clean_text(details["word"])
        clean_hint = clean_text(details["hint"])
        clean_example = clean_text(details["example_sentence"])

        correct = answer.strip().lower() == clean_correct_word.lower()
        pattern_hint = None

        if not correct:
            pattern = get_spelling_weak_pattern(user_id)
            if pattern and pattern in clean_correct_word.lower():
                pattern_hint = f"Focus on pattern '{pattern}'"

        resolved_lesson_id = lesson_id or get_lesson_id_for_word(word_id)

        record_spelling_attempt(
            user_id=user_id,
            lesson_id=resolved_lesson_id,
            word_id=word_id,
            submitted_text=answer,
            correct=correct,
            response_ms=response_ms,
            session_id=session_id,
            question_id=question_id,
        )

        update_spelling_pattern_stats(
            user_id,
            extract_patterns(clean_correct_word),
            correct,
        )

        return {
            "correct": correct,
            "correct_word": clean_correct_word,
            "hint": clean_text(pattern_hint) or clean_hint,
            "example_sentence": clean_example,
            "lesson_id": resolved_lesson_id,
            "question_id": question_id,
            "session_id": session_id,
        }

    except Exception as e:
        print("SPELLING SUBMIT ERROR:", str(e))
        raise

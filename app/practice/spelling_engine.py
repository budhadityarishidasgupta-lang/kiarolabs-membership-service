import random
import uuid
import json
import logging
from datetime import datetime, timezone, timedelta

from app.database import get_connection
from app.repositories.spelling_repository import (
    get_resume_word_id,
    get_weak_word_id,
    get_lesson_id_for_word,
    get_session_recent_word_ids,
    get_session_word_positions,
    get_latest_attempt_summary,
    has_prior_incorrect_attempt,
    is_word_eligible_for_review,
    is_word_mastered,
    get_word_timing_stats,
    get_next_unmastered_word,
    get_next_lesson_word_after,
    get_spelling_next_item,
    get_spelling_micro_challenge_data,
    get_spelling_word_details,
    record_spelling_attempt,
)
from app.repositories.spelling_stats_repository import (
    get_spelling_weak_pattern,
    update_spelling_pattern_stats,
)


logger = logging.getLogger(__name__)
REVIEW_ENCOURAGEMENT_MESSAGE = "Let's practise this one again - you were close last time."
SESSION_BOOTSTRAP_GAP = timedelta(minutes=15)
REVIEW_COOLDOWN_DISTANCE = 5
SESSION_RECENT_WINDOW = max(REVIEW_COOLDOWN_DISTANCE - 1, 1)


def _build_session_state(*, is_review: bool, review_reason: str | None, question_position: int, cooldown_distance: int | None):
    return {
        "is_review": bool(is_review),
        "review_reason": review_reason,
        "question_position": max(int(question_position or 1), 1),
        "cooldown_distance": cooldown_distance,
    }


def _add_review_metadata(payload, review_reason, *, question_position: int, cooldown_distance: int | None = None):
    is_review = bool(review_reason)
    payload["encouragement_message"] = REVIEW_ENCOURAGEMENT_MESSAGE if is_review else None
    payload["review_reason"] = review_reason
    payload["is_review"] = is_review
    payload["session_state"] = _build_session_state(
        is_review=is_review,
        review_reason=review_reason,
        question_position=question_position,
        cooldown_distance=cooldown_distance if is_review else None,
    )
    return payload


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


def _is_session_bootstrap(latest_attempt_summary) -> bool:
    if not latest_attempt_summary:
        return True

    created_at = latest_attempt_summary.get("created_at")
    if created_at is None:
        return True

    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)

    return datetime.now(timezone.utc) - created_at > SESSION_BOOTSTRAP_GAP


def _build_progression_candidate(
    user_id: int,
    lesson_id: int,
    conn,
    latest_attempt_summary,
    session_bootstrap: bool,
):
    resume_word_id = get_resume_word_id(
        user_id=user_id,
        lesson_id=lesson_id,
        conn=conn,
    )
    latest_attempted_word_id = latest_attempt_summary["word_id"] if latest_attempt_summary else None
    next_progression_word_id = None
    if latest_attempted_word_id:
        next_progression_word_id = get_next_lesson_word_after(
            lesson_id=lesson_id,
            current_word_id=latest_attempted_word_id,
            conn=conn,
        )
    next_unmastered_word_id = get_next_unmastered_word(
        user_id=user_id,
        lesson_id=lesson_id,
        conn=conn,
    )

    if session_bootstrap:
        progression_word_id = resume_word_id or next_unmastered_word_id
        selected_strategy = "resume" if resume_word_id else "next"
    else:
        progression_word_id = next_progression_word_id or next_unmastered_word_id or resume_word_id
        if next_progression_word_id:
            selected_strategy = "next"
        elif next_unmastered_word_id:
            selected_strategy = "next"
        else:
            selected_strategy = "resume"

    if not progression_word_id:
        return None, resume_word_id, next_unmastered_word_id

    progression_word = get_spelling_word_details(
        word_id=progression_word_id,
        conn=conn,
    )
    if not progression_word:
        return None, resume_word_id, next_unmastered_word_id

    timing_stats = get_word_timing_stats(
        user_id=user_id,
        lesson_id=lesson_id,
        word_id=progression_word_id,
        conn=conn,
    )
    mastered = is_word_mastered(
        user_id=user_id,
        lesson_id=lesson_id,
        word_id=progression_word_id,
        conn=conn,
    )
    return (
        {
            "item": progression_word,
            "selection_strategy": selected_strategy,
            "selection_score": 50 if selected_strategy == "resume" else 40,
            "timing": timing_stats,
            "mastered": mastered,
        },
        resume_word_id,
        next_unmastered_word_id,
    )


def _build_review_candidate(
    user_id: int,
    lesson_id: int,
    conn,
    session_recent_word_ids: list[int],
):
    weak_word_id = get_weak_word_id(
        user_id=user_id,
        lesson_id=lesson_id,
        conn=conn,
        exclude_word_ids=session_recent_word_ids,
    )
    if not weak_word_id:
        return None

    weak_word = get_spelling_word_details(
        word_id=weak_word_id,
        conn=conn,
    )
    if not weak_word:
        return None

    return {
        "item": weak_word,
        "selection_strategy": "review",
        "selection_score": 100,
        "timing": get_word_timing_stats(
            user_id=user_id,
            lesson_id=lesson_id,
            word_id=weak_word_id,
            conn=conn,
        ),
        "mastered": is_word_mastered(
            user_id=user_id,
            lesson_id=lesson_id,
            word_id=weak_word_id,
            conn=conn,
        ),
        "word_id": weak_word_id,
    }


def _log_cooldown_blocked(
    *,
    user_id: int,
    lesson_id: int,
    session_id: str | None,
    word_id: int | None,
    current_position: int,
    last_seen_position: int | None,
    cooldown_distance: int,
):
    logger.info(
        "[SPELLING_COOLDOWN_BLOCKED] %s",
        json.dumps(
            {
                "user_id": user_id,
                "lesson_id": lesson_id,
                "session_id": session_id,
                "word_id": word_id,
                "current_position": current_position,
                "last_seen_position": last_seen_position,
                "cooldown_distance": cooldown_distance,
            },
            sort_keys=True,
        ),
    )


def _should_schedule_review(
    review_candidate,
    progression_candidate,
    session_recent_word_ids: list[int],
    review_eligible: bool,
    current_question_position: int,
    session_bootstrap: bool,
) -> bool:
    if not review_candidate:
        return False

    if session_bootstrap and progression_candidate:
        return False

    if not review_eligible:
        return False

    if current_question_position <= REVIEW_COOLDOWN_DISTANCE:
        return False

    return len(session_recent_word_ids) >= SESSION_RECENT_WINDOW


def _get_lesson_attempt_count(user_id: int, lesson_id: int, conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM spelling_attempts
            WHERE user_id = %s
              AND lesson_id = %s
            """,
            (user_id, lesson_id),
        )
        row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def get_spelling_question(lesson_id: int, user_id: int, session_id: str | None = None):
    try:
        print("Lesson ID:", lesson_id)
        conn = get_connection()
        try:
            latest_attempt_summary = get_latest_attempt_summary(
                user_id=user_id,
                lesson_id=lesson_id,
                conn=conn,
            )
            session_bootstrap = _is_session_bootstrap(latest_attempt_summary)
            active_session_id = (
                session_id
                or (
                    latest_attempt_summary.get("session_id")
                    if latest_attempt_summary and not session_bootstrap
                    else None
                )
                or str(uuid.uuid4())
            )
            session_positions = get_session_word_positions(
                user_id=user_id,
                lesson_id=lesson_id,
                session_id=active_session_id,
                conn=conn,
            )
            current_question_position = int(session_positions["question_position"] or 1)
            session_recent_word_ids = get_session_recent_word_ids(
                user_id=user_id,
                lesson_id=lesson_id,
                session_id=active_session_id,
                conn=conn,
                limit=SESSION_RECENT_WINDOW,
            )
            recent_attempt_word_id_set = set(session_recent_word_ids)

            progression_candidate, resume_word_id, next_unmastered_word_id = _build_progression_candidate(
                user_id=user_id,
                lesson_id=lesson_id,
                conn=conn,
                latest_attempt_summary=latest_attempt_summary,
                session_bootstrap=session_bootstrap,
            )
            blocked_review_word_id = get_weak_word_id(
                user_id=user_id,
                lesson_id=lesson_id,
                conn=conn,
            )
            review_candidate = _build_review_candidate(
                user_id=user_id,
                lesson_id=lesson_id,
                conn=conn,
                session_recent_word_ids=session_recent_word_ids,
            )
            review_candidate_word_id = review_candidate["word_id"] if review_candidate else None
            review_eligible = bool(review_candidate_word_id) and is_word_eligible_for_review(
                user_id=user_id,
                lesson_id=lesson_id,
                session_id=active_session_id,
                word_id=review_candidate_word_id,
                conn=conn,
                cooldown_distance=REVIEW_COOLDOWN_DISTANCE,
                session_positions=session_positions,
            )

            weak_word_id = review_candidate["word_id"] if review_candidate else None

            blocked_review_eligible = bool(blocked_review_word_id) and is_word_eligible_for_review(
                user_id=user_id,
                lesson_id=lesson_id,
                session_id=active_session_id,
                word_id=blocked_review_word_id,
                conn=conn,
                cooldown_distance=REVIEW_COOLDOWN_DISTANCE,
                session_positions=session_positions,
            )
            if blocked_review_word_id and not blocked_review_eligible:
                _log_cooldown_blocked(
                    user_id=user_id,
                    lesson_id=lesson_id,
                    session_id=active_session_id,
                    word_id=blocked_review_word_id,
                    current_position=current_question_position,
                    last_seen_position=session_positions["last_seen_positions"].get(blocked_review_word_id),
                    cooldown_distance=REVIEW_COOLDOWN_DISTANCE,
                )

            selected_candidate = None
            selected_from_spaced_review = False
            if _should_schedule_review(
                review_candidate=review_candidate,
                progression_candidate=progression_candidate,
                session_recent_word_ids=session_recent_word_ids,
                review_eligible=review_eligible,
                current_question_position=current_question_position,
                session_bootstrap=session_bootstrap,
            ):
                selected_candidate = review_candidate
                selected_from_spaced_review = True
            elif progression_candidate:
                selected_candidate = progression_candidate

            if selected_candidate:
                item = selected_candidate["item"]
                selected_strategy = selected_candidate["selection_strategy"]
                best_score = selected_candidate["selection_score"]
                selected_timing_stats = selected_candidate["timing"]
                mastered = selected_candidate["mastered"]
            else:
                item = get_spelling_next_item(
                    user_id,
                    lesson_id,
                    recent_word_ids=session_recent_word_ids,
                    last_word_id=session_recent_word_ids[0] if session_recent_word_ids else None,
                )
                selected_strategy = item.get("_selection_strategy", "fallback") if item else "fallback"
                best_score = 0
                selected_timing_stats = {
                    "attempt_count": 0,
                    "avg_time_ms": 0,
                    "is_slow": False,
                }
                mastered = False

            if not item:
                print("Word Count:", 0)
                print("Sample Words:", [])
                return _add_review_metadata({
                    "word_id": None,
                    "word_audio": "",
                    "masked_word": "",
                    "hint": "",
                    "example_sentence": None,
                    "weak_word_id": weak_word_id,
                    "resume_from_word_id": resume_word_id,
                    "next_unmastered_word_id": next_unmastered_word_id,
                    "resumed": False,
                    "resume_strategy": "progression_resume",
                    "adaptive_strategy": "progression_with_spaced_review",
                    "selection_strategy": selected_strategy,
                    "selection_score": best_score,
                }, None, question_position=current_question_position)

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
            selected_word_id = item["word_id"]
            prior_incorrect_attempt = has_prior_incorrect_attempt(
                user_id=user_id,
                lesson_id=lesson_id,
                word_id=selected_word_id,
                conn=conn,
            )
            outside_cooldown = selected_word_id not in recent_attempt_word_id_set
            review_reason = None
            if prior_incorrect_attempt and outside_cooldown and selected_from_spaced_review:
                review_reason = "practice_review"

            if lesson_id == 870:
                sample_words = []
                try:
                    debug_cur = conn.cursor()
                    try:
                        debug_cur.execute(
                            """
                            SELECT DISTINCT w.word
                            FROM spelling_lesson_items li
                            JOIN spelling_lessons l
                              ON l.lesson_id = li.lesson_id
                            JOIN spelling_words w
                              ON w.word_id = li.word_id
                            WHERE li.lesson_id = %s
                              AND l.is_active = TRUE
                            ORDER BY w.word_id ASC
                            """,
                            (lesson_id,),
                        )
                        sample_words = [row[0] for row in debug_cur.fetchall()]
                    finally:
                        debug_cur.close()
                except Exception:
                    sample_words = []
                print("Word Count:", len(sample_words))
                print("Sample Words:", sample_words[:5])

            payload = {
                "question_id": question_id,
                "session_id": active_session_id,
                "lesson_id": lesson_id,
                "word_id": selected_word_id,
                "word_audio": "",
                "masked_word": mask_word(item["word"], patterns, blanks_count=3),
                "hint": clean_text(item["hint"]),
                "example_sentence": None,
                "weak_word_id": weak_word_id,
                "resume_from_word_id": resume_word_id,
                "next_unmastered_word_id": next_unmastered_word_id,
                "resumed": selected_strategy == "resume",
                "resume_strategy": "progression_resume",
                "adaptive_strategy": "progression_with_spaced_review",
                "selection_strategy": selected_strategy,
                "selection_score": best_score,
                "mastered": mastered,
                "timing": selected_timing_stats,
            }
            return _add_review_metadata(
                payload,
                review_reason,
                question_position=current_question_position,
                cooldown_distance=REVIEW_COOLDOWN_DISTANCE if review_reason else None,
            )
        finally:
            conn.close()

    except Exception as e:
        print("SPELLING QUESTION ERROR:", str(e))
        return {
            "word_id": None,
            "word_audio": "",
            "masked_word": "",
            "hint": "",
            "example_sentence": None,
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
        "example_sentence": None,
        "encouragement_message": None,
        "review_reason": None,
        "is_review": False,
        "session_state": _build_session_state(
            is_review=False,
            review_reason=None,
            question_position=1,
            cooldown_distance=None,
        ),
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

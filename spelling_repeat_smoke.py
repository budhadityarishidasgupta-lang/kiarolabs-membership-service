import argparse
import json
import sys

from app.database import get_connection
from app.practice.spelling_engine import get_spelling_question, submit_spelling_answer


def get_word_text(word_id: int) -> str:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT word
            FROM spelling_words
            WHERE word_id = %s
            LIMIT 1
            """,
            (word_id,),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"Word {word_id} not found")
        return row[0]
    finally:
        cur.close()
        conn.close()


def get_lesson_word_count(lesson_id: int) -> int:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(DISTINCT word_id)
            FROM spelling_lesson_items
            WHERE lesson_id = %s
            """,
            (lesson_id,),
        )
        row = cur.fetchone()
        return int(row[0] or 0)
    finally:
        cur.close()
        conn.close()


def get_latest_attempt(user_id: int, lesson_id: int, word_id: int):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                attempt_id,
                correct,
                wrong_letters_count,
                blanks_count,
                created_at
            FROM spelling_attempts
            WHERE user_id = %s
              AND lesson_id = %s
              AND word_id = %s
            ORDER BY created_at DESC, attempt_id DESC
            LIMIT 1
            """,
            (user_id, lesson_id, word_id),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "attempt_id": row[0],
            "correct": row[1],
            "wrong_letters_count": row[2],
            "blanks_count": row[3],
            "created_at": row[4].isoformat() if row[4] else None,
        }
    finally:
        cur.close()
        conn.close()


def make_wrong_answer(correct_word: str) -> str:
    wrong = "zzz" if correct_word.lower() != "zzz" else "qqq"
    if wrong.lower() == correct_word.lower():
        wrong = f"{correct_word}x"
    return wrong


def ensure(condition: bool, message: str):
    if not condition:
        raise AssertionError(message)


def run_smoke(user_id: int, lesson_id: int):
    ensure(
        get_lesson_word_count(lesson_id) >= 2,
        f"Lesson {lesson_id} must contain at least two words for the anti-repeat smoke check.",
    )

    question_one = get_spelling_question(lesson_id=lesson_id, user_id=user_id)
    ensure(question_one.get("word_id"), "First spelling question did not return a word_id.")

    word_one_id = question_one["word_id"]
    word_one_text = get_word_text(word_one_id)
    result_one = submit_spelling_answer(
        word_id=word_one_id,
        answer=make_wrong_answer(word_one_text),
        user_id=user_id,
        lesson_id=lesson_id,
    )
    ensure(result_one.get("correct") is False, "First submission must be incorrect for this smoke check.")

    latest_one = get_latest_attempt(user_id=user_id, lesson_id=lesson_id, word_id=word_one_id)
    ensure(latest_one is not None, "Wrong spelling attempt was not recorded.")
    ensure(latest_one["correct"] is False, "Recorded attempt was unexpectedly marked correct.")
    ensure(
        (latest_one["wrong_letters_count"] or 0) > 0,
        "wrong_letters_count was not populated for the wrong attempt.",
    )

    question_two = get_spelling_question(lesson_id=lesson_id, user_id=user_id)
    ensure(question_two.get("word_id"), "Second spelling question did not return a word_id.")
    word_two_id = question_two["word_id"]
    ensure(word_two_id != word_one_id, "Immediate repetition detected: second word matched the just-missed word.")

    word_two_text = get_word_text(word_two_id)
    result_two = submit_spelling_answer(
        word_id=word_two_id,
        answer=make_wrong_answer(word_two_text),
        user_id=user_id,
        lesson_id=lesson_id,
    )
    ensure(result_two.get("correct") is False, "Second submission must be incorrect for this smoke check.")

    question_three = get_spelling_question(lesson_id=lesson_id, user_id=user_id)
    ensure(question_three.get("word_id"), "Third spelling question did not return a word_id.")
    ensure(
        question_three["word_id"] == word_one_id,
        "Weak word did not return later as expected after an alternative was served.",
    )

    print(
        json.dumps(
            {
                "status": "ok",
                "user_id": user_id,
                "lesson_id": lesson_id,
                "first_word_id": word_one_id,
                "second_word_id": word_two_id,
                "third_word_id": question_three["word_id"],
                "first_attempt": latest_one,
            },
            indent=2,
        )
    )


def main():
    parser = argparse.ArgumentParser(description="Smoke check for SpellingSprint no-immediate-repeat behavior.")
    parser.add_argument("--user-id", type=int, required=True, help="User ID to run the smoke check against.")
    parser.add_argument("--lesson-id", type=int, required=True, help="Lesson ID with at least two spelling words.")
    args = parser.parse_args()

    try:
        run_smoke(user_id=args.user_id, lesson_id=args.lesson_id)
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()

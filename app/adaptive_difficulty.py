"""
Adaptive difficulty helper.

Reads from each app's existing attempts table to compute a mastery level,
then returns the target difficulty band for the next question.

Mastery levels:
  - beginner  : < 10 attempts OR accuracy < 50%
  - developing: 10+ attempts, 50–74% accuracy
  - mastered  : 10+ attempts, >= 75% accuracy

Difficulty mapping:
  - beginner   -> prefer 'easy'
  - developing -> prefer 'medium'
  - mastered   -> prefer 'hard'
"""

from __future__ import annotations

MASTERY_MIN_ATTEMPTS = 10
MASTERY_THRESHOLD = 75.0   # percent
DEVELOPING_THRESHOLD = 50.0


def _mastery_level(attempts: int, accuracy_pct: float) -> str:
    if attempts < MASTERY_MIN_ATTEMPTS:
        return "beginner"
    if accuracy_pct >= MASTERY_THRESHOLD:
        return "mastered"
    if accuracy_pct >= DEVELOPING_THRESHOLD:
        return "developing"
    return "beginner"


def target_difficulty(mastery: str) -> list[str]:
    """Return ordered list of preferred difficulty values for a mastery level."""
    if mastery == "mastered":
        return ["hard", "medium", "easy"]
    if mastery == "developing":
        return ["medium", "hard", "easy"]
    return ["easy", "medium", "hard"]


def get_student_mastery_math(cur, user_id: int, lesson_id: int) -> str:
    cur.execute(
        """
        SELECT COUNT(*) AS attempts,
               COALESCE(AVG(CASE WHEN is_correct THEN 100.0 ELSE 0.0 END), 0) AS accuracy
        FROM math_attempts
        WHERE student_id = %s AND lesson_id = %s
        """,
        (user_id, lesson_id),
    )
    row = cur.fetchone()
    attempts, accuracy = (int(row[0] or 0), float(row[1] or 0)) if row else (0, 0.0)
    return _mastery_level(attempts, accuracy)


def get_student_mastery_nvr(cur, user_id: int, lesson_id: int) -> str:
    cur.execute(
        """
        SELECT COUNT(*) AS attempts,
               COALESCE(AVG(CASE WHEN is_correct THEN 100.0 ELSE 0.0 END), 0) AS accuracy
        FROM nvr_attempts
        WHERE user_id = %s AND lesson_id = %s
        """,
        (user_id, lesson_id),
    )
    row = cur.fetchone()
    attempts, accuracy = (int(row[0] or 0), float(row[1] or 0)) if row else (0, 0.0)
    return _mastery_level(attempts, accuracy)


def get_student_mastery_grammar(cur, user_id: int, lesson_id: int) -> str:
    cur.execute(
        """
        SELECT COUNT(*) AS attempts,
               COALESCE(AVG(CASE WHEN is_correct THEN 100.0 ELSE 0.0 END), 0) AS accuracy
        FROM grammar_attempts
        WHERE user_id = %s AND lesson_id = %s
        """,
        (user_id, lesson_id),
    )
    row = cur.fetchone()
    attempts, accuracy = (int(row[0] or 0), float(row[1] or 0)) if row else (0, 0.0)
    return _mastery_level(attempts, accuracy)


def get_student_mastery_words(cur, user_id: int, lesson_id: int) -> str:
    cur.execute(
        """
        SELECT COUNT(*) AS attempts,
               COALESCE(AVG(CASE WHEN is_correct THEN 100.0 ELSE 0.0 END), 0) AS accuracy
        FROM words_attempts
        WHERE user_id = %s AND lesson_id = %s
        """,
        (user_id, lesson_id),
    )
    row = cur.fetchone()
    attempts, accuracy = (int(row[0] or 0), float(row[1] or 0)) if row else (0, 0.0)
    return _mastery_level(attempts, accuracy)


def get_student_mastery_spelling(cur, user_id: int, lesson_id: int) -> str:
    cur.execute(
        """
        SELECT COUNT(*) AS attempts,
               COALESCE(AVG(CASE WHEN correct THEN 100.0 ELSE 0.0 END), 0) AS accuracy
        FROM spelling_attempts
        WHERE user_id = %s AND lesson_id = %s
        """,
        (user_id, lesson_id),
    )
    row = cur.fetchone()
    attempts, accuracy = (int(row[0] or 0), float(row[1] or 0)) if row else (0, 0.0)
    return _mastery_level(attempts, accuracy)


def get_student_mastery_comprehension(cur, user_id: int, passage_id: int) -> str:
    cur.execute(
        """
        SELECT COUNT(*) AS attempts,
               COALESCE(AVG(CASE WHEN correct THEN 100.0 ELSE 0.0 END), 0) AS accuracy
        FROM comprehension_attempts
        WHERE user_id = %s AND passage_id = %s
        """,
        (user_id, passage_id),
    )
    row = cur.fetchone()
    attempts, accuracy = (int(row[0] or 0), float(row[1] or 0)) if row else (0, 0.0)
    return _mastery_level(attempts, accuracy)


def filter_by_difficulty(items: list[dict], preferred_difficulties: list[str], difficulty_key: str = "difficulty") -> list[dict]:
    """
    Return items filtered to the most preferred available difficulty.
    Falls back to next difficulty if none found, then returns all items.
    """
    for diff in preferred_difficulties:
        filtered = [item for item in items if str(item.get(difficulty_key) or "").lower() == diff.lower()]
        if filtered:
            return filtered
    return items


def get_student_mastery_synonym(cur, user_id: int, lesson_id: int) -> str:
    """Mastery for synonym/WordSprint — uses synonym_attempts or words_attempts."""
    # Determine which table and correct column exist
    for table, col in (("synonym_attempts", "is_correct"), ("synonym_attempts", "correct"),
                       ("words_attempts", "is_correct"), ("words_attempts", "correct")):
        try:
            cur.execute(
                f"""
                SELECT COUNT(*) AS attempts,
                       COALESCE(AVG(CASE WHEN {col} THEN 100.0 ELSE 0.0 END), 0) AS accuracy
                FROM {table}
                WHERE user_id = %s AND lesson_id = %s
                """,
                (user_id, lesson_id),
            )
            row = cur.fetchone()
            attempts, accuracy = (int(row[0] or 0), float(row[1] or 0)) if row else (0, 0.0)
            return _mastery_level(attempts, accuracy)
        except Exception:
            continue
    return "beginner"


def get_synonym_mastery_difficulty(user_id: int, lesson_id: int) -> list[int] | None:
    """
    Returns a list of preferred difficulty integers (1=easy, 2=medium, 3=hard)
    for the synonym engine based on student mastery.
    Returns None if mastery cannot be determined (falls back to no filter).
    """
    from app.database import get_connection
    conn = get_connection()
    cur = conn.cursor()
    try:
        mastery = get_student_mastery_synonym(cur, user_id, lesson_id)
        mapping = {"beginner": [1, 2], "developing": [2, 3], "mastered": [3, 2]}
        return mapping.get(mastery)
    except Exception:
        return None
    finally:
        cur.close()
        conn.close()

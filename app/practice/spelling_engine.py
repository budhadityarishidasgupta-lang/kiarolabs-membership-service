import random
from app.database import get_connection


def mask_word(word: str, blanks_count: int = 2):
    """
    Replace internal letters with underscores.
    Keeps first and last letters visible.
    """

    if not word or len(word) <= 3:
        return word

    chars = list(word)

    candidates = [i for i in range(1, len(chars) - 1) if chars[i].isalpha()]

    if not candidates:
        return word

    blanks_count = min(blanks_count, len(candidates))

    hidden_positions = random.sample(candidates, blanks_count)

    for pos in hidden_positions:
        chars[pos] = "_"

    return "".join(chars)


def get_spelling_question(lesson_id: int, user_id: int):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            w.word_id,
            w.word,
            COALESCE(lw.hint, '') AS hint,
            COALESCE(lw.example_sentence, '') AS example_sentence
        FROM spelling_words w
        JOIN spelling_lesson_words lw
            ON w.word_id = lw.word_id
        WHERE lw.lesson_id = %s
        ORDER BY RANDOM()
        LIMIT 1
        """,
        (lesson_id,),
    )

    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        return None

    word_id, word, hint, example_sentence = row

    masked_word = mask_word(word, 2)

    return {
        "word_id": word_id,
        "word_audio": word,
        "masked_word": masked_word,
        "hint": hint,
        "example_sentence": example_sentence,
    }


def submit_spelling_answer(word_id: int, answer: str, user_id: int):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            w.word,
            COALESCE(lw.hint, '') AS hint,
            COALESCE(lw.example_sentence, '') AS example_sentence
        FROM spelling_words w
        LEFT JOIN spelling_lesson_words lw
            ON w.word_id = lw.word_id
        WHERE w.word_id = %s
        LIMIT 1
        """,
        (word_id,),
    )

    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return {
            "correct": False,
            "correct_word": "",
            "hint": "",
            "example_sentence": "",
        }

    correct_word, hint, example_sentence = row

    correct = answer.strip().lower() == correct_word.strip().lower()

    cur.execute(
        """
        INSERT INTO spelling_attempts
        (user_id, word_id, answer, correct)
        VALUES (%s, %s, %s, %s)
        """,
        (user_id, word_id, answer, correct),
    )

    conn.commit()

    cur.close()
    conn.close()

    return {
        "correct": correct,
        "correct_word": correct_word,
        "hint": hint,
        "example_sentence": example_sentence,
    }

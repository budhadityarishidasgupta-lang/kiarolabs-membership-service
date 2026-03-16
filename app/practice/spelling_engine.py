import random
from app.database import get_connection


# ---------------------------------------------------------
# Word masking helper
# ---------------------------------------------------------
def mask_word(word: str, blanks_count: int = 2):
    """
    Replace internal letters with underscores.
    Keeps first and last letters visible.
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
        # never crash masking
        return word


# ---------------------------------------------------------
# Get spelling question
# ---------------------------------------------------------
def get_spelling_question(lesson_id: int, user_id: int):

    conn = None
    cur = None

    try:

        conn = get_connection()
        cur = conn.cursor()

        # Main query
        cur.execute(
            """
            SELECT
                w.word_id,
                w.word,
                COALESCE(w.hint,'') AS hint,
                COALESCE(w.example_sentence,'') AS example_sentence
            FROM spelling_lesson_words lw
            JOIN spelling_words w
                ON lw.word_id = w.word_id
            WHERE lw.lesson_id = %s
            ORDER BY RANDOM()
            LIMIT 1
            """,
            (lesson_id,),
        )

        row = cur.fetchone()

        # Fallback if lesson mapping missing
        if not row:

            cur.execute(
                """
                SELECT word_id, word
                FROM spelling_words
                ORDER BY RANDOM()
                LIMIT 1
                """
            )

            row = cur.fetchone()

            if not row:
                return {
                    "word_id": None,
                    "word_audio": "",
                    "masked_word": "",
                    "hint": "",
                    "example_sentence": "",
                }

            word_id, word = row
            hint = ""
            example_sentence = ""

        else:
            word_id, word, hint, example_sentence = row

        # difficulty scaling
        word_len = len(word)

        if word_len <= 5:
            blanks = 1
        elif word_len <= 8:
            blanks = 2
        else:
            blanks = 3

        masked_word = mask_word(word, blanks)

        return {
            "word_id": word_id,
            "word_audio": word,
            "masked_word": masked_word,
            "hint": hint,
            "example_sentence": example_sentence,
        }

    except Exception as e:

        print("SPELLING QUESTION ERROR:", str(e))

        return {
            "word_id": None,
            "word_audio": "",
            "masked_word": "",
            "hint": "",
            "example_sentence": "",
        }

    finally:

        if cur:
            cur.close()

        if conn:
            conn.close()


# ---------------------------------------------------------
# Submit spelling answer
# ---------------------------------------------------------
def submit_spelling_answer(word_id: int, answer: str, user_id: int):

    conn = None
    cur = None

    try:

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
            return {
                "correct": False,
                "correct_word": "",
                "hint": "",
                "example_sentence": "",
            }

        correct_word, hint, example_sentence = row

        # normalize spelling comparison
        correct = answer.strip().lower() == correct_word.strip().lower()

        # record attempt
        cur.execute(
            """
            INSERT INTO spelling_attempts
            (user_id, word_id, answer, correct)
            VALUES (%s, %s, %s, %s)
            """,
            (user_id, word_id, answer, correct),
        )

        conn.commit()

        return {
            "correct": bool(correct),
            "correct_word": correct_word,
            "hint": hint,
            "example_sentence": example_sentence,
        }

    except Exception as e:

        print("SPELLING SUBMIT ERROR:", str(e))

        return {
            "correct": False,
            "correct_word": "",
            "hint": "",
            "example_sentence": "",
        }

    finally:

        if cur:
            cur.close()

        if conn:
            conn.close()

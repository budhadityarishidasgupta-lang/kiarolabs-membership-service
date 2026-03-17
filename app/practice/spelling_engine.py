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

        # Query: unseen words first
        cur.execute(
            """
            SELECT
                w.word_id,
                w.word,
                COALESCE(w.hint, '') AS hint,
                COALESCE(w.example_sentence, '') AS example_sentence
            FROM spelling_lesson_words lw
            JOIN spelling_words w
                ON lw.word_id = w.word_id
            LEFT JOIN spelling_attempts sa
                ON sa.word_id = w.word_id
                AND sa.user_id = %s
            WHERE lw.lesson_id = %s
            AND sa.word_id IS NULL
            ORDER BY w.word_id
            LIMIT 1
            """,
            (user_id, lesson_id),
        )

        row = cur.fetchone()

        # Fallback: random within lesson
        if not row:
            cur.execute(
                """
                SELECT
                    w.word_id,
                    w.word,
                    COALESCE(w.hint, '') AS hint,
                    COALESCE(w.example_sentence, '') AS example_sentence
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

        if not row:
            return {
                "word_id": None,
                "word_audio": "",
                "masked_word": "",
                "hint": "",
                "example_sentence": "",
            }

        word_id, word, hint, example_sentence = row

        masked_word = mask_word(word, 2)

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
                word,
                COALESCE(hint, '') AS hint,
                COALESCE(example_sentence, '') AS example_sentence
            FROM spelling_words
            WHERE word_id = %s
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

        # validate platform user exists
        cur.execute(
            """
            SELECT user_id
            FROM users
            WHERE user_id = %s
            """,
            (user_id,),
        )

        user_row = cur.fetchone()

        if not user_row:
            raise Exception("Invalid user_id")

        # record attempt
        cur.execute(
            """
            INSERT INTO spelling_attempts
            (user_id, word_id, correct, time_taken, blanks_count, wrong_letters_count, course_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (user_id, word_id, correct, 0, 0, 0, 0),
        )

        conn.commit()

        return {
            "correct": correct,
            "correct_word": correct_word,
            "hint": hint,
            "example_sentence": example_sentence,
        }

    except Exception as e:

        print("SPELLING SUBMIT ERROR:", str(e))
        raise

    finally:

        if cur:
            cur.close()

        if conn:
            conn.close()

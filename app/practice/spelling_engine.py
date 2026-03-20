import random
from app.database import get_connection


def clean_text(value):
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


# ---------------------------------------------------------
# Word masking helper
# ---------------------------------------------------------
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
        # never crash masking
        return word


# ---------------------------------------------------------
# Pattern extraction helper
# ---------------------------------------------------------
def extract_patterns(word: str):
    patterns = ["ph", "gh", "tion", "sion", "ough", "dge", "tch", "ck", "wr", "kn"]
    found = []

    for p in patterns:
        if p in word.lower():
            found.append(p)

    return found


# ---------------------------------------------------------
# Get spelling question
# ---------------------------------------------------------
def get_spelling_question(lesson_id: int, user_id: int):

    conn = None
    cur = None

    try:

        conn = get_connection()
        cur = conn.cursor()

        # STEP 1: unseen words first
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
            LEFT JOIN spelling_word_stats s
                ON s.word_id = w.word_id
                AND s.user_id = %s
            WHERE lw.lesson_id = %s
            AND s.word_id IS NULL
            LIMIT 1
            """,
            (user_id, lesson_id),
        )

        row = cur.fetchone()

        # STEP 2: weak words (low accuracy)
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
                JOIN spelling_word_stats s
                    ON s.word_id = w.word_id
                    AND s.user_id = %s
                WHERE lw.lesson_id = %s
                AND s.accuracy < 0.5
                ORDER BY s.accuracy ASC
                LIMIT 1
                """,
                (user_id, lesson_id),
            )
            row = cur.fetchone()

        # STEP 3: recently wrong (never corrected yet)
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
                JOIN spelling_word_stats s
                    ON s.word_id = w.word_id
                    AND s.user_id = %s
                WHERE lw.lesson_id = %s
                AND s.last_correct_at IS NULL
                ORDER BY s.last_attempt_at DESC
                LIMIT 1
                """,
                (user_id, lesson_id),
            )
            row = cur.fetchone()

        # STEP 4: stale words (oldest attempted first)
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
                JOIN spelling_word_stats s
                    ON s.word_id = w.word_id
                    AND s.user_id = %s
                WHERE lw.lesson_id = %s
                ORDER BY s.last_attempt_at ASC
                LIMIT 1
                """,
                (user_id, lesson_id),
            )
            row = cur.fetchone()

        # STEP 5 fallback: random within lesson
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
        clean_hint = clean_text(hint)
        clean_example = clean_text(example_sentence)

        weak_pattern = None
        try:
            cur.execute(
                """
                SELECT pattern
                FROM spelling_pattern_stats
                WHERE user_id = %s
                ORDER BY accuracy ASC
                LIMIT 1
                """,
                (user_id,),
            )
            pattern_row = cur.fetchone()
            if pattern_row:
                weak_pattern = pattern_row[0]
        except Exception:
            weak_pattern = None

        patterns = [weak_pattern] if weak_pattern else None
        masked_word = mask_word(word, patterns)

        return {
            "word_id": word_id,
            "word_audio": word,
            "masked_word": masked_word,
            "hint": clean_hint,
            "example_sentence": clean_example,
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


def get_word_by_id(user_id: int, word_id: int):
    from app.database import get_connection

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT word, hint, example_sentence
        FROM spelling_words
        WHERE id = %s
    """, (word_id,))

    row = cur.fetchone()

    if not row:
        return {"error": "Word not found"}

    word, hint, example = row

    def mask_word_simple(w):
        return w[0] + "_"*(len(w)-2) + w[-1] if len(w) > 2 else w

    return {
        "word_id": word_id,
        "masked_word": mask_word_simple(word),
        "hint": hint or "",
        "example_sentence": example or ""
    }


def build_micro_challenge(user_id: int, word_id: int):
    from app.database import get_connection

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT word, hint, example_sentence
        FROM spelling_words
        WHERE id = %s
    """, (word_id,))

    row = cur.fetchone()

    if not row:
        return {"error": "Word not found"}

    word, hint, example = row

    def mask_variation(word, level):
        if level == 1:
            return word[0] + "_"*(len(word)-2) + word[-1]
        elif level == 2:
            return "_" + word[1:-1] + "_"
        else:
            return word[0:2] + "_"*(len(word)-3) + word[-1]

    questions = [
        {
            "attempt": 1,
            "masked_word": mask_variation(word, 1),
            "hint": hint or "",
            "example": example or ""
        },
        {
            "attempt": 2,
            "masked_word": mask_variation(word, 2),
            "hint": hint or "",
            "example": example or ""
        },
        {
            "attempt": 3,
            "masked_word": mask_variation(word, 3),
            "hint": hint or "",
            "example": example or ""
        }
    ]

    return {
        "word_id": word_id,
        "word": word,
        "questions": questions,
        "total": 3
    }


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
        clean_correct_word = clean_text(correct_word)
        clean_hint = clean_text(hint)
        clean_example = clean_text(example_sentence)

        # normalize spelling comparison
        correct = answer.strip().lower() == clean_correct_word.lower()
        pattern_hint = None

        if not correct:
            cur.execute(
                """
                SELECT pattern
                FROM spelling_pattern_stats
                WHERE user_id = %s
                ORDER BY accuracy ASC
                LIMIT 1
                """,
                (user_id,),
            )

            row = cur.fetchone()

            if row:
                pattern = row[0]

                if pattern in clean_correct_word.lower():
                    pattern_hint = f"Focus on pattern '{pattern}'"

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

        # ---- UPDATE STATS TABLE ----
        correct_count = 1 if correct else 0
        wrong_count = 0 if correct else 1

        cur.execute(
            """
            INSERT INTO spelling_word_stats (
                user_id,
                word_id,
                attempts_count,
                correct_count,
                wrong_count,
                last_attempt_at,
                last_correct_at,
                accuracy
            )
            VALUES (
                %s,
                %s,
                1,
                %s,
                %s,
                NOW(),
                CASE WHEN %s = 1 THEN NOW() ELSE NULL END,
                %s
            )
            ON CONFLICT (user_id, word_id)
            DO UPDATE SET
                attempts_count = spelling_word_stats.attempts_count + 1,
                correct_count = spelling_word_stats.correct_count + EXCLUDED.correct_count,
                wrong_count = spelling_word_stats.wrong_count + EXCLUDED.wrong_count,
                last_attempt_at = NOW(),
                last_correct_at = CASE
                    WHEN EXCLUDED.correct_count = 1 THEN NOW()
                    ELSE spelling_word_stats.last_correct_at
                END,
                accuracy = (
                    (spelling_word_stats.correct_count + EXCLUDED.correct_count)::float /
                    (spelling_word_stats.attempts_count + 1)
                )
            """,
            (
                user_id,
                word_id,
                correct_count,
                wrong_count,
                correct_count,
                1.0 if correct else 0.0,
            ),
        )

        patterns = extract_patterns(clean_correct_word)

        for p in patterns:

            correct_count = 1 if correct else 0
            wrong_count = 0 if correct else 1

            cur.execute(
                """
                INSERT INTO spelling_pattern_stats (
                    user_id,
                    pattern,
                    attempts_count,
                    correct_count,
                    wrong_count,
                    accuracy,
                    last_attempt_at
                )
                VALUES (%s, %s, 1, %s, %s, %s, NOW())
                ON CONFLICT (user_id, pattern)
                DO UPDATE SET
                    attempts_count = spelling_pattern_stats.attempts_count + 1,
                    correct_count = spelling_pattern_stats.correct_count + EXCLUDED.correct_count,
                    wrong_count = spelling_pattern_stats.wrong_count + EXCLUDED.wrong_count,
                    last_attempt_at = NOW(),
                    accuracy = (
                        (spelling_pattern_stats.correct_count + EXCLUDED.correct_count)::float /
                        (spelling_pattern_stats.attempts_count + 1)
                    )
                """,
                (
                    user_id,
                    p,
                    correct_count,
                    wrong_count,
                    1.0 if correct else 0.0,
                ),
            )

        conn.commit()

        return {
            "correct": correct,
            "correct_word": clean_correct_word,
            "hint": clean_text(pattern_hint) or clean_hint,
            "example_sentence": clean_example,
        }

    except Exception as e:

        print("SPELLING SUBMIT ERROR:", str(e))
        raise

    finally:

        if cur:
            cur.close()

        if conn:
            conn.close()

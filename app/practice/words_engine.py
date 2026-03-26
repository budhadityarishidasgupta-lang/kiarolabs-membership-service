import random
from app.database import get_connection


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
    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                c.id AS course_id,
                c.name AS course_name,
                l.id AS lesson_id,
                l.name AS lesson_name
            FROM words_courses c
            JOIN words_lessons l
                ON l.course_id = c.id
            ORDER BY c.id, l.id;
            """
        )

        rows = cur.fetchall()

        courses = {}

        for course_id, course_name, lesson_id, lesson_name in rows:
            if course_id not in courses:
                courses[course_id] = {
                    "course_id": course_id,
                    "course_name": course_name,
                    "lessons": [],
                }

            courses[course_id]["lessons"].append(
                {
                    "lesson_id": lesson_id,
                    "lesson_name": lesson_name,
                }
            )

        return list(courses.values())

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def get_words_question(lesson_id: int, user_id: int):
    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor()

        # STEP 1: unseen words first
        cur.execute(
            """
            SELECT
                w.id,
                w.word,
                COALESCE(w.hint, '') AS hint,
                COALESCE(w.example, '') AS example
            FROM words_lesson_words lw
            JOIN words_words w
                ON lw.word_id = w.id
            LEFT JOIN words_word_stats s
                ON s.word_id = w.id
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
                    w.id,
                    w.word,
                    COALESCE(w.hint, '') AS hint,
                    COALESCE(w.example, '') AS example
                FROM words_lesson_words lw
                JOIN words_words w
                    ON lw.word_id = w.id
                JOIN words_word_stats s
                    ON s.word_id = w.id
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
                    w.id,
                    w.word,
                    COALESCE(w.hint, '') AS hint,
                    COALESCE(w.example, '') AS example
                FROM words_lesson_words lw
                JOIN words_words w
                    ON lw.word_id = w.id
                JOIN words_word_stats s
                    ON s.word_id = w.id
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
                    w.id,
                    w.word,
                    COALESCE(w.hint, '') AS hint,
                    COALESCE(w.example, '') AS example
                FROM words_lesson_words lw
                JOIN words_words w
                    ON lw.word_id = w.id
                JOIN words_word_stats s
                    ON s.word_id = w.id
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
                    w.id,
                    w.word,
                    COALESCE(w.hint, '') AS hint,
                    COALESCE(w.example, '') AS example
                FROM words_lesson_words lw
                JOIN words_words w
                    ON lw.word_id = w.id
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
                "masked_word": "",
                "hint": "",
                "example": "",
            }

        word_id, word, hint, example = row

        return {
            "word_id": word_id,
            "masked_word": mask_word(word),
            "hint": clean_text(hint),
            "example": clean_text(example),
        }

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def submit_words_answer(word_id: int, answer: str, user_id: int):
    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT word
            FROM words_words
            WHERE word_id = %s
            LIMIT 1
            """,
            (word_id,),
        )

        row = cur.fetchone()

        if not row:
            return {
                "correct": False,
                "correct_answer": "",
                "xp": 0,
            }

        correct_word = clean_text(row[0])
        correct = answer.strip().lower() == correct_word.lower()

        # append-only attempts
        cur.execute(
            """
            INSERT INTO words_attempts
            (user_id, word_id, correct, time_taken, blanks_count, wrong_letters_count, course_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (user_id, word_id, correct, 0, 0, 0, 0),
        )

        # accuracy computation mirrors spelling module
        correct_count = 1 if correct else 0
        wrong_count = 0 if correct else 1

        cur.execute(
            """
            INSERT INTO words_word_stats (
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
                attempts_count = words_word_stats.attempts_count + 1,
                correct_count = words_word_stats.correct_count + EXCLUDED.correct_count,
                wrong_count = words_word_stats.wrong_count + EXCLUDED.wrong_count,
                last_attempt_at = NOW(),
                last_correct_at = CASE
                    WHEN EXCLUDED.correct_count = 1 THEN NOW()
                    ELSE words_word_stats.last_correct_at
                END,
                accuracy = (
                    (words_word_stats.correct_count + EXCLUDED.correct_count)::float /
                    (words_word_stats.attempts_count + 1)
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

        conn.commit()

        return {
            "correct": correct,
            "correct_answer": correct_word,
            "xp": 5 if correct else 0,
        }

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def build_words_micro_challenge(user_id: int, word_id: int):
    from app.database import get_connection
    import random

    conn = get_connection()
    cur = conn.cursor()

    # Get word + synonyms
    cur.execute("""
        SELECT word, option_1, option_2, option_3, option_4, correct_option_index
        FROM words
        WHERE id = %s
    """, (word_id,))

    row = cur.fetchone()

    if not row:
        return {"error": "Word not found"}

    word, o1, o2, o3, o4, correct_index = row

    options = [o1, o2, o3, o4]

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

from app.database import get_connection


# --------------------------------------------------
# Get Spelling Question
# --------------------------------------------------

def get_spelling_question(user_id: int, lesson_id: int):
    """
    Returns one spelling question for a given lesson.
    Router passes user_id and lesson_id.

    The question is selected randomly from the lesson's word pool.
    """

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT w.word_id, w.word
        FROM spelling_words w
        JOIN spelling_lesson_words lw
            ON w.word_id = lw.word_id
        WHERE lw.lesson_id = %s
        ORDER BY RANDOM()
        LIMIT 1
    """, (lesson_id,))

    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        return {
            "error": "No question available for this lesson yet"
        }

    word_id, word = row

    return {
        "question_type": "spelling",
        "word_id": word_id,
        "word_audio": word,
        "instructions": "Type the correct spelling"
    }


# --------------------------------------------------
# Submit Spelling Answer
# --------------------------------------------------

def submit_spelling_answer(user_id: int, word_id: int, answer: str):

    conn = get_connection()
    cur = conn.cursor()

    try:

        # Get correct word
        cur.execute(
            "SELECT word FROM spelling_words WHERE word_id = %s",
            (word_id,)
        )

        row = cur.fetchone()

        if not row:
            return {"error": "Word not found"}

        correct_word = row[0]

        correct = answer.strip().lower() == correct_word.lower()

        # Insert attempt using your actual table structure
        cur.execute("""
            INSERT INTO spelling_attempts
            (user_id, word_id, correct, time_taken, blanks_count, wrong_letters_count, course_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            user_id,
            word_id,
            correct,
            0,
            0,
            0,
            0
        ))

        conn.commit()

    finally:
        cur.close()
        conn.close()

    return {
        "correct": correct,
        "correct_word": correct_word
    }

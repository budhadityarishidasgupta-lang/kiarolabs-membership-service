from app.database import get_connection


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

from app.database import get_connection


def get_spelling_question(user_id):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT word
        FROM spelling_words
        ORDER BY RANDOM()
        LIMIT 1
    """)

    row = cur.fetchone()

    return {
        "question_type": "spelling",
        "word_audio": row[0],
        "instructions": "Type the correct spelling"
    }

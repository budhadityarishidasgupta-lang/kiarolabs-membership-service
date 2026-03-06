from app.database import get_connection


def get_synonym_question(user_id):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT word, synonym1, synonym2, synonym3, synonym4
        FROM words
        ORDER BY RANDOM()
        LIMIT 1
    """)

    row = cur.fetchone()

    return {
        "word": row[0],
        "options": [row[1], row[2], row[3], row[4]]
    }

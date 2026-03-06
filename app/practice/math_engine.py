from app.database import get_connection
import random


def get_math_question(user_id):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT id,
               question_text,
               option_a,
               option_b,
               option_c,
               option_d,
               correct_option,
               topic,
               difficulty
        FROM math_question_bank
        WHERE is_active = TRUE
        ORDER BY RANDOM()
        LIMIT 1
    """)

    row = cur.fetchone()

    if not row:
        return {"error": "No question found"}

    return {
        "question_id": row[0],
        "question": row[1],
        "options": {
            "A": row[2],
            "B": row[3],
            "C": row[4],
            "D": row[5]
        },
        "topic": row[7],
        "difficulty": row[8]
    }

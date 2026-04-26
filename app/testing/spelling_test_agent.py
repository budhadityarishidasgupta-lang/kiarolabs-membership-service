import random
from app.database import get_connection
from app.practice.spelling_engine import submit_spelling_answer
from app.intelligence.spelling_recommendations import generate_spelling_recommendations


def run_spelling_test(user_id: int, lesson_id: int):

    conn = get_connection()
    cur = conn.cursor()

    # Step 1: Fetch words
    cur.execute("""
        SELECT w.word_id, w.word
        FROM spelling_lesson_items li
        JOIN spelling_lessons l ON l.lesson_id = li.lesson_id
        JOIN spelling_words w ON w.word_id = li.word_id
        WHERE li.lesson_id = %s
          AND l.is_active = TRUE
        LIMIT 10
    """, (lesson_id,))

    rows = cur.fetchall()

    if not rows:
        cur.close()
        conn.close()
        return {"status": "FAIL", "reason": "No words found in lesson"}

    # Step 2: Simulate attempts
    for word_id, word in rows:

        # 50% correct, 50% wrong
        if random.random() > 0.5:
            answer = word
        else:
            answer = word[:-1] + "x"

        submit_spelling_answer(
            user_id=user_id,
            word_id=word_id,
            answer=answer
        )

    cur.close()
    conn.close()

    # Step 3: Generate recommendations
    recs = generate_spelling_recommendations(user_id)

    if not recs:
        return {"status": "FAIL", "reason": "No recommendations generated"}

    return {
        "status": "PASS",
        "recommendations_count": len(recs)
    }

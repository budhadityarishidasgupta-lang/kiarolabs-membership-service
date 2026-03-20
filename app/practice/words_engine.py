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

from app.database import get_connection
import random


def get_synonym_question(user_email):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT word_id, headword, synonyms
        FROM public.words
        ORDER BY RANDOM()
        LIMIT 1
    """)

    row = cur.fetchone()

    word_id = row[0]
    headword = row[1]
    synonyms = row[2]

    # split synonyms list
    synonym_list = [s.strip() for s in synonyms.split(",")]

    # choose one correct answer
    correct = random.choice(synonym_list)

    # generate distractors
    cur.execute("""
        SELECT synonyms
        FROM public.words
        WHERE word_id != %s
        ORDER BY RANDOM()
        LIMIT 3
    """, (word_id,))

    distractors = []

    for r in cur.fetchall():
        distractors.append(r[0].split(",")[0].strip())

    options = distractors + [correct]
    random.shuffle(options)

    return {
        "word_id": word_id,
        "word": headword,
        "options": options
    }

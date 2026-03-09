from app.database import get_connection
import random


def get_synonym_question(user_email):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT word_id, headword, synonyms
            FROM public.words
            WHERE synonyms IS NOT NULL AND TRIM(synonyms) <> ''
            ORDER BY RANDOM()
            LIMIT 1
        """)

        row = cur.fetchone()
        if not row:
            return {"error": "No synonym word found"}

        word_id = row[0]
        headword = row[1]
        synonyms = row[2]

        synonym_list = [s.strip() for s in synonyms.split(",") if s.strip()]
        if not synonym_list:
            return {"error": "No valid synonyms found"}

        correct = random.choice(synonym_list)

        cur.execute("""
            SELECT synonyms
            FROM public.words
            WHERE word_id != %s
              AND synonyms IS NOT NULL
              AND TRIM(synonyms) <> ''
            ORDER BY RANDOM()
            LIMIT 20
        """, (word_id,))

        distractor_pool = []
        for r in cur.fetchall():
            distractor_pool.extend([s.strip() for s in r[0].split(",") if s.strip()])

        distractor_pool = [d for d in distractor_pool if d.lower() != correct.lower()]
        unique_distractors = list(dict.fromkeys(distractor_pool))

        if len(unique_distractors) < 3:
            return {"error": "Not enough distractors found"}

        options = random.sample(unique_distractors, 3) + [correct]
        random.shuffle(options)

        return {
            "word_id": word_id,
            "word": headword,
            "options": options
        }
    finally:
        cur.close()
        conn.close()

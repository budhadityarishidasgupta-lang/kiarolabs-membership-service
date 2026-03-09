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


def submit_synonym_answer(user_id, user_email, word_id, chosen, response_ms):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT headword, synonyms, difficulty
            FROM public.words
            WHERE word_id = %s
            """,
            (word_id,),
        )
        row = cur.fetchone()

        if not row:
            return {"error": "Word not found"}

        headword, synonyms, difficulty = row
        synonym_list = [s.strip() for s in (synonyms or "").split(",") if s.strip()]

        if not synonym_list:
            return {"error": "No valid synonyms found"}

        normalized_chosen = (chosen or "").strip().lower()
        correct = normalized_chosen in [s.lower() for s in synonym_list]
        correct_answer = synonym_list[0]

        cur.execute(
            """
            INSERT INTO public.attempts
            (user_id, course_id, lesson_id, headword, is_correct, response_ms, chosen, correct_choice, ts, archived_at)
            VALUES (%s, NULL, NULL, %s, %s, %s, %s, %s, NOW(), NULL)
            """,
            (user_id, headword, correct, response_ms, chosen, correct_answer),
        )

        cur.execute(
            """
            SELECT total_attempts, correct_attempts, correct_streak
            FROM public.word_stats
            WHERE user_id = %s AND headword = %s
            """,
            (user_id, headword),
        )
        stats = cur.fetchone()

        if stats:
            total_attempts, correct_attempts, correct_streak = stats
            next_total = (total_attempts or 0) + 1
            next_correct_attempts = (correct_attempts or 0) + (1 if correct else 0)
            next_correct_streak = (correct_streak or 0) + 1 if correct else 0

            cur.execute(
                """
                UPDATE public.word_stats
                SET total_attempts = %s,
                    correct_attempts = %s,
                    correct_streak = %s,
                    last_seen = NOW(),
                    difficulty = %s
                WHERE user_id = %s AND headword = %s
                """,
                (next_total, next_correct_attempts, next_correct_streak, difficulty, user_id, headword),
            )
        else:
            cur.execute(
                """
                INSERT INTO public.word_stats
                (user_id, headword, correct_streak, total_attempts, correct_attempts, last_seen, mastered, difficulty, due_date, xp_points, streak_count, mastery_score)
                VALUES (%s, %s, %s, 1, %s, NOW(), FALSE, %s, NULL, 0, %s, %s)
                """,
                (
                    user_id,
                    headword,
                    1 if correct else 0,
                    1 if correct else 0,
                    difficulty,
                    1 if correct else 0,
                    1.0 if correct else 0.0,
                ),
            )

        conn.commit()

        return {
            "correct": correct,
            "correct_answer": correct_answer,
        }
    finally:
        cur.close()
        conn.close()

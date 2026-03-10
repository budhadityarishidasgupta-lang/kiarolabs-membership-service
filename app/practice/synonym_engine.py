from app.database import get_connection
import random


# --------------------------------------------------
# INTERNAL HELPERS
# --------------------------------------------------

def _resolve_user_id(cur, user_email):
    cur.execute(
        """
        SELECT id
        FROM kiaro_membership.members
        WHERE email = %s
        """,
        (user_email,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def _build_options(cur, word_id, correct_answer):
    cur.execute(
        """
        SELECT synonyms
        FROM public.words
        WHERE word_id != %s
          AND synonyms IS NOT NULL
          AND TRIM(synonyms) <> ''
        ORDER BY RANDOM()
        LIMIT 25
        """,
        (word_id,),
    )

    distractor_pool = []
    for r in cur.fetchall():
        distractor_pool.extend(
            [s.strip() for s in r[0].split(",") if s.strip()]
        )

    distractor_pool = [
        d for d in distractor_pool
        if d.lower() != correct_answer.lower()
    ]

    distractor_pool = list(dict.fromkeys(distractor_pool))

    if len(distractor_pool) < 3:
        return None

    options = random.sample(distractor_pool, 3) + [correct_answer]
    random.shuffle(options)

    return options


# --------------------------------------------------
# QUESTION GENERATION
# --------------------------------------------------

def get_synonym_question(user_email):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT word_id, headword, synonyms
            FROM public.words
            WHERE synonyms IS NOT NULL
              AND TRIM(synonyms) <> ''
            ORDER BY RANDOM()
            LIMIT 1
        """)

        row = cur.fetchone()
        if not row:
            return {"error": "No synonym word found"}

        word_id, headword, synonyms = row

        synonym_list = [
            s.strip() for s in synonyms.split(",") if s.strip()
        ]

        if not synonym_list:
            return {"error": "No valid synonyms"}

        correct = random.choice(synonym_list)

        options = _build_options(cur, word_id, correct)
        if not options:
            return {"error": "Not enough distractors"}

        return {
            "word_id": word_id,
            "word": headword,
            "options": options
        }

    finally:
        cur.close()
        conn.close()


# --------------------------------------------------
# ANSWER SUBMISSION
# --------------------------------------------------

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

        synonym_list = [
            s.strip() for s in (synonyms or "").split(",") if s.strip()
        ]

        normalized = (chosen or "").strip().lower()

        correct = normalized in [s.lower() for s in synonym_list]
        correct_answer = synonym_list[0]

        # record attempt
        cur.execute(
            """
            INSERT INTO public.attempts
            (user_id, course_id, lesson_id, headword,
             is_correct, response_ms, chosen,
             correct_choice, ts, archived_at)
            VALUES (%s,NULL,NULL,%s,%s,%s,%s,%s,NOW(),NULL)
            """,
            (user_id, headword, correct, response_ms, chosen, correct_answer),
        )

        # update stats
        cur.execute(
            """
            SELECT total_attempts, correct_attempts, correct_streak
            FROM public.word_stats
            WHERE user_id=%s AND headword=%s
            """,
            (user_id, headword),
        )

        stats = cur.fetchone()

        if stats:
            total, correct_count, streak = stats

            total += 1
            correct_count += 1 if correct else 0
            streak = streak + 1 if correct else 0

            cur.execute(
                """
                UPDATE public.word_stats
                SET total_attempts=%s,
                    correct_attempts=%s,
                    correct_streak=%s,
                    last_seen=NOW(),
                    difficulty=%s
                WHERE user_id=%s AND headword=%s
                """,
                (total, correct_count, streak, difficulty, user_id, headword),
            )

        else:
            cur.execute(
                """
                INSERT INTO public.word_stats
                (user_id, headword,
                 correct_streak,
                 total_attempts,
                 correct_attempts,
                 last_seen,
                 mastered,
                 difficulty,
                 due_date,
                 xp_points,
                 streak_count,
                 mastery_score)
                VALUES (%s,%s,%s,1,%s,NOW(),FALSE,%s,NULL,0,%s,%s)
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
            "correct_answer": correct_answer
        }

    finally:
        cur.close()
        conn.close()


# --------------------------------------------------
# PROGRESS
# --------------------------------------------------

def get_synonym_progress(user_email):
    conn = get_connection()
    cur = conn.cursor()

    try:
        user_id = _resolve_user_id(cur, user_email)

        if not user_id:
            return {
                "mastered_words": 0,
                "words_due": 0,
                "total_attempts": 0,
                "accuracy": 0.0
            }

        cur.execute(
            """
            SELECT COUNT(*)
            FROM public.word_stats
            WHERE user_id=%s AND mastered=TRUE
            """,
            (user_id,),
        )
        mastered = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COUNT(*)
            FROM public.word_stats
            WHERE user_id=%s
              AND due_date IS NOT NULL
              AND due_date <= NOW()
            """,
            (user_id,),
        )
        due = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COUNT(*),
                   COALESCE(SUM(CASE WHEN is_correct THEN 1 ELSE 0 END),0)
            FROM public.attempts
            WHERE user_id=%s
            """,
            (user_id,),
        )

        total, correct = cur.fetchone()

        accuracy = correct / total if total else 0.0

        return {
            "mastered_words": mastered,
            "words_due": due,
            "total_attempts": total,
            "accuracy": accuracy
        }

    finally:
        cur.close()
        conn.close()


# --------------------------------------------------
# NEXT QUESTION
# --------------------------------------------------

def get_next_synonym_question(user_email):
    conn = get_connection()
    cur = conn.cursor()

    try:
        user_id = _resolve_user_id(cur, user_email)

        row = None

        if user_id:
            cur.execute(
                """
                SELECT w.word_id, w.headword, w.synonyms
                FROM public.word_stats ws
                JOIN public.words w
                  ON w.headword = ws.headword
                WHERE ws.user_id=%s
                  AND ws.due_date IS NOT NULL
                  AND ws.due_date <= NOW()
                ORDER BY ws.due_date ASC
                LIMIT 1
                """,
                (user_id,),
            )

            row = cur.fetchone()

        if not row:
            cur.execute(
                """
                SELECT word_id, headword, synonyms
                FROM public.words
                WHERE synonyms IS NOT NULL
                ORDER BY RANDOM()
                LIMIT 1
                """
            )

            row = cur.fetchone()

        if not row:
            return {"error": "No synonym word found"}

        word_id, headword, synonyms = row

        synonym_list = [
            s.strip() for s in synonyms.split(",") if s.strip()
        ]

        correct = random.choice(synonym_list)

        options = _build_options(cur, word_id, correct)

        return {
            "word_id": word_id,
            "word": headword,
            "options": options
        }

    finally:
        cur.close()
        conn.close()


# --------------------------------------------------
# DASHBOARD
# --------------------------------------------------

def get_dashboard_stats(user_email):
    progress = get_synonym_progress(user_email)

    return {
        "synonyms": progress,
        "streak": 0,
        "xp": progress["total_attempts"] * 10
    }


# --------------------------------------------------
# SESSION START
# --------------------------------------------------

def get_practice_session(user_email):
    progress = get_synonym_progress(user_email)
    question = get_next_synonym_question(user_email)

    return {
        "course": "synonyms",
        "progress": progress,
        "session_length": 10,
        "xp_per_question": 10,
        "question": question
    }
from app.database import get_connection
import random
from fastapi import HTTPException


# --------------------------------------------------
# INTERNAL HELPERS
# --------------------------------------------------

def _resolve_user_id(cur, user_email):
    cur.execute(
        """
        SELECT user_id
        FROM public.users
        WHERE LOWER(email) = LOWER(%s)
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


def _table_exists(cur, table_name):
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = %s
        LIMIT 1
        """,
        (table_name,),
    )
    return cur.fetchone() is not None


def _get_table_columns(cur, table_name):
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        """,
        (table_name,),
    )
    return {row[0] for row in cur.fetchall()}


def _resolve_synonym_attempt_store(cur):
    for table_name in ("synonym_attempts", "words_attempts"):
        if _table_exists(cur, table_name):
            return table_name, _get_table_columns(cur, table_name)
    return None, set()


def _get_synonym_correct_column(columns):
    if "is_correct" in columns:
        return "is_correct"
    if "correct" in columns:
        return "correct"
    return None


def _get_synonym_timestamp_column(columns):
    if "created_at" in columns:
        return "created_at"
    if "submitted_at" in columns:
        return "submitted_at"
    return None


def _insert_synonym_attempt(cur, user_id, word_id, chosen, correct, response_ms):
    table_name, columns = _resolve_synonym_attempt_store(cur)
    if not table_name:
        raise HTTPException(status_code=500, detail="Synonym attempts table not available")

    insert_columns = ["user_id", "word_id"]
    insert_values = [user_id, word_id]

    answer_column = None
    for candidate in ("answer", "selected_answer", "chosen_answer"):
        if candidate in columns:
            answer_column = candidate
            break
    if answer_column:
        insert_columns.append(answer_column)
        insert_values.append(chosen)

    correct_column = _get_synonym_correct_column(columns)
    if not correct_column:
        raise HTTPException(status_code=500, detail="Synonym attempts table missing correctness column")
    insert_columns.append(correct_column)
    insert_values.append(correct)

    if "response_ms" in columns:
        insert_columns.append("response_ms")
        insert_values.append(response_ms or 0)
    elif "time_taken_ms" in columns:
        insert_columns.append("time_taken_ms")
        insert_values.append(response_ms or 0)

    timestamp_column = None
    for candidate in ("created_at", "submitted_at"):
        if candidate in columns:
            timestamp_column = candidate
            break

    values_sql = ["%s"] * len(insert_columns)
    if timestamp_column:
        insert_columns.append(timestamp_column)
        values_sql.append("NOW()")

    query = f"""
        INSERT INTO public.{table_name}
        ({", ".join(insert_columns)})
        VALUES ({", ".join(values_sql)})
    """
    cur.execute(query, tuple(insert_values))


def _get_recent_incorrect_synonym_word_ids(cur, user_id):
    table_name, columns = _resolve_synonym_attempt_store(cur)
    correct_column = _get_synonym_correct_column(columns)
    timestamp_column = _get_synonym_timestamp_column(columns)
    if not table_name or not correct_column or not timestamp_column:
        return []

    cur.execute(
        f"""
        SELECT word_id
        FROM public.{table_name}
        WHERE user_id = %s
          AND {correct_column} = FALSE
        ORDER BY {timestamp_column} DESC
        LIMIT 5
        """,
        (user_id,),
    )
    return [row[0] for row in cur.fetchall() if row and row[0]]


def get_synonym_attempt_summary(user_id):
    conn = get_connection()
    cur = conn.cursor()

    try:
        table_name, columns = _resolve_synonym_attempt_store(cur)
        correct_column = _get_synonym_correct_column(columns)
        if not table_name or not correct_column:
            return {"attempts": 0, "accuracy": 0.0}

        cur.execute(
            f"""
            SELECT
                COUNT(*) AS attempts,
                COALESCE(AVG(CASE WHEN {correct_column} THEN 1 ELSE 0 END) * 100, 0)
            FROM public.{table_name}
            WHERE user_id = %s
            """,
            (user_id,),
        )
        row = cur.fetchone() or (0, 0.0)
        accuracy = float(row[1] or 0)
        return {
            "attempts": row[0] or 0,
            "accuracy": round(accuracy, 2),
        }
    finally:
        cur.close()
        conn.close()


def get_latest_synonym_attempt_word_id(user_id):
    conn = get_connection()
    cur = conn.cursor()

    try:
        table_name, columns = _resolve_synonym_attempt_store(cur)
        timestamp_column = _get_synonym_timestamp_column(columns)
        if not table_name or not timestamp_column:
            return None

        cur.execute(
            f"""
            SELECT word_id
            FROM public.{table_name}
            WHERE user_id = %s
            ORDER BY {timestamp_column} DESC
            LIMIT 1
            """,
            (user_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()
        conn.close()


# --------------------------------------------------
# QUESTION GENERATION
# --------------------------------------------------

def get_synonym_question(user_email):
    conn = get_connection()
    cur = conn.cursor()

    try:
        row = None
        selected_word_id = None
        user_id = _resolve_user_id(cur, user_email)

        if user_id:
            weak_words = _get_recent_incorrect_synonym_word_ids(cur, user_id)
            if weak_words:
                selected_word_id = random.choice(weak_words)

        if selected_word_id:
            cur.execute(
                """
                SELECT word_id, headword, synonyms
                FROM public.words
                WHERE word_id = %s
                  AND synonyms IS NOT NULL
                  AND TRIM(synonyms) <> ''
                LIMIT 1
                """,
                (selected_word_id,),
            )
            row = cur.fetchone()

        if not row:
            cur.execute(
                """
                SELECT word_id, headword, synonyms
                FROM public.words
                WHERE synonyms IS NOT NULL
                  AND TRIM(synonyms) <> ''
                ORDER BY RANDOM()
                LIMIT 1
                """
            )
            row = cur.fetchone()

        if not row:
            cur.execute(
                """
                SELECT word_id, headword, synonyms
                FROM public.words
                WHERE synonyms IS NOT NULL
                  AND TRIM(synonyms) <> ''
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
    payload = {
        "word_id": word_id,
        "chosen": chosen,
        "response_ms": response_ms,
    }
    print("SUBMIT DEBUG:", payload)
    print("USER DEBUG:", user_id, user_email)

    conn = get_connection()
    cur = conn.cursor()

    try:
        # --- PRIMARY SOURCE (JWT) ---
        print("USER DEBUG BEFORE:", user_id, user_email)

        if not user_id:
            raise HTTPException(
                status_code=400,
                detail="User not identified - JWT missing user_id"
            )

        if word_id is None:
            raise HTTPException(status_code=400, detail="word_id is required")

        if not (chosen or "").strip():
            raise HTTPException(status_code=400, detail="chosen is required")

        cur.execute(
            """
            SELECT word_id, synonyms
            FROM public.words
            WHERE word_id = %s
            """,
            (word_id,),
        )

        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Word not found")

        _, synonyms = row

        if not synonyms:
            raise HTTPException(
                status_code=500,
                detail="No synonyms found for word"
            )

        if isinstance(synonyms, str):
            synonym_list = [s.strip().lower() for s in synonyms.split(",") if s.strip()]
        elif isinstance(synonyms, list):
            synonym_list = [str(s).strip().lower() for s in synonyms if str(s).strip()]
        else:
            raise HTTPException(
                status_code=500,
                detail="Invalid synonyms format"
            )

        if not synonym_list:
            raise HTTPException(
                status_code=500,
                detail="Failed to evaluate answer"
            )

        correct = chosen.strip().lower() in synonym_list
        correct_answer = synonym_list[0]

        print("INSERT DEBUG:", user_id, word_id, chosen, correct)

        _insert_synonym_attempt(cur, user_id, word_id, chosen, correct, response_ms)

        conn.commit()

        return {
            "correct": correct,
            "correct_answer": correct_answer
        }
    except HTTPException:
        raise
    except Exception as e:
        print("SUBMIT ERROR:", e)
        raise HTTPException(
            status_code=500,
            detail="Internal server error while submitting answer",
        )

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

        attempt_summary = get_synonym_attempt_summary(user_id)
        total = attempt_summary["attempts"]
        accuracy = (attempt_summary["accuracy"] / 100.0) if total else 0.0

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
    def compute_module_meta(module):
        attempts = module.get("attempts", 0)
        accuracy = module.get("accuracy", 0)
        unlocked = module.get("unlocked", False)

        if not unlocked:
            status = "locked"
        elif attempts == 0:
            status = "not_started"
        elif accuracy >= 80:
            status = "mastered"
        elif attempts >= 10:
            status = "completed"
        else:
            status = "in_progress"

        mastered = accuracy >= 80 and attempts >= 10

        if not unlocked:
            next_action = "locked"
        elif attempts == 0:
            next_action = "start"
        elif mastered:
            next_action = "advance"
        elif status == "completed":
            next_action = "retry"
        else:
            next_action = "continue"

        if not unlocked:
            priority = 999
        elif not mastered:
            priority = 100 - accuracy
        else:
            priority = 999

        module["status"] = status
        module["mastered"] = mastered
        module["next_action"] = next_action
        module["priority"] = priority

        return module

    progress = get_synonym_progress(user_email)
    modules = {
        "spelling": {
            "unlocked": True,
            "attempts": 0,
            "accuracy": 0,
        },
        "words": {
            "unlocked": True,
            "attempts": progress["total_attempts"],
            "accuracy": round(progress["accuracy"] * 100, 1),
        },
        "maths": {
            "unlocked": True,
            "attempts": 0,
            "accuracy": 0,
        },
    }

    # ComprehensionSprint Stats
    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor()

        # Get user_id from email
        cur.execute(
            """
            SELECT user_id FROM users WHERE email = %s
            """,
            (user_email,),
        )
        user_row = cur.fetchone()

        if user_row:
            user_id = user_row[0]

            cur.execute(
                """
                SELECT
                    COUNT(*) as attempts,
                    SUM(CASE WHEN correct THEN 1 ELSE 0 END) as correct
                FROM comprehension_attempts
                WHERE user_id = %s
                """,
                (user_id,),
            )

            row = cur.fetchone()

            attempts = row[0] or 0
            correct = row[1] or 0

            accuracy = round((correct / attempts) * 100, 1) if attempts > 0 else 0

            modules["comprehension"] = {
                "unlocked": True,
                "attempts": attempts,
                "accuracy": accuracy
            }

        else:
            modules["comprehension"] = {
                "unlocked": False,
                "attempts": 0,
                "accuracy": 0
            }

    except Exception as e:
        print("Comprehension dashboard error:", e)
        modules["comprehension"] = {
            "unlocked": False,
            "attempts": 0,
            "accuracy": 0
        }
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    for key in modules:
        modules[key] = compute_module_meta(modules[key])

    return {
        "synonyms": progress,
        "streak": 0,
        "xp": progress["total_attempts"] * 10,
        "modules": modules,
    }


# --------------------------------------------------
# SESSION START
# --------------------------------------------------

def _get_lesson_synonym_question(lesson_id):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT w.word_id, w.headword, w.synonyms
            FROM public.words w
            JOIN public.lesson_words lw ON lw.word_id = w.word_id
            WHERE lw.lesson_id = %s
              AND w.synonyms IS NOT NULL
              AND TRIM(w.synonyms) <> ''
            ORDER BY RANDOM()
            LIMIT 1
            """,
            (lesson_id,),
        )

        row = cur.fetchone()
        if not row:
            return {"error": "No synonym word found for lesson"}

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
            "options": options,
        }

    finally:
        cur.close()
        conn.close()


def get_practice_session(user_email, lesson_id):
    progress = get_synonym_progress(user_email)
    question = _get_lesson_synonym_question(lesson_id)

    return {
        "course": "synonyms",
        "progress": progress,
        "session_length": 10,
        "xp_per_question": 10,
        "question": question
    }

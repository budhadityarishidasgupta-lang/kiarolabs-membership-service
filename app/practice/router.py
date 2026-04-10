print("🚀 COMPREHENSION ROUTER LOADED")
print("🚀 ROUTER FILE IS LOADING")

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from typing import Optional
from pydantic import BaseModel
import csv
import io
import random

from app.auth import get_current_user
from app.database import get_connection

# Engines
from app.practice.math_engine import (
    get_math_lessons,
    get_math_question,
    submit_math_answer
)

from app.practice.math_test_engine import (
    get_math_tests,
    start_math_test,
    submit_math_test
)

from app.practice.synonym_engine import (
    get_synonym_question,
    submit_synonym_answer,
    get_synonym_progress,
    get_next_synonym_question,
    get_practice_session,
)
from app.practice.spelling_engine import (
    get_spelling_question,
    submit_spelling_answer
)
from app.practice.words_engine import (
    get_words_courses,
    get_words_question,
    submit_words_answer,
)
from app.dashboard.spelling_dashboard import get_spelling_dashboard
from app.comprehension.service import (
    list_passages,
    start_passage,
    submit_answer,
)
from app.comprehension.repository import (
    insert_passage,
    insert_question
)

router = APIRouter(prefix="/practice", tags=["practice"])


def is_admin(user):
    # 🔒 Phase 1: hardcoded admin (safe, reversible)
    return user.get("sub") == "rishi@test.com"


# -----------------------------
# Request Models
# -----------------------------

class SynonymAnswerRequest(BaseModel):
    word_id: int
    chosen: str
    response_ms: int

class SpellingAnswerRequest(BaseModel):
    word_id: int
    answer: str

class WordsAnswerRequest(BaseModel):
    word_id: int
    answer: str

class SessionAnswerRequest(BaseModel):
    word_id: int
    chosen: str
    response_ms: int


# -----------------------------
# Course / Lesson Discovery
# -----------------------------

@router.get("/courses")
def get_courses(user=Depends(get_current_user)):
    """
    Returns WordSprint courses and lessons
    used by the curriculum sidebar
    """

    conn = get_connection()

    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT
                c.course_id,
                c.title AS course_name,
                l.lesson_id,
                l.title AS lesson_name,
                l.sort_order AS lesson_order
            FROM public.courses c
            JOIN public.lessons l
                ON l.course_id = c.course_id
            WHERE c.course_type = 'synonym'
            ORDER BY c.course_id, COALESCE(l.sort_order,0)
        """)

        rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()


    courses = {}

    for course_id, course_name, lesson_id, lesson_name, lesson_order in rows:

        if course_id not in courses:
            courses[course_id] = {
                "course_id": course_id,
                "course_name": course_name,
                "lessons": []
            }

        courses[course_id]["lessons"].append({
            "lesson_id": lesson_id,
            "lesson_name": lesson_name,
            "lesson_order": lesson_order
        })

    return list(courses.values())


# -----------------------------
# MathSprint Endpoints
# -----------------------------

@router.get("/math/lessons")
def math_lessons():
    return get_math_lessons()


@router.get("/math/question")
def math_question(lesson_id: int, user=Depends(get_current_user)):
    return get_math_question(
        lesson_id=lesson_id,
        user_id=user.get("user_id")
    )


@router.post("/math/submit")
def math_submit(payload: dict, user=Depends(get_current_user)):
    return submit_math_answer(
        student_id=user["user_id"],
        lesson_id=payload["lesson_id"],
        question_id=payload["question_id"],
        selected_option=payload["selected_option"]
    )


@router.get("/math/tests")
def math_tests(user=Depends(get_current_user)):
    return get_math_tests(user)


@router.get("/math/test/start")
def math_test_start(test_id: str):
    return start_math_test(test_id)


@router.post("/math/test/submit")
def math_test_submit(payload: dict):
    return submit_math_test(payload["answers"])


# -----------------------------
# SpellingSprint Endpoints
# -----------------------------

@router.get("/spelling/courses")
def get_spelling_courses(user=Depends(get_current_user)):

    conn = get_connection()

    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT
                c.course_id,
                c.course_name,
                l.lesson_id,
                COALESCE(l.display_name, l.lesson_name) AS lesson_name,
                l.sort_order
            FROM spelling_courses c
            JOIN spelling_lessons l
                ON l.course_id = c.course_id
            WHERE l.is_active = true
            AND l.lesson_id IN (
                866,867,868,869,
                847,848,849,
                857,858,860,870
            )
            ORDER BY c.course_id, l.sort_order
        """)

        rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    courses = {}

    for course_id, course_name, lesson_id, lesson_name, lesson_order in rows:

        if course_id not in courses:
            courses[course_id] = {
                "course_id": course_id,
                "course_name": course_name,
                "lessons": []
            }

        courses[course_id]["lessons"].append({
            "lesson_id": lesson_id,
            "lesson_name": lesson_name,
            "lesson_order": lesson_order
        })

    return list(courses.values())


# -----------------------------
# SpellingSprint Question
# -----------------------------

@router.get("/spelling/question")
def spelling_question(
    lesson_id: int,
    word_id: Optional[int] = None,
    user=Depends(get_current_user)
):
    """
    Returns the next spelling word for a lesson
    """
    if word_id:
        from app.practice.spelling_engine import get_word_by_id
        return get_word_by_id(user["user_id"], word_id)

    print(f"SPELLING QUESTION DEBUG user={user}")
    print(f"SPELLING QUESTION DEBUG lesson_id={lesson_id}")

    if not user.get("user_id"):
        raise HTTPException(
            status_code=400,
            detail="User not provisioned in learning system"
        )

    print(f"SPELLING QUESTION DEBUG user={user}")
    print(f"SPELLING QUESTION DEBUG lesson_id={lesson_id}")

    return get_spelling_question(
        lesson_id=lesson_id,
        user_id=user["user_id"]
    )


@router.get("/spelling/micro-challenge")
def spelling_micro_challenge(
    word_id: int,
    user=Depends(get_current_user)
):
    from app.practice.spelling_engine import build_micro_challenge
    return build_micro_challenge(user["user_id"], word_id)




@router.get("/words/micro-challenge")
def words_micro_challenge(
    word_id: int,
    user=Depends(get_current_user)
):
    from app.practice.words_engine import build_words_micro_challenge
    return build_words_micro_challenge(user["user_id"], word_id)




@router.get("/engagement")
def get_engagement(user=Depends(get_current_user)):
    from app.database import get_connection

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT total_xp, current_streak
        FROM user_engagement
        WHERE user_id = %s
    """, (user["user_id"],))

    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        return {"xp": 0, "streak": 0}

    return {
        "xp": row[0],
        "streak": row[1]
    }


@router.get("/progress/weekly-improvement")
def get_weekly_improvement(user=Depends(get_current_user)):
    conn = get_connection()
    cur = conn.cursor()

    user_id = user["user_id"]

    try:
        # Current 7 days
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE correct = true) * 1.0 / NULLIF(COUNT(*),0)
            FROM spelling_attempts
            WHERE user_id = %s
            AND created_at >= NOW() - INTERVAL '7 days'
        """, (user_id,))

        current = cur.fetchone()[0] or 0

        # Previous 7 days
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE correct = true) * 1.0 / NULLIF(COUNT(*),0)
            FROM spelling_attempts
            WHERE user_id = %s
            AND created_at >= NOW() - INTERVAL '14 days'
            AND created_at < NOW() - INTERVAL '7 days'
        """, (user_id,))

        previous = cur.fetchone()[0] or 0
    finally:
        cur.close()
        conn.close()

    improvement = current - previous

    return {
        "current_accuracy": round(current * 100, 1),
        "previous_accuracy": round(previous * 100, 1),
        "improvement": round(improvement * 100, 1)
    }

@router.post("/words/micro-challenge/submit")
def submit_words_micro_challenge(
    payload: dict,
    user=Depends(get_current_user)
):
    """
    payload:
    {
        word_id: int,
        answers: [selected_option_index_1, selected_option_index_2]
    }
    """
    from app.database import get_connection

    conn = get_connection()
    cur = conn.cursor()

    word_id = payload["word_id"]
    answers = payload["answers"]

    # Fetch correct answer index (reuse existing structure)
    cur.execute("""
        SELECT correct_option_index
        FROM words
        WHERE id = %s
    """, (word_id,))

    row = cur.fetchone()

    if not row:
        return {"error": "Word not found"}

    correct_index = row[0]

    correct_count = sum(1 for a in answers if a == correct_index)
    accuracy = correct_count / len(answers)
    cur.execute("""
        SELECT accuracy
        FROM spelling_word_stats
        WHERE user_id = %s AND word_id = %s
    """, (user["user_id"], word_id))

    row_prev = cur.fetchone()
    previous_accuracy = row_prev[0] if row_prev else 0
    improvement = accuracy - previous_accuracy

    xp = int(accuracy * 10)

    from app.services.engagement_service import update_user_engagement

    engagement = update_user_engagement(user["user_id"], xp)

    return {
        "correct": correct_count,
        "total": len(answers),
        "accuracy": accuracy,
        "previous_accuracy": previous_accuracy,
        "improvement": improvement,
        "xp": xp,
        "total_xp": engagement["xp"],
        "streak": engagement["streak"],
        "message": "Word mastered!" if accuracy == 1 else "Keep practicing!"
    }

@router.post("/spelling/micro-challenge/submit")
def submit_micro_challenge(
    payload: dict,
    user=Depends(get_current_user)
):
    from app.database import get_connection

    conn = get_connection()
    cur = conn.cursor()

    word_id = payload["word_id"]
    answers = payload["answers"]

    cur.execute("SELECT word FROM spelling_words WHERE id = %s", (word_id,))
    row = cur.fetchone()

    if not row:
        return {"error": "Word not found"}

    correct_word = row[0]

    correct_count = sum(1 for a in answers if a.lower() == correct_word.lower())
    accuracy = correct_count / len(answers)
    cur.execute("""
        SELECT accuracy
        FROM spelling_word_stats
        WHERE user_id = %s AND word_id = %s
    """, (user["user_id"], word_id))

    row_prev = cur.fetchone()
    previous_accuracy = row_prev[0] if row_prev else 0
    improvement = accuracy - previous_accuracy

    xp = int(accuracy * 10)

    from app.services.engagement_service import update_user_engagement

    engagement = update_user_engagement(user["user_id"], xp)

    return {
        "correct": correct_count,
        "total": len(answers),
        "accuracy": accuracy,
        "previous_accuracy": previous_accuracy,
        "improvement": improvement,
        "xp": xp,
        "total_xp": engagement["xp"],
        "streak": engagement["streak"],
        "message": "Word mastered!" if accuracy == 1 else "Keep practicing!"
    }

# -----------------------------
# SpellingSprint Submit Answer
# -----------------------------

@router.post("/spelling/answer")
def spelling_answer(req: SpellingAnswerRequest, user=Depends(get_current_user)):
    """
    Saves spelling attempt and validates answer
    """
    if not user.get("user_id"):
        raise HTTPException(
            status_code=400,
            detail="User not provisioned in learning system"
        )

    return submit_spelling_answer(
        user_id=user["user_id"],
        word_id=req.word_id,
        answer=req.answer
    )


@router.post("/spelling/submit")
def spelling_submit(req: SpellingAnswerRequest, user=Depends(get_current_user)):
    if not user.get("user_id"):
        raise HTTPException(
            status_code=400,
            detail="User not provisioned in learning system"
        )

    return submit_spelling_answer(
        user_id=user["user_id"],
        word_id=req.word_id,
        answer=req.answer
    )


@router.get("/spelling/recommendations")
def spelling_recommendations(user=Depends(get_current_user)):
    from app.intelligence.spelling_recommendations import generate_spelling_recommendations
    return generate_spelling_recommendations(user["user_id"])


@router.get("/spelling/dashboard")
def spelling_dashboard(user=Depends(get_current_user)):
    if not user.get("user_id"):
        raise HTTPException(
            status_code=400,
            detail="User not provisioned in learning system"
        )

    return get_spelling_dashboard(user["user_id"])


# -----------------------------
# WordSprint Endpoints
# -----------------------------

@router.get("/words/courses")
def words_courses(user=Depends(get_current_user)):
    return get_words_courses()


@router.get("/words/question")
def words_question(lesson_id: int, user=Depends(get_current_user)):
    if not user.get("user_id"):
        raise HTTPException(
            status_code=400,
            detail="User not provisioned in learning system"
        )

    return get_words_question(
        lesson_id=lesson_id,
        user_id=user["user_id"],
    )


@router.post("/words/submit")
def words_submit(req: WordsAnswerRequest, user=Depends(get_current_user)):
    if not user.get("user_id"):
        raise HTTPException(
            status_code=400,
            detail="User not provisioned in learning system"
        )

    return submit_words_answer(
        user_id=user["user_id"],
        word_id=req.word_id,
        answer=req.answer,
    )

# -----------------------------
# WordSprint (Synonym) Endpoints
# -----------------------------

@router.get("/synonym/question")
def synonym_question(user=Depends(get_current_user)):
    return get_synonym_question(user["sub"])


@router.post("/synonym/answer")
def synonym_answer(req: SynonymAnswerRequest, user=Depends(get_current_user)):
    return submit_synonym_answer(
        user_id=user["user_id"],
        user_email=user["sub"],
        word_id=req.word_id,
        chosen=req.chosen,
        response_ms=req.response_ms,
    )


@router.get("/synonym/progress")
def synonym_progress(user=Depends(get_current_user)):
    return get_synonym_progress(user["sub"])


@router.get("/synonym/next-question")
def synonym_next(user=Depends(get_current_user)):
    return get_next_synonym_question(user["sub"])


# -----------------------------
# Unified Practice Session API
# -----------------------------

@router.get("/session/start")
def start_session(lesson_id: Optional[int] = 7, user=Depends(get_current_user)):
    """
    Starts a learning session and returns:
    - user progress
    - first question
    - session metadata
    """
    if lesson_id is None:
        lesson_id = 7

    return get_practice_session(user["sub"], lesson_id)


@router.post("/session/answer")
def session_answer(req: SessionAnswerRequest, user=Depends(get_current_user)):
    """
    Unified answer endpoint for frontend session handling.
    Currently routes to synonym engine.
    """
    return submit_synonym_answer(
        user_id=user["user_id"],
        user_email=user["sub"],
        word_id=req.word_id,
        chosen=req.chosen,
        response_ms=req.response_ms,
    )


@router.get("/session/next")
def session_next(user=Depends(get_current_user)):
    """
    Returns next question in session.
    """
    return get_next_synonym_question(user["sub"])


# -----------------------------
# Dashboard
# -----------------------------

def get_dashboard_stats(user_email):
    progress = get_synonym_progress(user_email)
    modules = {
        "spelling": {"unlocked": True, "attempts": 0, "accuracy": 0},
        "words": {"unlocked": True, "attempts": 0, "accuracy": 0},
        "maths": {"unlocked": True, "attempts": 0, "accuracy": 0},
        "comprehension": {"unlocked": True, "attempts": 0, "accuracy": 0},
    }

    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
        SELECT user_id FROM users WHERE email = %s
        """, (user_email,))
        user_row = cursor.fetchone()
        user_id = user_row[0] if user_row else None

        if user_id:
            cursor.execute("""
            SELECT
            COUNT(*) as attempts,
            COALESCE(AVG(CASE WHEN correct THEN 1 ELSE 0 END) * 100, 0)
            FROM spelling_attempts
            WHERE user_id = %s
            """, (user_id,))

            row = cursor.fetchone()

            spelling_attempts = row[0] or 0
            spelling_accuracy = round(row[1] or 0, 2)

            modules["spelling"] = {
                "unlocked": True,
                "attempts": spelling_attempts,
                "accuracy": spelling_accuracy
            }

            cursor.execute("""
            SELECT
            COUNT(*) as attempts,
            COALESCE(AVG(CASE WHEN is_correct THEN 1 ELSE 0 END) * 100, 0)
            FROM synonym_attempts
            WHERE user_id = %s
            """, (user_id,))

            row = cursor.fetchone()

            modules["words"] = {
                "unlocked": True,
                "attempts": row[0] or 0,
                "accuracy": round(row[1] or 0, 2)
            }

            cursor.execute("""
            SELECT
            COUNT(*) as attempts,
            COALESCE(AVG(CASE WHEN correct THEN 1 ELSE 0 END) * 100, 0)
            FROM math_attempts
            WHERE user_id = %s
            """, (user_id,))

            row = cursor.fetchone()

            modules["maths"] = {
                "unlocked": True,
                "attempts": row[0] or 0,
                "accuracy": round(row[1] or 0, 2)
            }

            cursor.execute("""
            SELECT
            COUNT(*) as attempts,
            COALESCE(AVG(CASE WHEN correct THEN 1 ELSE 0 END) * 100, 0)
            FROM comprehension_attempts
            WHERE user_id = %s
            """, (user_id,))

            row = cursor.fetchone()

            comprehension_attempts = row[0] or 0
            comprehension_accuracy = round(row[1] or 0, 2)

            modules["comprehension"] = {
                "unlocked": True,
                "attempts": comprehension_attempts,
                "accuracy": comprehension_accuracy
            }

    except Exception as e:
        print("Dashboard stats error:", e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

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

    for key in modules:
        modules[key] = compute_module_meta(modules[key])

    return {
        "synonyms": progress,
        "streak": 0,
        "xp": progress["total_attempts"] * 10,
        "modules": modules,
    }


@router.get("/dashboard")
def dashboard(user=Depends(get_current_user)):
    """
    Returns student progress summary
    across learning modules.
    """
    return get_dashboard_stats(user["sub"])

@router.get("/resume")
def get_resume_learning(user=Depends(get_current_user)):
    user_id = user["user_id"]

    conn = get_connection()
    cur = conn.cursor()

    result = {
        "spelling": None,
        "words": None,
        "maths": None,
        "comprehension": None
    }

    try:

        # -----------------------------
        # SPELLING
        # -----------------------------
        cur.execute("""
            SELECT word_id
            FROM spelling_attempts
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT 1
        """, (user_id,))
        row = cur.fetchone()

        if row:
            result["spelling"] = {
                "word_id": row[0],
                "next_action": "continue"
            }

        # -----------------------------
        # WORDSPRINT (SYNONYMS)
        # -----------------------------
        cur.execute("""
            SELECT word_id
            FROM synonym_attempts
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT 1
        """, (user_id,))
        row = cur.fetchone()

        if row:
            result["words"] = {
                "word_id": row[0],
                "next_action": "continue"
            }

        # -----------------------------
        # MATHSPRINT
        # -----------------------------
        cur.execute("""
            SELECT question_id
            FROM math_attempts
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT 1
        """, (user_id,))
        row = cur.fetchone()

        if row:
            result["maths"] = {
                "question_id": row[0],
                "next_action": "continue"
            }

        # -----------------------------
        # COMPREHENSION
        # -----------------------------
        cur.execute("""
            SELECT passage_id, question_id
            FROM comprehension_attempts
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT 1
        """, (user_id,))
        row = cur.fetchone()

        if row:
            passage_id = row[0]

            cur.execute("""
                SELECT question_id
                FROM comprehension_attempts
                WHERE user_id = %s AND passage_id = %s
            """, (user_id, passage_id))
            attempted = {r[0] for r in cur.fetchall()}

            cur.execute("""
                SELECT question_id
                FROM comprehension_questions
                WHERE passage_id = %s
                ORDER BY sort_order
            """, (passage_id,))
            all_questions = [r[0] for r in cur.fetchall()]

            next_question_id = None

            for question_id in all_questions:
                if question_id not in attempted:
                    next_question_id = question_id
                    break

            if not next_question_id and all_questions:
                next_question_id = all_questions[0]

            result["comprehension"] = {
                "passage_id": passage_id,
                "question_id": next_question_id,
                "next_action": "continue"
            }

        return result

    finally:
        cur.close()
        conn.close()


@router.get("/spelling/test")
def spelling_test(user=Depends(get_current_user)):
    """
    Test agent for spelling engine:
    - simulates attempts
    - generates stats
    - validates recommendations
    """

    from app.testing.spelling_test_agent import run_spelling_test

    return run_spelling_test(
        user_id=user["user_id"],
        lesson_id=866
    )

@router.get("/spelling/dashboard")
def spelling_dashboard_v2(user=Depends(get_current_user)):
    """
    Returns spelling dashboard:
    - summary stats
    - weak words
    - recommendations
    """

    from app.dashboard.spelling_dashboard import get_spelling_dashboard

    return get_spelling_dashboard(user["user_id"])


# -----------------------------
# ComprehensionSprint Endpoints
# -----------------------------

@router.get("/comprehension/passages")
def get_comprehension_passages(user=Depends(get_current_user)):
    return list_passages()

@router.get("/comprehension/courses")
def get_comprehension_courses(user=Depends(get_current_user)):
    """
    Returns comprehension passages grouped like:
    - Foundation
    - Intermediate
    - Advanced

    Same structure as WordSprint courses → lessons
    """

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                passage_id,
                title,
                difficulty
            FROM comprehension_passages
            WHERE is_active = true
            ORDER BY
                CASE
                    WHEN LOWER(difficulty) = 'foundation' THEN 1
                    WHEN LOWER(difficulty) = 'easy' THEN 1
                    WHEN LOWER(difficulty) = 'medium' THEN 2
                    WHEN LOWER(difficulty) = 'intermediate' THEN 2
                    WHEN LOWER(difficulty) = 'advanced' THEN 3
                    ELSE 1
                END,
                passage_id
        """)

        rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    courses = {
        "Foundation": [],
        "Intermediate": [],
        "Advanced": []
    }

    for passage_id, title, difficulty in rows:
        level = (difficulty or "").lower()

        if level in ["foundation", "easy"]:
            bucket = "Foundation"
        elif level in ["medium", "intermediate"]:
            bucket = "Intermediate"
        elif level in ["advanced"]:
            bucket = "Advanced"
        else:
            bucket = "Foundation"

        courses[bucket].append({
            "lesson_id": passage_id,
            "lesson_name": title
        })

    return [
        {
            "course_name": key,
            "lessons": value
        }
        for key, value in courses.items()
        if value
    ]


@router.get("/comprehension/start")
def start_comprehension(passage_id: int, user=Depends(get_current_user)):
    if not user.get("user_id"):
        raise HTTPException(
            status_code=400,
            detail="User not provisioned"
        )

    result = start_passage(passage_id, user["user_id"])

    if not result:
        raise HTTPException(status_code=404, detail="Passage not found")

    questions = result["questions"]

    next_question_id = None
    for question in questions:
        if not question.get("attempted"):
            next_question_id = question["question_id"]
            break

    if next_question_id is None and questions:
        next_question_id = questions[0]["question_id"]

    return {
        "passage": result["passage"],
        "questions": questions,
        "start_question_id": next_question_id,
    }


@router.get("/comprehension/question")
def get_comprehension_question(passage_id: int, user=Depends(get_current_user)):
    user_id = user["user_id"]

    conn = get_connection()
    cur = conn.cursor()

    try:
        selected_question_id = None

        cur.execute(
            """
            SELECT question_id
            FROM comprehension_attempts
            WHERE user_id = %s
            AND correct = false
            ORDER BY created_at DESC
            LIMIT 5
            """,
            (user_id,),
        )
        weak_questions = [r[0] for r in cur.fetchall() if r and r[0]]

        if weak_questions:
            selected_question_id = random.choice(weak_questions)

        if not selected_question_id:
            cur.execute(
                """
                SELECT question_id
                FROM comprehension_questions
                WHERE passage_id = %s
                ORDER BY RANDOM()
                LIMIT 1
                """,
                (passage_id,),
            )
            fallback_row = cur.fetchone()
            if fallback_row:
                selected_question_id = fallback_row[0]

        if not selected_question_id:
            cur.execute(
                """
                SELECT question_id
                FROM comprehension_questions
                WHERE passage_id = %s
                ORDER BY RANDOM()
                LIMIT 1
                """,
                (passage_id,),
            )
            safety_row = cur.fetchone()
            if safety_row:
                selected_question_id = safety_row[0]

        if not selected_question_id:
            raise HTTPException(status_code=404, detail="Question not found")

        cur.execute(
            """
            SELECT question_id, question_text, option_a, option_b, option_c, option_d
            FROM comprehension_questions
            WHERE question_id = %s
              AND passage_id = %s
            LIMIT 1
            """,
            (selected_question_id, passage_id),
        )
        question = cur.fetchone()

        if not question:
            cur.execute(
                """
                SELECT question_id, question_text, option_a, option_b, option_c, option_d
                FROM comprehension_questions
                WHERE passage_id = %s
                ORDER BY RANDOM()
                LIMIT 1
                """,
                (passage_id,),
            )
            question = cur.fetchone()

        if not question:
            raise HTTPException(status_code=404, detail="Question not found")

        return {
            "question_id": question[0],
            "question_text": question[1],
            "options": [question[2], question[3], question[4], question[5]],
        }
    finally:
        cur.close()
        conn.close()


@router.get("/comprehension/passage-summary")
def passage_summary(passage_id: int, user=Depends(get_current_user)):
    user_id = user["user_id"]

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                COUNT(*) as attempted,
                SUM(CASE WHEN correct THEN 1 ELSE 0 END) as correct
            FROM comprehension_attempts
            WHERE user_id = %s AND passage_id = %s
        """, (user_id, passage_id))

        row = cur.fetchone()

        total = row[0] or 0
        correct = row[1] or 0

        accuracy = round((correct / total) * 100, 1) if total > 0 else 0
        mastered = (accuracy >= 80) and (total >= 10)

        return {
            "passage_id": passage_id,
            "attempted": total,
            "correct": correct,
            "accuracy": accuracy,
            "completed": total >= 10,
            "mastered": mastered
        }

    finally:
        cur.close()
        conn.close()


@router.get("/comprehension/next-passage")
def get_next_passage(current_passage_id: int, user=Depends(get_current_user)):
    user_id = user["user_id"]

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN correct THEN 1 ELSE 0 END) as correct
            FROM comprehension_attempts
            WHERE user_id = %s AND passage_id = %s
        """, (user_id, current_passage_id))

        row = cur.fetchone()

        total = row[0] or 0
        correct = row[1] or 0
        accuracy = (correct / total) * 100 if total > 0 else 0
        mastered = (accuracy >= 80) and (total >= 10)

        if not mastered:
            return {
                "next_passage_id": current_passage_id,
                "unlocked": False,
                "reason": "Complete passage with 80% accuracy"
            }

        cur.execute("""
            SELECT passage_id
            FROM comprehension_passages
            WHERE passage_id > %s AND is_active = true
            ORDER BY passage_id ASC
            LIMIT 1
        """, (current_passage_id,))

        next_row = cur.fetchone()

        if not next_row:
            return {
                "next_passage_id": None,
                "unlocked": True,
                "message": "All passages completed"
            }

        return {
            "next_passage_id": next_row[0],
            "unlocked": True
        }

    finally:
        cur.close()
        conn.close()


@router.post("/comprehension/answer")
def submit_comprehension_answer(payload: dict, user=Depends(get_current_user)):
    if not user.get("user_id"):
        raise HTTPException(
            status_code=400,
            detail="User not provisioned in learning system"
        )

    return submit_answer(
        user_id=user["user_id"],
        passage_id=payload["passage_id"],
        question_id=payload["question_id"],
        selected_answer=payload["selected_answer"]
    )


@router.post("/comprehension/upload")
def upload_comprehension_csv(
    file: UploadFile = File(...),
    user=Depends(get_current_user)
):
    if not is_admin(user):
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        # ✅ FIX 1 — Handle BOM properly
        content = file.file.read().decode("utf-8-sig")

        reader = csv.DictReader(io.StringIO(content))

        current_passage_id = None

        for idx, row in enumerate(reader, start=1):
            # ✅ FIX 2 — Clean headers + values
            row = {
                (k.strip() if k else k): (v.strip() if isinstance(v, str) else v)
                for k, v in row.items()
            }

            # ✅ FIX 3 — Validate required fields
            if not row.get("question_text"):
                raise HTTPException(
                    status_code=400,
                    detail=f"Row {idx}: missing question_text"
                )

            # ✅ FIX 4 — Create passage
            if row.get("new_passage") == "1":
                current_passage_id = insert_passage(
                    title=row.get("title"),
                    passage_text=row.get("passage_text"),
                    difficulty=row.get("difficulty")
                )

            # ✅ FIX 5 — Guardrail (THIS WAS YOUR 500 ERROR)
            if not current_passage_id:
                raise HTTPException(
                    status_code=400,
                    detail=f"Row {idx}: question before passage"
                )

            # ✅ FIX 6 — Safe insert
            insert_question(
                passage_id=current_passage_id,
                question_text=row["question_text"],
                a=row["option_a"],
                b=row["option_b"],
                c=row["option_c"],
                d=row["option_d"],
                correct=row["correct_answer"],
                qtype=row.get("question_type", "comprehension"),
                order=int(row.get("sort_order") or 0)
            )

        return {"status": "success"}

    except HTTPException:
        raise

    except Exception as e:
        import traceback
        print("❌ COMPREHENSION CSV ERROR:", str(e))
        print(traceback.format_exc())

        raise HTTPException(status_code=500, detail=str(e))

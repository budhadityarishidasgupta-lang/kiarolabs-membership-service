print("🚀 COMPREHENSION ROUTER LOADED")
print("🚀 ROUTER FILE IS LOADING")

import logging
import uuid

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
    check_mock_access,
    get_math_tests,
    submit_math_paper,
    start_math_test,
    submit_math_test
)

from app.practice.synonym_engine import (
    get_synonym_question,
    submit_synonym_answer,
    get_synonym_progress,
    get_next_synonym_question,
    get_practice_session,
    get_latest_synonym_attempt_word_id,
    get_synonym_attempt_summary,
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
admin_router = APIRouter(tags=["admin"])
logger = logging.getLogger(__name__)


def is_admin(user):
    return user.get("role") == "admin"


def _missing_param(name: str):
    logger.warning("Practice endpoint missing required parameter: %s", name)
    raise HTTPException(status_code=400, detail=f"Missing required parameter: {name}")


def _require_user_id(user):
    user_id = user.get("user_id")
    if not user_id:
        logger.warning("Practice endpoint rejected request because user_id is missing")
        raise HTTPException(status_code=400, detail="User not provisioned in learning system")
    return user_id


def _require_payload_param(payload: dict, name: str):
    if not isinstance(payload, dict):
        _missing_param(name)
    value = payload.get(name)
    if value is None:
        _missing_param(name)
    return value


def _raise_not_found(detail: str):
    logger.warning("Practice endpoint invalid identifier: %s", detail)
    raise HTTPException(status_code=404, detail=detail)


def _get_table_columns(cur, table_name: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        """,
        (table_name,),
    )
    return {row[0] for row in cur.fetchall()}


def _window_attempt_totals(cur, table_name: str, *, user_columns: list[str], correct_columns: list[str], user_id: int, days_back_start: int, days_back_end: int | None = None):
    columns = _get_table_columns(cur, table_name)
    matched_user_columns = [column for column in user_columns if column in columns]
    matched_correct_column = next((column for column in correct_columns if column in columns), None)

    if not matched_user_columns or not matched_correct_column:
        return 0, 0

    where_clause = " OR ".join(f"{column} = %s" for column in matched_user_columns)
    params = [user_id for _ in matched_user_columns]

    time_clause = "created_at >= NOW() - (%s || ' days')::interval"
    params.append(days_back_start)

    if days_back_end is not None:
        time_clause += " AND created_at < NOW() - (%s || ' days')::interval"
        params.append(days_back_end)

    cur.execute(
        f"""
        SELECT
            COUNT(*),
            COUNT(*) FILTER (WHERE {matched_correct_column} = TRUE)
        FROM {table_name}
        WHERE ({where_clause})
          AND {time_clause}
        """,
        tuple(params),
    )
    attempts, correct = cur.fetchone()
    return attempts or 0, correct or 0


def _safe_execute(label: str, func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected practice endpoint failure in %s", label)
        raise HTTPException(status_code=500, detail="Internal error. Please try again.")


def _get_module_resume(cur, module: str, user_id: int):
    module_key = (module or "").strip().lower()

    if module_key == "spelling":
        cur.execute(
            """
            SELECT lw.lesson_id, sa.word_id
            FROM spelling_attempts sa
            LEFT JOIN spelling_lesson_words lw
                ON lw.word_id = sa.word_id
            WHERE sa.user_id = %s
            ORDER BY sa.created_at DESC
            LIMIT 1
            """,
            (user_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "lesson_id": row[0],
            "question_id": row[1],
        }

    if module_key == "words":
        cur.execute(
            """
            SELECT lw.lesson_id, wa.word_id
            FROM words_attempts wa
            LEFT JOIN words_lesson_words lw
                ON lw.word_id = wa.word_id
            WHERE wa.user_id = %s
            ORDER BY wa.created_at DESC
            LIMIT 1
            """,
            (user_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "lesson_id": row[0],
            "question_id": row[1],
        }

    if module_key in {"math", "maths"}:
        cur.execute(
            """
            SELECT lesson_id, question_id
            FROM math_attempts
            WHERE student_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (user_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "lesson_id": row[0],
            "question_id": row[1],
        }

    if module_key == "comprehension":
        cur.execute(
            """
            SELECT passage_id, question_id
            FROM comprehension_attempts
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (user_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "lesson_id": row[0],
            "question_id": row[1],
        }

    raise HTTPException(status_code=404, detail="Module not found")


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
    response_ms: int | None = None

class WordsAnswerRequest(BaseModel):
    word_id: int
    answer: str

class SessionAnswerRequest(BaseModel):
    word_id: int
    chosen: str
    response_ms: int


class RetryIncorrectRequest(BaseModel):
    paper_code: str
    incorrect_questions: list[int]


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
def math_question(
    lesson_id: Optional[int] = None,
    session_id: Optional[str] = None,
    user=Depends(get_current_user),
):
    if lesson_id is None:
        _missing_param("lesson_id")

    result = _safe_execute(
        "math_question",
        get_math_question,
        lesson_id=lesson_id,
        user_id=user.get("user_id"),
        session_id=session_id,
    )

    if not result or result.get("question_id") is None:
        _raise_not_found("Question not found")

    return result


@router.post("/math/submit")
def math_submit(payload: dict, user=Depends(get_current_user)):
    user_id = _require_user_id(user)
    session_id = payload.get("session_id") or str(uuid.uuid4())

    if "paper_code" in payload or "answers" in payload:
        paper_code = _require_payload_param(payload, "paper_code")
        answers = _require_payload_param(payload, "answers")
        return _safe_execute(
            "math_submit_paper",
            submit_math_paper,
            user_id=user_id,
            paper_code=paper_code,
            answers=answers,
            session_id=session_id,
        )

    lesson_id = _require_payload_param(payload, "lesson_id")
    question_id = _require_payload_param(payload, "question_id")
    selected_option = _require_payload_param(payload, "selected_option")

    result = _safe_execute(
        "math_submit",
        submit_math_answer,
        student_id=user_id,
        lesson_id=lesson_id,
        question_id=question_id,
        selected_option=selected_option,
        session_id=session_id,
    )

    if result.get("error") == "Question not found":
        _raise_not_found("Question not found")

    if result.get("error"):
        logger.warning("Practice math submit rejected: %s", result["error"])
        raise HTTPException(status_code=400, detail=result["error"])

    return result


@router.post("/math/retry-incorrect")
def math_retry_incorrect(req: RetryIncorrectRequest, user=Depends(get_current_user)):
    if not req.incorrect_questions:
        return {
            "questions": [],
            "message": "No incorrect questions",
        }

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT question_number, question_text
            FROM math_printable_questions
            WHERE paper_code = %s
              AND question_number = ANY(%s)
            ORDER BY question_number
            """,
            (req.paper_code, req.incorrect_questions),
        )

        rows = cur.fetchall()

        return {
            "questions": [
                {
                    "question_number": row[0],
                    "question_text": row[1],
                }
                for row in rows
            ]
        }
    finally:
        cur.close()
        conn.close()


@router.get("/math/history")
def math_history(paper_code: str, user=Depends(get_current_user)):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'math_attempts'
            """
        )
        math_attempt_columns = {row[0] for row in cur.fetchall()}

        if {"user_id", "paper_code", "score", "total", "created_at"}.issubset(math_attempt_columns):
            cur.execute(
                """
                SELECT score, total, created_at
                FROM math_attempts
                WHERE user_id = %s
                  AND paper_code = %s
                ORDER BY created_at DESC
                LIMIT 5
                """,
                (user["user_id"], paper_code),
            )
        else:
            cur.execute(
                """
                SELECT score, total, created_at
                FROM math_submission_attempts
                WHERE user_id = %s
                  AND paper_code = %s
                ORDER BY created_at DESC
                LIMIT 5
                """,
                (user["user_id"], paper_code),
            )

        rows = cur.fetchall()

        return [
            {
                "score": row[0],
                "total": row[1],
                "percentage": (row[0] * 100 / row[1]) if row[1] else 0,
                "date": row[2],
            }
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()


@router.get("/math/tests")
def math_tests(user=Depends(get_current_user)):
    # 🔒 CRITICAL FIX — prevent None user crash
    if not user or not user.get("sub"):
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing user"
        )

    return get_math_tests(user)


@router.get("/math/test/start")
def math_test_start(test_id: str, user=Depends(get_current_user)):
    email = user.get("sub")

    if user.get("role") == "admin":
        return start_math_test(test_id)

    # 🚨 CRITICAL CHECK
    has_access = check_mock_access(email, test_id)

    if not has_access:
        raise HTTPException(
            status_code=403,
            detail="Mock test not purchased"
        )

    return start_math_test(test_id)


@router.post("/math/test/submit")
def math_test_submit(payload: dict):
    return submit_math_test(payload["answers"])


@admin_router.post("/admin/math/printable/upload")
def upload_math_printable_csv(
    file: UploadFile = File(...),
    user=Depends(get_current_user),
):
    if not is_admin(user):
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        content = file.file.read().decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(content))

        required_fields = {
            "paper_code",
            "question_number",
            "question_text",
            "option_a",
            "option_b",
            "option_c",
            "option_d",
            "correct_answer",
        }

        if not reader.fieldnames:
            raise HTTPException(status_code=400, detail="CSV has no header row")

        headers = {field.strip() for field in reader.fieldnames if field}
        missing = sorted(required_fields - headers)
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"CSV missing required columns: {', '.join(missing)}",
            )

        conn = get_connection()
        cur = conn.cursor()
        rows_uploaded = 0

        try:
            for idx, row in enumerate(reader, start=1):
                clean = {
                    (key.strip() if key else key): (value.strip() if isinstance(value, str) else value)
                    for key, value in row.items()
                }

                paper_code = clean.get("paper_code")
                question_number = clean.get("question_number")
                question_text = clean.get("question_text")
                correct_answer = clean.get("correct_answer")

                if not paper_code or not question_number or not question_text or not correct_answer:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Row {idx}: paper_code, question_number, question_text and correct_answer are required",
                    )

                try:
                    question_number_int = int(question_number)
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Row {idx}: question_number must be an integer",
                    )

                cur.execute(
                    """
                    INSERT INTO math_printable_questions
                    (paper_code, question_number, question_text, option_a, option_b, option_c, option_d)
                    SELECT %s, %s, %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM math_printable_questions
                        WHERE paper_code = %s
                          AND question_number = %s
                    )
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        paper_code,
                        question_number_int,
                        question_text,
                        clean.get("option_a"),
                        clean.get("option_b"),
                        clean.get("option_c"),
                        clean.get("option_d"),
                        paper_code,
                        question_number_int,
                    ),
                )

                cur.execute(
                    """
                    INSERT INTO math_printable_answer_keys
                    (paper_code, question_number, correct_answer)
                    SELECT %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM math_printable_answer_keys
                        WHERE paper_code = %s
                          AND question_number = %s
                    )
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        paper_code,
                        question_number_int,
                        correct_answer,
                        paper_code,
                        question_number_int,
                    ),
                )

                rows_uploaded += 1

            conn.commit()
            return {"status": "uploaded", "rows": rows_uploaded}
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            print("MATH PRINTABLE CSV ERROR:", str(e))
            raise HTTPException(status_code=500, detail="CSV upload failed")
        finally:
            cur.close()
            conn.close()

    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="CSV must be UTF-8 encoded")


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
    lesson_id: Optional[int] = None,
    word_id: Optional[int] = None,
    session_id: Optional[str] = None,
    user=Depends(get_current_user)
):
    """
    Returns the next spelling word for a lesson
    """
    user_id = _require_user_id(user)

    if word_id is not None:
        from app.practice.spelling_engine import get_word_by_id
        result = _safe_execute("spelling_question_by_id", get_word_by_id, user_id, word_id)
        if not result or result.get("word_id") is None or result.get("error"):
            _raise_not_found("Question not found")
        return result

    if lesson_id is None:
        _missing_param("lesson_id")

    result = _safe_execute(
        "spelling_question",
        get_spelling_question,
        lesson_id=lesson_id,
        user_id=user_id,
        session_id=session_id,
    )

    if not result or result.get("word_id") is None:
        _raise_not_found("Question not found")

    return result


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
        current_attempts = 0
        current_correct = 0
        previous_attempts = 0
        previous_correct = 0

        table_configs = [
            ("spelling_attempts", ["user_id"], ["correct"]),
            ("words_attempts", ["user_id"], ["correct"]),
            ("math_attempts", ["student_id", "user_id"], ["is_correct", "correct"]),
            ("comprehension_attempts", ["user_id"], ["correct"]),
        ]

        for table_name, user_columns, correct_columns in table_configs:
            attempts, correct = _window_attempt_totals(
                cur,
                table_name,
                user_columns=user_columns,
                correct_columns=correct_columns,
                user_id=user_id,
                days_back_start=7,
            )
            current_attempts += attempts
            current_correct += correct

            attempts, correct = _window_attempt_totals(
                cur,
                table_name,
                user_columns=user_columns,
                correct_columns=correct_columns,
                user_id=user_id,
                days_back_start=14,
                days_back_end=7,
            )
            previous_attempts += attempts
            previous_correct += correct
    finally:
        cur.close()
        conn.close()

    current = (current_correct / current_attempts) if current_attempts else 0
    previous = (previous_correct / previous_attempts) if previous_attempts else 0
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
def spelling_answer(payload: dict, user=Depends(get_current_user)):
    """
    Saves spelling attempt and validates answer
    """
    user_id = _require_user_id(user)
    word_id = _require_payload_param(payload, "word_id")
    answer = _require_payload_param(payload, "answer")
    lesson_id = payload.get("lesson_id")
    question_id = payload.get("question_id")
    session_id = payload.get("session_id") or str(uuid.uuid4())

    result = _safe_execute(
        "spelling_answer",
        submit_spelling_answer,
        user_id=user_id,
        lesson_id=lesson_id,
        word_id=word_id,
        answer=answer,
        session_id=session_id,
        question_id=question_id,
    )

    if not result or not result.get("correct_word"):
        _raise_not_found("Question not found")

    return result


@router.post("/spelling/submit")
def spelling_submit(payload: dict, user=Depends(get_current_user)):
    user_id = _require_user_id(user)
    word_id = _require_payload_param(payload, "word_id")
    answer = _require_payload_param(payload, "answer")
    lesson_id = payload.get("lesson_id")
    question_id = payload.get("question_id")
    try:
        response_ms = int(payload.get("response_ms") or 0)
    except (TypeError, ValueError):
        response_ms = 0
    session_id = payload.get("session_id") or str(uuid.uuid4())

    result = _safe_execute(
        "spelling_submit",
        submit_spelling_answer,
        user_id=user_id,
        lesson_id=lesson_id,
        word_id=word_id,
        answer=answer,
        response_ms=response_ms,
        session_id=session_id,
        question_id=question_id,
    )

    if not result or not result.get("correct_word"):
        _raise_not_found("Question not found")

    return result


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
def words_question(lesson_id: Optional[int] = None, user=Depends(get_current_user)):
    user_id = _require_user_id(user)

    if lesson_id is None:
        _missing_param("lesson_id")

    result = _safe_execute(
        "words_question",
        get_words_question,
        lesson_id=lesson_id,
        user_id=user_id,
    )

    if not result or result.get("word_id") is None:
        _raise_not_found("Question not found")

    return result


@router.post("/words/submit")
def words_submit(payload: dict, user=Depends(get_current_user)):
    user_id = _require_user_id(user)
    word_id = _require_payload_param(payload, "word_id")
    answer = _require_payload_param(payload, "answer")

    result = _safe_execute(
        "words_submit",
        submit_words_answer,
        user_id=user_id,
        word_id=word_id,
        answer=answer,
    )

    if not result or not result.get("correct_answer"):
        _raise_not_found("Question not found")

    return result

# -----------------------------
# WordSprint (Synonym) Endpoints
# -----------------------------

@router.get("/synonym/question")
def synonym_question(user=Depends(get_current_user)):
    return get_synonym_question(user["sub"])


@router.post("/synonym/answer")
def synonym_answer(req: SynonymAnswerRequest, user: dict = Depends(get_current_user)):
    user_id = user.get("user_id")
    user_email = user.get("sub")

    return submit_synonym_answer(
        user_id=user_id,
        user_email=user_email,
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

            synonym_summary = get_synonym_attempt_summary(user_id)

            modules["words"] = {
                "unlocked": True,
                "attempts": synonym_summary["attempts"],
                "accuracy": synonym_summary["accuracy"],
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

        # SPELLING
        try:
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
        except:
            result["spelling"] = None

        # WORDSPRINT
        try:
            latest_word_id = get_latest_synonym_attempt_word_id(user_id)

            if latest_word_id:
                result["words"] = {
                    "word_id": latest_word_id,
                    "next_action": "continue"
                }
        except:
            result["words"] = None

        # MATHSPRINT
        try:
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
        except:
            result["maths"] = None

        # COMPREHENSION
        try:
            cur.execute("""
                SELECT passage_id, question_id
                FROM comprehension_attempts
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT 1
            """, (user_id,))
            row = cur.fetchone()

            if row:
                result["comprehension"] = {
                    "passage_id": row[0],
                    "question_id": row[1],
                    "next_action": "continue"
                }
        except:
            result["comprehension"] = None

        return result

    finally:
        cur.close()
        conn.close()


@router.get("/{module}/resume")
def get_module_resume(module: str, user=Depends(get_current_user)):
    user_id = _require_user_id(user)

    conn = get_connection()
    cur = conn.cursor()

    try:
        result = _get_module_resume(cur, module, user_id)
        if not result:
            _raise_not_found("Resume not found")
        return result
    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected practice endpoint failure in get_module_resume")
        raise HTTPException(status_code=500, detail="Internal error. Please try again.")
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
    return _safe_execute("get_comprehension_passages", list_passages)

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

    except Exception:
        logger.exception("Unexpected practice endpoint failure in get_comprehension_courses")
        raise HTTPException(status_code=500, detail="Internal error. Please try again.")
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
def start_comprehension(passage_id: Optional[int] = None, user=Depends(get_current_user)):
    if passage_id is None:
        _missing_param("passage_id")

    user_id = _require_user_id(user)

    result = _safe_execute("start_comprehension", start_passage, passage_id, user_id)

    if not result:
        logger.warning("Practice comprehension start invalid passage_id: %s", passage_id)
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
def get_comprehension_question(
    passage_id: Optional[int] = None,
    question_id: Optional[int] = None,
    user=Depends(get_current_user),
):
    if passage_id is None:
        _missing_param("passage_id")

    user_id = _require_user_id(user)

    conn = get_connection()
    cur = conn.cursor()

    try:
        selected_question_id = None

        if question_id is not None:
            selected_question_id = question_id

        if selected_question_id is None:
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
                ORDER BY sort_order ASC, question_id ASC
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
            _raise_not_found("Question not found")

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

        if question_id is not None and not question:
            _raise_not_found("Question not found")

        if not question:
            cur.execute(
                """
                SELECT question_id, question_text, option_a, option_b, option_c, option_d
                FROM comprehension_questions
                WHERE passage_id = %s
                ORDER BY sort_order ASC, question_id ASC
                LIMIT 1
                """,
                (passage_id,),
            )
            question = cur.fetchone()

        if not question:
            _raise_not_found("Question not found")

        return {
            "question_id": question[0],
            "question_text": question[1],
            "options": [question[2], question[3], question[4], question[5]],
        }
    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected practice endpoint failure in get_comprehension_question")
        raise HTTPException(status_code=500, detail="Internal error. Please try again.")
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
    user_id = _require_user_id(user)
    passage_id = _require_payload_param(payload, "passage_id")
    question_id = _require_payload_param(payload, "question_id")
    selected_answer = _require_payload_param(payload, "selected_answer")

    from app.comprehension.repository import get_question_by_id

    question = _safe_execute("submit_comprehension_answer.lookup", get_question_by_id, question_id)
    if not question or question.get("passage_id") != passage_id:
        _raise_not_found("Question not found")

    return _safe_execute(
        "submit_comprehension_answer",
        submit_answer,
        user_id=user_id,
        passage_id=passage_id,
        question_id=question_id,
        selected_answer=selected_answer,
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

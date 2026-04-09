# app/comprehension/service.py

from app.comprehension.repository import (
    get_active_passages,
    get_question_by_id,
    insert_attempt
)
from app.database import get_connection


# =========================
# PASSAGE LIST
# =========================

def list_passages():
    return get_active_passages()


# =========================
# START PASSAGE SESSION
# =========================

def start_passage(passage_id, user_id=None):
    conn = get_connection()
    cur = conn.cursor()

    try:
        # Fetch passage
        cur.execute("""
            SELECT passage_id, title, passage_text, difficulty
            FROM comprehension_passages
            WHERE passage_id = %s AND is_active = true
        """, (passage_id,))
        passage = cur.fetchone()

        if not passage:
            return None

        # Fetch questions
        cur.execute("""
            SELECT question_id, question_text, option_a, option_b, option_c, option_d, sort_order
            FROM comprehension_questions
            WHERE passage_id = %s
            ORDER BY sort_order
        """, (passage_id,))
        questions = cur.fetchall()

        # NEW: get attempted questions for this user + passage
        attempted = set()

        if user_id:
            cur.execute("""
                SELECT question_id
                FROM comprehension_attempts
                WHERE user_id = %s AND passage_id = %s
            """, (user_id, passage_id))

            attempted = {row[0] for row in cur.fetchall()}

        return {
            "passage": {
                "passage_id": passage[0],
                "title": passage[1],
                "passage_text": passage[2],
                "difficulty": passage[3]
            },
            "questions": [
                {
                    "question_id": q[0],
                    "question_text": q[1],
                    "options": [q[2], q[3], q[4], q[5]],
                    "attempted": q[0] in attempted
                }
                for q in questions
            ]
        }

    finally:
        cur.close()
        conn.close()


# =========================
# SUBMIT ANSWER
# =========================

def submit_answer(user_id, passage_id, question_id, selected_answer):
    question = get_question_by_id(question_id)

    if not question:
        return {"correct": False}

    # safety check: ensure question belongs to passage
    if question.get("passage_id") != passage_id:
        return {"correct": False}

    correct = (question["correct_answer"] == selected_answer)

    insert_attempt(
        user_id=user_id,
        passage_id=passage_id,
        question_id=question_id,
        selected_answer=selected_answer,
        correct=correct
    )

    return {
        "correct": correct
    }

import json
from datetime import datetime

from fastapi import HTTPException

from app.database import get_connection


def init_math_submission_tables():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS math_submission_attempts (
                id SERIAL PRIMARY KEY,
                user_id INT NOT NULL,
                paper_code TEXT NOT NULL,
                answers JSONB NOT NULL,
                score INT NOT NULL,
                total INT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _normalize_board(board):
    if board is None:
        return []
    if isinstance(board, list):
        board_str = " ".join(str(part) for part in board)
    elif isinstance(board, tuple):
        board_str = " ".join(str(part) for part in board)
    else:
        board_str = str(board)

    normalized = []
    if "GL" in board_str:
        normalized.append("GL")
    if "CEM" in board_str:
        normalized.append("CEM")
    if "Independent" in board_str:
        normalized.append("Independent")

    return normalized


def _json_answer_count(answers):
    if not answers:
        return 0
    if isinstance(answers, (list, tuple, dict)):
        return len(answers)
    if isinstance(answers, str):
        try:
            parsed = json.loads(answers)
            if isinstance(parsed, (list, tuple, dict)):
                return len(parsed)
        except Exception:
            return 0
    return 0


def _get_table_columns(cur, table_name):
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        """,
        (table_name,),
    )
    return {row[0] for row in cur.fetchall()}


def _get_question_count(cur, paper_code, fallback_total):
    math_question_columns = _get_table_columns(cur, "math_questions")
    if "paper_code" not in math_question_columns:
        return fallback_total or 0

    cur.execute(
        """
        SELECT COUNT(*)
        FROM math_questions
        WHERE paper_code = %s
        """,
        (paper_code,),
    )
    return cur.fetchone()[0]


def _get_submission_state(cur, user_id):
    state = {}
    if not user_id:
        return state

    cur.execute(
        """
        SELECT paper_code, answers, score, total, created_at
        FROM math_submission_attempts
        WHERE user_id = %s
        ORDER BY created_at DESC
        """,
        (user_id,),
    )

    for paper_code, answers, score, total, created_at in cur.fetchall():
        if paper_code in state:
            continue

        state[paper_code] = {
            "answered_questions": _json_answer_count(answers),
            "last_score": score,
            "stored_total": total,
            "last_attempt_time": created_at,
        }

    math_attempt_columns = _get_table_columns(cur, "math_attempts")
    required_columns = {"user_id", "paper_code", "created_at"}

    if required_columns.issubset(math_attempt_columns):
        cur.execute(
            """
            SELECT paper_code, COUNT(*) AS answered_questions, MAX(created_at) AS last_attempt_time
            FROM math_attempts
            WHERE user_id = %s
            GROUP BY paper_code
            """,
            (user_id,),
        )

        for paper_code, answered_questions, last_attempt_time in cur.fetchall():
            existing = state.get(paper_code)
            if existing and existing["last_attempt_time"] and existing["last_attempt_time"] >= last_attempt_time:
                continue

            state[paper_code] = {
                "answered_questions": answered_questions,
                "last_score": None,
                "stored_total": None,
                "last_attempt_time": last_attempt_time,
            }

        if "score" in math_attempt_columns:
            for paper_code in list(state):
                cur.execute(
                    """
                    SELECT score
                    FROM math_attempts
                    WHERE user_id = %s
                      AND paper_code = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (user_id, paper_code),
                )
                score_row = cur.fetchone()
                if score_row and score_row[0] is not None:
                    state[paper_code]["last_score"] = score_row[0]

    return state


def _build_resume_mock(tests):
    resumable = [
        test
        for test in tests
        if test.get("purchased") is True and test.get("in_progress") is True
    ]

    if not resumable:
        return None

    resumable.sort(key=lambda test: test.get("_last_attempt_time") or datetime.min, reverse=True)
    latest = resumable[0]

    return {
        "paper_code": latest["paper_code"],
        "title": latest["title"],
        "saved_question_number": latest["saved_question_number"],
        "total_questions": latest["total_questions"],
    }


def _build_progression(tests):
    tests_sorted = sorted(tests, key=lambda test: test["sort_order"])
    progression = []

    for test in tests_sorted:
        if not test["purchased"]:
            state = "locked"
        elif test["completed"]:
            state = "completed"
        elif test["in_progress"]:
            state = "in_progress"
        else:
            state = "not_attempted"

        progression.append(
            {
                "paper_code": test["paper_code"],
                "state": state,
                "score": test.get("last_score"),
            }
        )

    return progression


def _get_recommended_paper_code(tests):
    tests_sorted = sorted(tests, key=lambda test: test["sort_order"])

    candidates = [
        test
        for test in tests_sorted
        if test["purchased"] and not test["completed"] and not test["in_progress"]
    ]

    if candidates:
        return candidates[0]["paper_code"]

    unpurchased = [test for test in tests_sorted if not test["purchased"]]
    if unpurchased:
        return unpurchased[0]["paper_code"]

    return None


def _build_math_test_response(row, access, total_questions=None, state=None):
    paper_code = row[0]
    paper_name = row[1]
    total = total_questions if total_questions is not None else row[3]
    state = state or {}
    answered_questions = state.get("answered_questions", 0)
    completed = bool(total and answered_questions >= total)
    in_progress = answered_questions > 0 and not completed
    last_score = state.get("last_score") if completed else None
    purchased = access == "full"

    return {
        "test_id": paper_code,
        "name": paper_name,
        "duration": row[2],
        "total_questions": total,
        "access": access,
        "paper_code": paper_code,
        "paper_name": paper_name,
        "title": row[4],
        "subject": row[5],
        "board": _normalize_board(row[6]),
        "difficulty": row[7],
        "sort_order": row[8],
        "purchased": purchased,
        "in_progress": in_progress,
        "completed": completed,
        "saved_question_number": answered_questions + 1 if in_progress else None,
        "last_score": last_score,
        "_last_attempt_time": state.get("last_attempt_time"),
    }


def get_math_tests(user):
    conn = get_connection()
    cur = conn.cursor()

    email = user.get("sub") if user else None
    member_id = user.get("member_id") if user else None
    user_id = user.get("user_id") if user else None

    if not email:
        cur.close()
        conn.close()
        return {"tests": [], "resume_mock": None}

    # 🔓 Admin / UAT bypass
    # Step 1: resolve member_id from email if needed
    if not member_id:
        cur.execute(
            """
            SELECT id
            FROM kiaro_membership.members
            WHERE LOWER(email) = LOWER(%s)
        """,
            (email,),
        )
        member_row = cur.fetchone()
        member_id = member_row[0] if member_row else None

    # Step 2: get purchased tests
    purchased_tests = set()
    if member_id:
        cur.execute(
            """
            SELECT test_id
            FROM math_user_test_access
            WHERE member_id = %s
        """,
            (member_id,),
        )
        purchased_tests = {row[0] for row in cur.fetchall()}

    admin_bypass = user.get("role") == "admin" or email in ["rishi@test.com", "testrishi@gmail.com"]

    print("MOCK TEST MEMBER:", member_id)
    print("MOCK TEST PURCHASED:", purchased_tests)

    # Step 3: get all active tests
    cur.execute(
        """
        SELECT
            paper_code,
            paper_name,
            duration_minutes,
            total_questions,
            title,
            subject,
            board,
            difficulty,
            sort_order
        FROM math_test_papers
        WHERE is_active = TRUE
        ORDER BY sort_order ASC;
    """
    )

    all_tests = cur.fetchall()
    submission_state = _get_submission_state(cur, user_id)

    final_tests = []

    # Step 4: build final response
    for test in all_tests:
        test_id = test[0]
        access = "full" if admin_bypass or test_id in purchased_tests else "locked"
        total_questions = _get_question_count(cur, test_id, test[3])

        final_tests.append(
            _build_math_test_response(
                test,
                access,
                total_questions=total_questions,
                state=submission_state.get(test_id),
            )
        )

    resume_mock = _build_resume_mock(final_tests)
    progression = _build_progression(final_tests)
    recommended_paper_code = _get_recommended_paper_code(final_tests)

    for test in final_tests:
        test.pop("_last_attempt_time", None)

    cur.close()
    conn.close()

    return {
        "tests": final_tests,
        "resume_mock": resume_mock,
        "progression": progression,
        "recommended_paper_code": recommended_paper_code,
    }


def start_math_test(test_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT total_questions
        FROM math_test_papers
        WHERE paper_code = %s
    """,
        (test_id,),
    )

    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return {"error": "Test not found"}

    total_questions = row[0]
    cur.execute(
        """
        SELECT
            q.id,
            q.stem,
            q.option_a,
            q.option_b,
            q.option_c,
            q.option_d,
            q.option_e,
            q.correct_option
        FROM math_questions q
        ORDER BY RANDOM()
        LIMIT %s;
    """,
        (total_questions,),
    )

    questions = []

    for q in cur.fetchall():
        options = [q[2], q[3], q[4], q[5], q[6]]

        # remove null options
        options = [opt for opt in options if opt is not None]

        questions.append(
            {
                "question_id": q[0],
                "stem": q[1],
                "options": options,
                "correct_option": q[7],
            }
        )

    print(f"DEBUG TEST: returning {len(questions)} cleaned questions")

    cur.close()
    conn.close()

    return {
        "test_id": test_id,
        "questions": questions,
    }


def check_mock_access(email: str, test_id: str):
    conn = get_connection()
    cur = conn.cursor()

    try:
        # Get member id
        cur.execute(
            """
            SELECT id
            FROM kiaro_membership.members
            WHERE LOWER(email) = LOWER(%s)
        """,
            (email,),
        )
        row = cur.fetchone()

        if not row:
            return False

        member_id = row[0]

        # Check access
        cur.execute(
            """
            SELECT 1
            FROM math_user_test_access
            WHERE member_id = %s
            AND test_id = %s
        """,
            (member_id, test_id),
        )

        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()


def submit_math_test(answers):
    score = 0
    total = len(answers)

    for a in answers:
        if a["selected_option"] == a["correct_option"]:
            score += 1

    return {"score": score, "total": total}


def submit_math_paper(user_id, paper_code, answers):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT question_number, correct_answer
            FROM math_printable_answer_keys
            WHERE paper_code = %s
            ORDER BY question_number
            """,
            (paper_code,),
        )
        rows = cur.fetchall()

        if not rows:
            raise HTTPException(status_code=400, detail="Answer key not found")

        if isinstance(answers, dict):
            user_answers = [
                answers.get(f"q{question_number}", answers.get(str(question_number)))
                for question_number, _correct in rows
            ]
        else:
            user_answers = answers or []

        results = []
        score = 0

        for i, row in enumerate(rows):
            question_number = row[0]
            correct_answer = str(row[1]).strip().lower()

            user_answer = ""
            if i < len(user_answers):
                user_answer = str(user_answers[i]).strip().lower()

            is_correct = user_answer == correct_answer

            if is_correct:
                score += 1

            results.append(
                {
                    "question_number": question_number,
                    "user_answer": user_answer,
                    "correct_answer": correct_answer,
                    "is_correct": is_correct,
                }
            )

        total = len(rows)

        cur.execute(
            """
            INSERT INTO math_submission_attempts
            (user_id, paper_code, answers, score, total)
            VALUES (%s, %s, %s::jsonb, %s, %s)
            """,
            (user_id, paper_code, json.dumps(answers or {}), score, total),
        )

        conn.commit()

        return {
            "score": score,
            "total": total,
            "percentage": (score * 100 / total) if total else 0,
            "breakdown": results,
        }
    finally:
        cur.close()
        conn.close()

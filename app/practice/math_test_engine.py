import json

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
        return board
    if isinstance(board, tuple):
        return list(board)
    if isinstance(board, str):
        return [part.strip() for part in board.split(",") if part.strip()]
    return [board]


def _build_math_test_response(row, access):
    paper_code = row[0]
    paper_name = row[1]

    return {
        "test_id": paper_code,
        "name": paper_name,
        "duration": row[2],
        "total_questions": row[3],
        "access": access,
        "paper_code": paper_code,
        "paper_name": paper_name,
        "title": row[4],
        "subject": row[5],
        "board": _normalize_board(row[6]),
        "difficulty": row[7],
        "sort_order": row[8],
    }


def get_math_tests(user):
    conn = get_connection()
    cur = conn.cursor()

    email = user.get("sub") if user else None
    member_id = user.get("member_id") if user else None

    if not email:
        cur.close()
        conn.close()
        return []

    # 🔓 Admin / UAT bypass
    if email in ["rishi@test.com", "testrishi@gmail.com"]:
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
        rows = cur.fetchall()

        cur.close()
        conn.close()

        return [_build_math_test_response(r, "full") for r in rows]

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

    cur.close()
    conn.close()

    final_tests = []

    # Step 4: build final response
    for test in all_tests:
        test_id = test[0]
        access = "full" if test_id in purchased_tests else "locked"
        final_tests.append(_build_math_test_response(test, access))

    return final_tests


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
            SELECT id, correct_option
            FROM math_questions
            WHERE paper_code = %s
            """,
            (paper_code,),
        )
        rows = cur.fetchall()

        correct_answers = {str(question_id): correct for question_id, correct in rows}
        total = len(correct_answers)
        score = 0

        for raw_question_id, selected in (answers or {}).items():
            question_id = str(raw_question_id).removeprefix("q")
            correct = correct_answers.get(question_id)
            if correct and str(selected).strip().upper() == str(correct).strip().upper():
                score += 1

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
        }
    finally:
        cur.close()
        conn.close()

from app.database import get_connection


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
                total_questions
            FROM math_test_papers
            WHERE is_active = TRUE
            ORDER BY paper_code;
        """
        )
        rows = cur.fetchall()

        cur.close()
        conn.close()

        return [
            {
                "test_id": r[0],
                "name": r[1],
                "duration": r[2],
                "total_questions": r[3],
                "access": "full",
            }
            for r in rows
        ]

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
            total_questions
        FROM math_test_papers
        WHERE is_active = TRUE
        ORDER BY paper_code;
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
        final_tests.append(
            {
                "test_id": test_id,
                "name": test[1],
                "duration": test[2],
                "total_questions": test[3],
                "access": access,
            }
        )

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


def submit_math_test(answers):
    score = 0
    total = len(answers)

    for a in answers:
        if a["selected_option"] == a["correct_option"]:
            score += 1

    return {"score": score, "total": total}

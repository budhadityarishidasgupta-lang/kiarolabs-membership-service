from app.database import get_connection


def get_math_tests(user):
    conn = get_connection()
    cur = conn.cursor()

    email = user.get("sub") if user else None

    if not email:
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
            }
            for r in rows
        ]

    # 🔒 Default: free tests only
    cur.execute(
        """
        SELECT
            paper_code,
            paper_name,
            duration_minutes,
            total_questions
        FROM math_test_papers
        WHERE is_active = TRUE
          AND is_free = TRUE
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
        }
        for r in rows
    ]


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

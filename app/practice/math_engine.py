from app.database import get_connection


def get_math_lessons():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            lesson_name,
            display_name,
            topic,
            difficulty
        FROM math_lessons
        WHERE is_active = TRUE
        ORDER BY id;
    """)

    rows = cur.fetchall()

    lessons = []
    for r in rows:
        lessons.append({
            "lesson_id": r[0],
            "lesson_name": r[1],
            "display_name": r[2],
            "topic": r[3],
            "difficulty": r[4],
        })

    cur.close()
    conn.close()

    return lessons


def get_math_question(lesson_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            q.id,
            q.stem,
            q.option_a,
            q.option_b,
            q.option_c,
            q.option_d,
            q.option_e,
            q.correct_option,
            COALESCE(q.hint, '') AS hint,
            COALESCE(q.explanation, '') AS explanation
        FROM math_questions q
        JOIN math_lesson_questions lq
            ON lq.question_id = q.id
        WHERE lq.lesson_id = %s
        ORDER BY RANDOM()
        LIMIT 1;
    """, (lesson_id,))

    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        return None

    return {
        "question_id": row[0],
        "stem": row[1],
        "options": [
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
        ],
        "correct_option": row[7],
        "hint": row[8],
        "explanation": row[9],
    }


def submit_math_answer(student_id, lesson_id, question_id, selected_option):
    from app.database import get_connection

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            correct_option,
            option_a,
            option_b,
            option_c,
            option_d,
            option_e
        FROM math_questions
        WHERE id = %s
    """, (question_id,))

    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return {"error": "Question not found"}

    correct_option, option_a, option_b, option_c, option_d, option_e = row

    options_map = {
        "A": option_a,
        "B": option_b,
        "C": option_c,
        "D": option_d,
        "E": option_e,
    }

    normalized_selected = None

    if isinstance(selected_option, str):
        raw = selected_option.strip()

        # Case 1: frontend already sends A/B/C/D/E
        if raw in options_map:
            normalized_selected = raw
        else:
            # Case 2: frontend sends full option text
            for key, value in options_map.items():
                if value is not None and str(value).strip() == raw:
                    normalized_selected = key
                    break

    if normalized_selected is None:
        cur.close()
        conn.close()
        return {
            "error": "Invalid selected option"
        }

    is_correct = (normalized_selected == correct_option)

    cur.execute("""
        INSERT INTO math_attempts
        (student_id, question_id, lesson_id, selected_option, is_correct, created_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
    """, (student_id, question_id, lesson_id, normalized_selected, is_correct))

    conn.commit()
    cur.close()
    conn.close()

    return {
        "correct": is_correct,
        "correct_option": correct_option
    }

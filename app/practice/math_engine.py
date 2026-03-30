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
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT correct_option
        FROM math_questions
        WHERE id = %s
    """, (question_id,))

    correct = cur.fetchone()[0]

    is_correct = selected_option == correct

    cur.execute("""
        INSERT INTO math_attempts
        (student_id, question_id, lesson_id, selected_option, is_correct, created_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
    """, (student_id, question_id, lesson_id, selected_option, is_correct))

    conn.commit()
    cur.close()
    conn.close()

    return {
        "correct": is_correct,
        "correct_option": correct,
    }

from datetime import datetime
import re

from app.database import get_connection
from app.repositories.math_stats_repository import ensure_math_stats_table, update_math_stats_from_attempt


def _math_lesson_visibility_sql(table_alias: str = "") -> str:
    prefix = f"{table_alias}." if table_alias else ""
    return (
        f"NOT ("
        f"COALESCE({prefix}lesson_name, '') ~* '^\\s*e2e\\b' OR "
        f"COALESCE({prefix}display_name, '') ~* '^\\s*e2e\\b' OR "
        f"COALESCE({prefix}topic, '') ~* '^\\s*e2e\\b'"
        f")"
    )


def get_math_lessons_list():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            f"""
            SELECT
                id,
                lesson_name,
                display_name,
                topic,
                difficulty
            FROM math_lessons
            WHERE is_active = TRUE
              AND {_math_lesson_visibility_sql()}
            ORDER BY id;
            """
        )

        rows = cur.fetchall()
        return [
            {
                "lesson_id": row[0],
                "lesson_name": row[1],
                "display_name": row[2],
                "topic": row[3],
                "difficulty": row[4],
            }
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()


def _map_question_rows(rows) -> list[dict]:
    return [
        {
            "question_id": row[0],
            "stem": row[1],
            "option_a": row[2],
            "option_b": row[3],
            "option_c": row[4],
            "option_d": row[5],
            "correct_option": row[6],
            "times_seen": row[7],
            "times_correct": row[8],
            "times_wrong": row[9],
            "accuracy": float(row[10] or 0),
            "last_seen_at": row[11],
            "is_weak": bool(row[12]),
        }
        for row in rows
    ]


def _get_lesson_details(cur, lesson_id: int):
    cur.execute(
        f"""
        SELECT
            id,
            lesson_name,
            display_name,
            topic,
            difficulty
        FROM math_lessons
        WHERE id = %s
          AND {_math_lesson_visibility_sql()}
        LIMIT 1
        """,
        (lesson_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "lesson_id": row[0],
        "lesson_name": row[1],
        "display_name": row[2],
        "topic": row[3],
        "difficulty": row[4],
    }


def _fetch_questions_for_filter(cur, user_id: int, where_sql: str, params: tuple) -> list[dict]:
    cur.execute(
        f"""
        SELECT
            q.id,
            q.stem,
            q.option_a,
            q.option_b,
            q.option_c,
            q.option_d,
            q.correct_option,
            COALESCE(s.times_seen, 0) AS times_seen,
            COALESCE(s.times_correct, 0) AS times_correct,
            COALESCE(s.times_wrong, 0) AS times_wrong,
            COALESCE(s.accuracy, 0) AS accuracy,
            s.last_seen_at,
            COALESCE(s.is_weak, FALSE) AS is_weak
        FROM math_questions q
        LEFT JOIN math_question_stats s
            ON s.question_id = q.id
           AND s.user_id = %s
        WHERE {where_sql}
        ORDER BY q.id ASC
        """,
        (user_id, *params),
    )

    return _map_question_rows(cur.fetchall())


def _extract_lesson_keywords(lesson: dict) -> list[str]:
    raw_parts = [
        lesson.get("display_name") or "",
        lesson.get("lesson_name") or "",
        lesson.get("topic") or "",
    ]
    raw_text = " ".join(part for part in raw_parts if part)
    tokens = re.findall(r"[A-Za-z0-9]+", raw_text.lower())
    stop_words = {"with", "and", "the", "same", "into", "than"}
    keywords = []

    for token in tokens:
        if token in stop_words:
            continue
        if len(token) < 3:
            continue
        if token not in keywords:
            keywords.append(token)

    return keywords


def _fetch_lesson_questions(cur, user_id: int, lesson_id: int) -> list[dict]:
    lesson = _get_lesson_details(cur, lesson_id)
    if not lesson:
        return []

    topic = (lesson.get("topic") or "").strip()
    difficulty = (lesson.get("difficulty") or "").strip()

    if topic and difficulty:
        rows = _fetch_questions_for_filter(
            cur,
            user_id,
            "LOWER(COALESCE(q.topic, '')) = LOWER(%s) AND LOWER(COALESCE(q.difficulty, '')) = LOWER(%s)",
            (topic, difficulty),
        )
        if rows:
            return rows

    if topic:
        rows = _fetch_questions_for_filter(
            cur,
            user_id,
            "LOWER(COALESCE(q.topic, '')) = LOWER(%s)",
            (topic,),
        )
        if rows:
            return rows

    if difficulty:
        rows = _fetch_questions_for_filter(
            cur,
            user_id,
            "LOWER(COALESCE(q.difficulty, '')) = LOWER(%s)",
            (difficulty,),
        )
        if rows:
            return rows

    keywords = _extract_lesson_keywords(lesson)
    if keywords:
        keyword_clauses = []
        keyword_params = []
        for keyword in keywords:
            like_value = f"%{keyword}%"
            keyword_clauses.append(
                "(LOWER(COALESCE(q.topic, '')) LIKE LOWER(%s) OR LOWER(COALESCE(q.stem, '')) LIKE LOWER(%s))"
            )
            keyword_params.extend([like_value, like_value])

        rows = _fetch_questions_for_filter(
            cur,
            user_id,
            " OR ".join(keyword_clauses),
            tuple(keyword_params),
        )
        if rows:
            return rows

    return []


def _get_latest_question_id(cur, user_id: int, lesson_id: int):
    cur.execute(
        """
        SELECT question_id
        FROM math_attempts
        WHERE student_id = %s
          AND lesson_id = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (user_id, lesson_id),
    )
    row = cur.fetchone()
    return row[0] if row else None


def _avoid_immediate_repeat(items: list[dict], last_question_id):
    if len(items) <= 1 or not last_question_id:
        return items
    filtered = [item for item in items if item["question_id"] != last_question_id]
    return filtered or items


def _sort_last_seen(value):
    return value if value is not None else datetime.min


def get_math_next_question(user_id: int, lesson_id_or_scope: int):
    ensure_math_stats_table()
    conn = get_connection()
    cur = conn.cursor()

    try:
        items = _fetch_lesson_questions(cur, user_id, lesson_id_or_scope)
        if not items:
            return None

        last_question_id = _get_latest_question_id(cur, user_id, lesson_id_or_scope)

        unseen = [item for item in items if item["times_seen"] == 0]
        weak = [item for item in items if item["is_weak"] or item["times_wrong"] > 0]
        review = [item for item in items if item["times_seen"] > 0 and item not in weak]

        unseen = sorted(unseen, key=lambda item: item["question_id"])
        weak = sorted(weak, key=lambda item: (item["accuracy"], _sort_last_seen(item["last_seen_at"]), item["question_id"]))
        review = sorted(review, key=lambda item: (item["accuracy"], _sort_last_seen(item["last_seen_at"]), item["question_id"]))

        for pool in (unseen, weak, review):
            candidate_pool = _avoid_immediate_repeat(pool, last_question_id)
            if candidate_pool:
                return candidate_pool[0]

        return items[0]
    finally:
        cur.close()
        conn.close()


def get_math_question_record(question_id: int):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                id,
                stem,
                option_a,
                option_b,
                option_c,
                option_d,
                option_e,
                topic,
                difficulty,
                correct_option
            FROM math_questions
            WHERE id = %s
            LIMIT 1
            """,
            (question_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "question_id": row[0],
            "stem": row[1],
            "option_a": row[2],
            "option_b": row[3],
            "option_c": row[4],
            "option_d": row[5],
            "option_e": row[6],
            "topic": row[7],
            "difficulty": row[8],
            "correct_option": row[9],
        }
    finally:
        cur.close()
        conn.close()


def record_math_attempt(
    user_id: int,
    lesson_id: int,
    question_id: int,
    correct: bool,
    selected_option: str,
    session_id: str | None = None,
):
    question = get_math_question_record(question_id)
    if not question:
        return

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO math_attempts
            (
                student_id,
                question_id,
                lesson_id,
                selected_option,
                is_correct,
                created_at,
                attempted_at,
                session_id,
                submitted_at,
                contract_version
            )
            VALUES (%s, %s, %s, %s, %s, NOW(), NOW(), %s, NOW(), %s)
            """,
            (
                user_id,
                question_id,
                lesson_id,
                selected_option,
                correct,
                session_id,
                "v1",
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    update_math_stats_from_attempt(user_id, question_id, correct)

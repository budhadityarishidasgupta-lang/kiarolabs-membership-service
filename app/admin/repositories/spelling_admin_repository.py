import logging
import re

from app.database import get_connection


logger = logging.getLogger(__name__)

CANONICAL_SPELLING_LESSON_RE = re.compile(r'^"[A-Z/\' ]+" pattern Words$')
BAD_SPELLING_LESSON_RE = re.compile(r"^\s*(?:lx-p\d+|\d+|patterns?|spelling patterns?)\s*$", re.IGNORECASE)
PATTERN_EXTRACTION_RULES = (
    ("SSION", lambda word: word.endswith("ssion")),
    ("TION", lambda word: word.endswith("tion")),
    ("SION", lambda word: word.endswith("sion")),
    ("CION", lambda word: word.endswith("cion")),
    ("URE", lambda word: word.endswith("ure")),
    ("PH", lambda word: "ph" in word),
    ("CH", lambda word: "ch" in word),
    ("GH", lambda word: "gh" in word),
    ("DGE", lambda word: "dge" in word),
    ("TCH", lambda word: "tch" in word),
    ("CK", lambda word: "ck" in word),
    ("WR", lambda word: "wr" in word),
    ("KN", lambda word: "kn" in word),
    ("GN", lambda word: "gn" in word),
    ("MB", lambda word: word.endswith("mb")),
    ("OUGH", lambda word: "ough" in word),
)


def _normalize_pattern_token(value: str) -> str:
    cleaned = (value or "").strip().upper().replace('"', "")
    cleaned = re.sub(r"\s+", "", cleaned)
    cleaned = re.sub(r"[^A-Z/']", "", cleaned)
    return cleaned


def _canonical_spelling_lesson_name(pattern: str) -> str:
    return f'"{pattern}" pattern Words'


def _extract_pattern_from_text(value: str) -> str | None:
    raw = (value or "").strip()
    if not raw:
        return None

    quoted = re.search(r'"([^"]+)"', raw)
    if quoted:
        normalized = _normalize_pattern_token(quoted.group(1))
        if normalized:
            return normalized

    tokenized = re.findall(r"[A-Za-z/']+", raw)
    for token in tokenized:
        normalized = _normalize_pattern_token(token)
        if normalized in {rule[0] for rule in PATTERN_EXTRACTION_RULES}:
            return normalized

    lowered = raw.lower()
    if BAD_SPELLING_LESSON_RE.match(raw):
        return None

    for pattern, matcher in PATTERN_EXTRACTION_RULES:
        if matcher(lowered):
            return pattern
    return None


def _coerce_spelling_lesson_name(value: str) -> str:
    pattern = _extract_pattern_from_text(value)
    if not pattern:
        logger.warning("spelling_lesson_name_unassigned: %r", value)
        return "UNASSIGNED"

    lesson_name = _canonical_spelling_lesson_name(pattern)
    if not CANONICAL_SPELLING_LESSON_RE.match(lesson_name):
        raise ValueError("lesson_name failed canonical spelling lesson validation")
    return lesson_name


def _fetch_existing_spelling_lesson(cur, course_id: int, canonical_lesson_name: str):
    cur.execute(
        """
        SELECT lesson_id, course_id, lesson_name, COALESCE(display_name, lesson_name), COALESCE(sort_order, 0), COALESCE(is_active, TRUE)
        FROM spelling_lessons
        WHERE course_id = %s
          AND lesson_name = %s
        LIMIT 1
        """,
        (course_id, canonical_lesson_name),
    )
    return cur.fetchone()


def get_spelling_overview():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT COUNT(DISTINCT c.course_id)
            FROM spelling_courses c
            JOIN spelling_lessons l
                ON l.course_id = c.course_id
            WHERE COALESCE(l.is_active, TRUE) = TRUE
            """
        )
        course_count = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COUNT(*)
            FROM spelling_lessons
            WHERE COALESCE(is_active, TRUE) = TRUE
            """
        )
        lesson_count = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COUNT(DISTINCT li.word_id)
            FROM spelling_lesson_items li
            JOIN spelling_lessons l
                ON l.lesson_id = li.lesson_id
            WHERE COALESCE(l.is_active, TRUE) = TRUE
            """
        )
        item_count = cur.fetchone()[0]

        return {
            "module": "spelling",
            "label": "Spelling",
            "supports_courses": True,
            "course_count": course_count,
            "lesson_count": lesson_count,
            "item_count": item_count,
        }
    finally:
        cur.close()
        conn.close()


def list_spelling_courses():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                c.course_id,
                c.course_name,
                COUNT(DISTINCT l.lesson_id) AS lesson_count
            FROM spelling_courses c
            LEFT JOIN spelling_lessons l
                ON l.course_id = c.course_id
               AND COALESCE(l.is_active, TRUE) = TRUE
            GROUP BY c.course_id, c.course_name
            ORDER BY c.course_id ASC
            """
        )
        rows = cur.fetchall()

        return [
            {
                "course_id": row[0],
                "course_name": row[1],
                "lesson_count": row[2],
            }
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()


def create_spelling_course(course_name: str):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO spelling_courses (course_name)
            VALUES (%s)
            RETURNING course_id, course_name
            """,
            (course_name.strip(),),
        )
        row = cur.fetchone()
        conn.commit()
        return {
            "course_id": row[0],
            "course_name": row[1],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def list_spelling_lessons(course_id: int | None = None):
    conn = get_connection()
    cur = conn.cursor()

    try:
        params = []
        where_sql = ""
        if course_id is not None:
            where_sql = "WHERE l.course_id = %s AND COALESCE(l.is_active, TRUE) = TRUE"
            params.append(course_id)
        else:
            where_sql = "WHERE COALESCE(l.is_active, TRUE) = TRUE"

        cur.execute(
            f"""
            SELECT
                l.lesson_id,
                l.course_id,
                c.course_name,
                l.lesson_name,
                COALESCE(l.display_name, l.lesson_name) AS display_name,
                COALESCE(l.sort_order, 0) AS sort_order,
                COALESCE(l.is_active, TRUE) AS is_active,
                COUNT(DISTINCT li.word_id) AS item_count
            FROM spelling_lessons l
            JOIN spelling_courses c
                ON c.course_id = l.course_id
            LEFT JOIN spelling_lesson_items li
                ON li.lesson_id = l.lesson_id
            {where_sql}
            GROUP BY
                l.lesson_id,
                l.course_id,
                c.course_name,
                l.lesson_name,
                l.display_name,
                l.sort_order,
                l.is_active
            ORDER BY l.course_id ASC, COALESCE(l.sort_order, 0) ASC, l.lesson_id ASC
            """,
            tuple(params),
        )
        rows = cur.fetchall()

        return [
            {
                "lesson_id": row[0],
                "course_id": row[1],
                "course_name": row[2],
                "lesson_name": row[3],
                "display_name": row[4],
                "sort_order": row[5],
                "is_active": row[6],
                "item_count": row[7],
            }
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()


def create_spelling_lesson(
    course_id: int,
    lesson_name: str,
    display_name: str | None = None,
    is_active: bool = True,
):
    conn = get_connection()
    cur = conn.cursor()

    try:
        canonical_lesson_name = _coerce_spelling_lesson_name(lesson_name)
        canonical_display_name = (
            canonical_lesson_name if canonical_lesson_name == "UNASSIGNED" else (display_name or "").strip() or canonical_lesson_name
        )

        existing_row = _fetch_existing_spelling_lesson(cur, course_id, canonical_lesson_name)
        if existing_row:
            conn.rollback()
            return {
                "lesson_id": existing_row[0],
                "course_id": existing_row[1],
                "lesson_name": existing_row[2],
                "display_name": existing_row[3],
                "sort_order": existing_row[4],
                "is_active": existing_row[5],
            }

        cur.execute(
            """
            SELECT COALESCE(MAX(sort_order), 0) + 1
            FROM spelling_lessons
            WHERE course_id = %s
            """,
            (course_id,),
        )
        sort_order = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO spelling_lessons (course_id, lesson_name, display_name, sort_order, is_active)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING lesson_id, course_id, lesson_name, COALESCE(display_name, lesson_name), sort_order, is_active
            """,
            (course_id, canonical_lesson_name, canonical_display_name, sort_order, is_active),
        )
        row = cur.fetchone()
        conn.commit()
        return {
            "lesson_id": row[0],
            "course_id": row[1],
            "lesson_name": row[2],
            "display_name": row[3],
            "sort_order": row[4],
            "is_active": row[5],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def update_spelling_lesson(
    lesson_id: int,
    *,
    lesson_name: str,
    display_name: str | None = None,
):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cleaned_lesson_name = _coerce_spelling_lesson_name(lesson_name)
        cleaned_display_name = (
            cleaned_lesson_name if cleaned_lesson_name == "UNASSIGNED" else ((display_name or "").strip() or cleaned_lesson_name)
        )

        if not cleaned_lesson_name:
            raise ValueError("lesson_name is required")

        cur.execute(
            """
            UPDATE spelling_lessons
            SET lesson_name = %s,
                display_name = %s
            WHERE lesson_id = %s
            RETURNING lesson_id, course_id, lesson_name, COALESCE(display_name, lesson_name), COALESCE(sort_order, 0), COALESCE(is_active, TRUE)
            """,
            (cleaned_lesson_name, cleaned_display_name, lesson_id),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None

        cur.execute(
            """
            SELECT course_name
            FROM spelling_courses
            WHERE course_id = %s
            """,
            (row[1],),
        )
        course_row = cur.fetchone()

        cur.execute(
            """
            SELECT COUNT(DISTINCT word_id)
            FROM spelling_lesson_items
            WHERE lesson_id = %s
            """,
            (lesson_id,),
        )
        item_count = cur.fetchone()[0]

        conn.commit()
        return {
            "lesson_id": row[0],
            "course_id": row[1],
            "course_name": course_row[0] if course_row else None,
            "lesson_name": row[2],
            "display_name": row[3],
            "sort_order": row[4],
            "is_active": row[5],
            "item_count": item_count,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def list_spelling_lesson_content(lesson_id: int):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                l.lesson_id,
                l.lesson_name,
                COALESCE(l.display_name, l.lesson_name) AS display_name
            FROM spelling_lessons l
            WHERE l.lesson_id = %s
            LIMIT 1
            """,
            (lesson_id,),
        )
        lesson_row = cur.fetchone()
        if not lesson_row:
            return None

        cur.execute(
            """
            SELECT
                w.word_id,
                COALESCE(w.example_sentence, '') AS prompt,
                w.word
            FROM spelling_lesson_items li
            JOIN spelling_lessons l
                ON l.lesson_id = li.lesson_id
            JOIN spelling_words w
                ON w.word_id = li.word_id
            WHERE li.lesson_id = %s
              AND COALESCE(l.is_active, TRUE) = TRUE
            ORDER BY w.word_id ASC
            """,
            (lesson_id,),
        )
        rows = cur.fetchall()

        items = [
            {
                "item_id": row[0],
                "prompt": row[1].strip() or f"Word #{row[0]}",
                "answer": row[2],
            }
            for row in rows
        ]

        return {
            "lesson_id": lesson_row[0],
            "lesson_name": lesson_row[1],
            "display_name": lesson_row[2],
            "items": items,
        }
    finally:
        cur.close()
        conn.close()


def update_spelling_content_answer(item_id: int, answer: str):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cleaned_answer = (answer or "").strip()
        if not cleaned_answer:
            raise ValueError("answer is required")

        cur.execute(
            """
            UPDATE spelling_words
            SET word = %s
            WHERE word_id = %s
            RETURNING word_id, COALESCE(example_sentence, ''), word
            """,
            (cleaned_answer, item_id),
        )
        row = cur.fetchone()
        if not row:
            return None
        conn.commit()
        return {
            "item_id": row[0],
            "prompt": row[1].strip() or f"Word #{row[0]}",
            "answer": row[2],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

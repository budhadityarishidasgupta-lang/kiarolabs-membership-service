"""
MathSprint Admin — Practice CSV ingest + export.
Ported from english-spelling-trainer for use in kiarolabs-membership-service.
No schema changes. Idempotent upserts only.
"""
import io
import json
from typing import BinaryIO, Dict

import pandas as pd
import psycopg2.extras

from app.database import get_connection


def _parse_geometry_schema(raw: str):
    """Parse geometry_schema CSV cell into psycopg2.extras.Json or None."""
    if not raw or not str(raw).strip():
        return None
    try:
        parsed = json.loads(raw)
        return psycopg2.extras.Json(parsed)
    except (json.JSONDecodeError, TypeError):
        return None


REQUIRED_COLUMNS = [
    "question_id",
    "topic",
    "difficulty",
    "stem",
    "option_a",
    "option_b",
    "option_c",
    "option_d",
    "correct_option",
]
OPTIONAL_COLUMNS = ["option_e", "explanation", "hint", "geometry_schema"]

TEMPLATE_COLUMNS = [
    "question_id", "topic", "difficulty", "stem",
    "option_a", "option_b", "option_c", "option_d",
    "correct_option", "option_e", "explanation", "hint",
    "geometry_schema",
]

TEMPLATE_EXAMPLE = {
    "question_id": "MB-FRAC-001",
    "topic": "Fractions",
    "difficulty": "Core",
    "stem": "What is 1/2 + 1/4?",
    "option_a": "1/4",
    "option_b": "3/4",
    "option_c": "1",
    "option_d": "2/4",
    "correct_option": "B",
    "option_e": "",
    "explanation": "1/2 = 2/4, so 2/4 + 1/4 = 3/4.",
    "hint": "Convert to same denominator first.",
    "geometry_schema": "",
}


def _norm_topic_to_lesson_name(topic: str) -> str:
    import re
    s = (topic or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "untitled"


def build_blank_template_csv() -> bytes:
    df = pd.DataFrame([TEMPLATE_EXAMPLE], columns=TEMPLATE_COLUMNS)
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


def export_lesson_csv(lesson_id: int) -> bytes:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                q.question_id,
                q.topic,
                q.difficulty,
                q.stem,
                q.option_a,
                q.option_b,
                q.option_c,
                q.option_d,
                COALESCE(q.option_e, '') AS option_e,
                q.correct_option,
                COALESCE(q.explanation, '') AS explanation,
                COALESCE(q.hint, '') AS hint,
                COALESCE(q.geometry_schema::text, '') AS geometry_schema
            FROM math_questions q
            JOIN math_lesson_questions mlq ON mlq.question_id = q.id
            WHERE mlq.lesson_id = %s
            ORDER BY mlq.position, q.id
            """,
            (lesson_id,),
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    cols = [
        "question_id", "topic", "difficulty", "stem",
        "option_a", "option_b", "option_c", "option_d",
        "option_e", "correct_option", "explanation", "hint",
        "geometry_schema",
    ]
    df = pd.DataFrame(rows, columns=cols)
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


def ingest_math_practice_csv(file_obj: BinaryIO, *, course_id: int = 1) -> Dict[str, int]:
    """
    Idempotent ingestion for MathSprint practice CSVs.
    Upserts lessons (by topic->lesson_name), questions (by question_id),
    and lesson<->question mappings. Never deletes.
    """
    try:
        content = file_obj.read()
        text = content.decode("utf-8-sig")
    except Exception as exc:
        raise ValueError(f"Could not read CSV: {exc}") from exc

    df = pd.read_csv(io.StringIO(text))
    df.columns = [c.strip() for c in df.columns]

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    for col in OPTIONAL_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df["question_id"] = df["question_id"].astype(str).str.strip()
    df["topic"] = df["topic"].astype(str).str.strip()
    df["difficulty"] = df["difficulty"].astype(str).str.strip()
    df["stem"] = df["stem"].astype(str).str.strip()
    df["correct_option"] = df["correct_option"].astype(str).str.strip().str.upper()

    lessons_seen: set = set()
    questions_upserted = 0
    mappings_created = 0

    conn = get_connection()
    cur = conn.cursor()
    try:
        def upsert_lesson(topic: str) -> int:
            lesson_name = _norm_topic_to_lesson_name(topic)
            display_name = topic.strip()
            lessons_seen.add(lesson_name)
            cur.execute(
                """
                INSERT INTO math_lessons (course_id, lesson_code, lesson_name, display_name, is_active)
                VALUES (%s, %s, %s, %s, TRUE)
                ON CONFLICT (course_id, lesson_name)
                DO UPDATE SET display_name = EXCLUDED.display_name
                RETURNING id;
                """,
                (course_id, lesson_name, lesson_name, display_name),
            )
            return cur.fetchone()[0]

        def upsert_question(row: dict) -> int:
            nonlocal questions_upserted
            correct = row["correct_option"]
            if correct not in {"A", "B", "C", "D", "E"}:
                raise ValueError(f"Invalid correct_option '{correct}' for question_id={row['question_id']}")
            cur.execute(
                """
                INSERT INTO math_questions (
                    question_id, stem, option_a, option_b, option_c, option_d, option_e,
                    correct_option, topic, difficulty, explanation, hint, geometry_schema
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (question_id)
                DO UPDATE SET
                    stem = EXCLUDED.stem,
                    option_a = EXCLUDED.option_a,
                    option_b = EXCLUDED.option_b,
                    option_c = EXCLUDED.option_c,
                    option_d = EXCLUDED.option_d,
                    option_e = EXCLUDED.option_e,
                    correct_option = EXCLUDED.correct_option,
                    topic = EXCLUDED.topic,
                    difficulty = EXCLUDED.difficulty,
                    explanation = EXCLUDED.explanation,
                    hint = EXCLUDED.hint,
                    geometry_schema = EXCLUDED.geometry_schema
                RETURNING id;
                """,
                (
                    row["question_id"], row["stem"],
                    row["option_a"], row["option_b"], row["option_c"], row["option_d"],
                    row.get("option_e", ""), correct,
                    row["topic"], row["difficulty"],
                    row.get("explanation", ""), row.get("hint", ""),
                    _parse_geometry_schema(row.get("geometry_schema", "")),
                ),
            )
            questions_upserted += 1
            return cur.fetchone()[0]

        def ensure_mapping(lesson_id: int, question_id: int, position: int) -> None:
            nonlocal mappings_created
            cur.execute(
                """
                INSERT INTO math_lesson_questions (lesson_id, question_id, position)
                VALUES (%s, %s, %s)
                ON CONFLICT (lesson_id, question_id)
                DO UPDATE SET position = EXCLUDED.position;
                """,
                (lesson_id, question_id, position),
            )
            mappings_created += 1

        for idx, r in df.iterrows():
            row = {c: str(r.get(c, "")) for c in df.columns}
            if not row["question_id"]:
                continue
            lesson_id = upsert_lesson(row["topic"])
            question_id = upsert_question(row)
            ensure_mapping(lesson_id, question_id, idx + 1)

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    return {
        "lessons_processed": len(lessons_seen),
        "questions_upserted": questions_upserted,
        "mappings_processed": mappings_created,
    }

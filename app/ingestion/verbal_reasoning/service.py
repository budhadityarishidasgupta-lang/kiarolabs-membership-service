from __future__ import annotations

import csv
import io

from fastapi import HTTPException, UploadFile

from app.database import get_connection
from app.ingestion.verbal_reasoning.parser import VrReviewRow, convert_vr_pdf_to_review_rows
from app.repositories.vr_repository import insert_vr_answers_bulk, insert_vr_questions_bulk


DEFAULT_VR_PAPERS = [
    ("vr-01", "Verbal Reasoning Practice Paper 01", 1),
    ("vr-02", "Verbal Reasoning Practice Paper 02", 2),
    ("vr-03", "Verbal Reasoning Practice Paper 03", 3),
    ("vr-04", "Verbal Reasoning Practice Paper 04", 4),
    ("vr-05", "Verbal Reasoning Practice Paper 05", 5),
    ("vr-06", "Verbal Reasoning Practice Paper 06", 6),
    ("vr-07", "Verbal Reasoning Practice Paper 07", 7),
    ("vr-08", "Verbal Reasoning Practice Paper 08", 8),
    ("vr-09", "Verbal Reasoning Practice Paper 09", 9),
    ("vr-10", "Verbal Reasoning Practice Paper 10", 10),
]


def init_verbal_reasoning_printable_tables():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS verbal_reasoning_printable_papers (
                paper_code TEXT PRIMARY KEY,
                paper_name TEXT NOT NULL,
                sort_order INTEGER NOT NULL DEFAULT 0,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS verbal_reasoning_printable_questions (
                paper_code TEXT NOT NULL REFERENCES verbal_reasoning_printable_papers(paper_code) ON DELETE CASCADE,
                question_number INTEGER NOT NULL,
                section_title TEXT,
                question_type TEXT,
                question_text TEXT NOT NULL,
                option_a TEXT,
                option_b TEXT,
                option_c TEXT,
                option_d TEXT,
                option_e TEXT,
                source_block TEXT,
                notes TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (paper_code, question_number)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS verbal_reasoning_printable_answer_keys (
                paper_code TEXT NOT NULL REFERENCES verbal_reasoning_printable_papers(paper_code) ON DELETE CASCADE,
                question_number INTEGER NOT NULL,
                correct_answer TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (paper_code, question_number)
            )
            """
        )

        cur.executemany(
            """
            INSERT INTO verbal_reasoning_printable_papers (paper_code, paper_name, sort_order)
            VALUES (%s, %s, %s)
            ON CONFLICT (paper_code) DO UPDATE
            SET paper_name = EXCLUDED.paper_name,
                sort_order = EXCLUDED.sort_order,
                updated_at = NOW()
            """,
            DEFAULT_VR_PAPERS,
        )

        conn.commit()
    finally:
        cur.close()
        conn.close()


def import_verbal_reasoning_pdf_as_draft(pdf_path: str, paper_code: str | None = None) -> dict:
    rows = convert_vr_pdf_to_review_rows(pdf_path, paper_code)
    if not rows:
        return {"paper_code": paper_code or "vr-01", "questions_imported": 0, "answers_deleted": 0}

    resolved_paper_code = rows[0].paper_code
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            DELETE FROM verbal_reasoning_printable_answer_keys
            WHERE paper_code = %s
            """,
            (resolved_paper_code,),
        )
        answers_deleted = cur.rowcount

        for row in rows:
            notes = row.notes
            if row.review_flags:
                notes = f"{row.notes} Flags: {row.review_flags}"

            cur.execute(
                """
                INSERT INTO verbal_reasoning_printable_questions
                (
                    paper_code,
                    question_number,
                    section_title,
                    question_type,
                    question_text,
                    option_a,
                    option_b,
                    option_c,
                    option_d,
                    option_e,
                    source_block,
                    notes,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (paper_code, question_number)
                DO UPDATE SET
                    section_title = EXCLUDED.section_title,
                    question_type = EXCLUDED.question_type,
                    question_text = EXCLUDED.question_text,
                    option_a = EXCLUDED.option_a,
                    option_b = EXCLUDED.option_b,
                    option_c = EXCLUDED.option_c,
                    option_d = EXCLUDED.option_d,
                    option_e = EXCLUDED.option_e,
                    source_block = EXCLUDED.source_block,
                    notes = EXCLUDED.notes,
                    updated_at = NOW()
                """,
                (
                    resolved_paper_code,
                    row.question_number,
                    row.section_title,
                    row.question_type,
                    row.question_text,
                    row.option_a or None,
                    row.option_b or None,
                    row.option_c or None,
                    row.option_d or None,
                    row.option_e or None,
                    row.source_block,
                    notes,
                ),
            )

        conn.commit()
        return {
            "paper_code": resolved_paper_code,
            "questions_imported": len(rows),
            "answers_deleted": answers_deleted,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def upload_verbal_reasoning_answer_csv(file: UploadFile, selected_paper_code: str | None = None) -> dict:
    try:
        content = file.file.read().decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail="CSV must be UTF-8 encoded") from exc

    reader = csv.DictReader(io.StringIO(content))
    required_fields = {"paper_code", "question_number", "correct_answer"}
    if not reader.fieldnames:
        raise HTTPException(status_code=400, detail="CSV has no header row")

    headers = {field.strip() for field in reader.fieldnames if field}
    missing = sorted(required_fields - headers)
    if missing:
        raise HTTPException(status_code=400, detail=f"CSV missing required columns: {', '.join(missing)}")

    conn = get_connection()
    cur = conn.cursor()

    question_rows = []
    answer_rows = []
    seen_pairs: dict[tuple[str, int], str] = {}

    try:
        for idx, row in enumerate(reader, start=2):
            clean = {
                (key.strip() if key else key): (value.strip() if isinstance(value, str) else value)
                for key, value in row.items()
            }

            paper_code = str(clean.get("paper_code") or "").strip().lower()
            question_number_raw = str(clean.get("question_number") or "").strip()
            correct_answer = str(clean.get("correct_answer") or "").strip().upper()

            if not paper_code or not question_number_raw or not correct_answer:
                raise HTTPException(
                    status_code=400,
                    detail=f"Row {idx}: paper_code, question_number and correct_answer are required",
                )

            if selected_paper_code and paper_code != selected_paper_code.strip().lower():
                raise HTTPException(
                    status_code=400,
                    detail=f"Row {idx}: paper_code {paper_code} does not match selected paper {selected_paper_code}",
                )

            try:
                question_number = int(question_number_raw)
            except ValueError as exc:
                raise HTTPException(
                    status_code=400,
                    detail=f"Row {idx}: question_number must be an integer",
                ) from exc

            if question_number <= 0:
                raise HTTPException(status_code=400, detail=f"Row {idx}: question_number must be positive")

            if len(correct_answer) > 32:
                raise HTTPException(status_code=400, detail=f"Row {idx}: correct_answer is too long")

            cur.execute(
                """
                SELECT 1
                FROM verbal_reasoning_printable_papers
                WHERE paper_code = %s
                """,
                (paper_code,),
            )
            if cur.fetchone() is None:
                raise HTTPException(status_code=400, detail=f"Row {idx}: invalid paper_code {paper_code}")

            pair = (paper_code, question_number)
            previous = seen_pairs.get(pair)
            if previous is not None and previous != correct_answer:
                raise HTTPException(
                    status_code=400,
                    detail=f"Row {idx}: conflicting duplicate answer for {paper_code} Q{question_number}",
                )
            if previous is not None:
                continue
            seen_pairs[pair] = correct_answer

            question_rows.append(
                {
                    "paper_code": paper_code,
                    "question_number": question_number,
                    "question_text": f"Question {question_number}",
                    "notes": "Created from admin answer CSV upload",
                }
            )
            answer_rows.append(
                {
                    "paper_code": paper_code,
                    "question_number": question_number,
                    "correct_answer": correct_answer,
                }
            )

        if not answer_rows:
            raise HTTPException(status_code=400, detail="CSV contains no valid answer rows")

        question_stats = insert_vr_questions_bulk(question_rows, conn=conn)
        answer_stats = insert_vr_answers_bulk(answer_rows, conn=conn)
        conn.commit()

        resolved_paper_codes = sorted({row["paper_code"] for row in answer_rows})
        return {
            "status": "uploaded",
            "paper_code": resolved_paper_codes[0] if len(resolved_paper_codes) == 1 else None,
            "paper_codes": resolved_paper_codes,
            "rows": len(answer_rows),
            "questions_inserted": question_stats["inserted"],
            "questions_existing": question_stats["existing"],
            "answers_inserted": answer_stats["inserted"],
            "answers_updated": answer_stats["updated"],
            "answers_unchanged": answer_stats["unchanged"],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

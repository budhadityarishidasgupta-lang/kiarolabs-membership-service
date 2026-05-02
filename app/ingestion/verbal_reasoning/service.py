from __future__ import annotations

import csv
import io
import re

from fastapi import HTTPException, UploadFile

from app.database import get_connection
from app.ingestion.verbal_reasoning.parser import VrReviewRow, convert_vr_pdf_to_review_rows
from app.repositories.vr_repository import (
    bulk_upsert_vr_answers,
    bulk_upsert_vr_questions,
    create_or_update_vr_paper,
    init_vr_tables,
    normalize_vr_paper_code,
)

VR_ANSWER_PATTERN = re.compile(r"^[A-E](?:/[A-E])*$")


def init_verbal_reasoning_printable_tables():
    init_vr_tables()


def import_verbal_reasoning_pdf_as_draft(pdf_path: str, paper_code: str | None = None) -> dict:
    rows = convert_vr_pdf_to_review_rows(pdf_path, paper_code)
    if not rows:
        return {"paper_code": paper_code or "VR-P1", "questions_imported": 0, "answers_deleted": 0}

    resolved_paper_code = rows[0].paper_code
    conn = get_connection()
    cur = conn.cursor()

    try:
        create_or_update_vr_paper(
            paper_code=resolved_paper_code,
            title=f"Verbal Reasoning Practice Paper {resolved_paper_code.split('P')[-1].zfill(2)}",
            description="Imported verbal reasoning printable paper.",
            answer_key_uploaded=False,
            conn=conn,
        )
        question_rows = []
        for row in rows:
            question_rows.append(
                {
                    "paper_code": resolved_paper_code,
                    "question_number": row.question_number,
                    "question_type": row.question_type,
                    "question_text": row.question_text,
                    "option_a": row.option_a or None,
                    "option_b": row.option_b or None,
                    "option_c": row.option_c or None,
                    "option_d": row.option_d or None,
                    "option_e": row.option_e or None,
                }
            )
        stats = bulk_upsert_vr_questions(question_rows, conn=conn)

        conn.commit()
        return {
            "paper_code": resolved_paper_code,
            "questions_imported": len(rows),
            "answers_deleted": 0,
            "questions_updated": stats["updated"],
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
    row_errors: list[dict] = []

    try:
        for idx, row in enumerate(reader, start=2):
            clean = {
                (key.strip() if key else key): (value.strip() if isinstance(value, str) else value)
                for key, value in row.items()
            }

            paper_code = normalize_vr_paper_code(str(clean.get("paper_code") or "").strip())
            question_number_raw = str(clean.get("question_number") or "").strip()
            correct_answer = str(clean.get("correct_answer") or "").strip().upper()

            if not paper_code or not question_number_raw or not correct_answer:
                row_errors.append(
                    {"row": idx, "detail": "paper_code, question_number and correct_answer are required"}
                )
                continue

            if selected_paper_code and paper_code != normalize_vr_paper_code(selected_paper_code.strip()):
                row_errors.append(
                    {"row": idx, "detail": f"paper_code {paper_code} does not match selected paper {selected_paper_code}"}
                )
                continue

            try:
                question_number = int(question_number_raw)
            except ValueError:
                row_errors.append({"row": idx, "detail": "question_number must be an integer"})
                continue

            if question_number < 1 or question_number > 80:
                row_errors.append({"row": idx, "detail": "question_number must be between 1 and 80"})
                continue

            if len(correct_answer) > 32:
                row_errors.append({"row": idx, "detail": "correct_answer is too long"})
                continue

            if not VR_ANSWER_PATTERN.match(correct_answer):
                row_errors.append(
                    {"row": idx, "detail": "correct_answer must be A-E or slash-separated multi-answer format"}
                )
                continue

            cur.execute(
                """
                SELECT 1
                FROM vr_papers
                WHERE paper_code = %s
                """,
                (paper_code,),
            )
            if cur.fetchone() is None:
                row_errors.append({"row": idx, "detail": f"invalid paper_code {paper_code}"})
                continue

            pair = (paper_code, question_number)
            previous = seen_pairs.get(pair)
            if previous is not None and previous != correct_answer:
                row_errors.append(
                    {"row": idx, "detail": f"conflicting duplicate answer for {paper_code} Q{question_number}"}
                )
                continue
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
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "CSV contains no valid answer rows",
                    "rows_failed": len(row_errors),
                    "errors": row_errors,
                },
            )

        question_stats = bulk_upsert_vr_questions(question_rows, conn=conn)
        answer_stats = bulk_upsert_vr_answers(answer_rows, conn=conn)
        conn.commit()

        resolved_paper_codes = sorted({row["paper_code"] for row in answer_rows})
        return {
            "status": "success",
            "paper_code": resolved_paper_codes[0] if len(resolved_paper_codes) == 1 else None,
            "paper_codes": resolved_paper_codes,
            "rows": len(answer_rows),
            "rows_processed": len(answer_rows),
            "rows_failed": len(row_errors),
            "errors": row_errors,
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

from __future__ import annotations

import csv
import io

from fastapi import HTTPException, UploadFile

from app.database import get_connection
from app.repositories.english_printable_repository import (
    ENGLISH_EXPECTED_QUESTION_COUNT,
    bulk_upsert_english_answers,
    bulk_upsert_english_questions,
    get_english_paper_meta,
    init_english_printable_tables,
    normalize_english_paper_code,
)


def init_english_paper_printable_tables():
    init_english_printable_tables()


def _normalize_english_answer(value) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    if len(normalized) == 1 and normalized.isalpha():
        return normalized.upper()
    return normalized


def upload_english_answer_csv(file: UploadFile, selected_paper_code: str | None = None) -> dict:
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
    row_errors: list[dict] = []
    question_rows: list[dict] = []
    answer_rows: list[dict] = []
    seen_pairs: dict[tuple[str, int], str] = {}
    selected_normalized_code = normalize_english_paper_code(selected_paper_code.strip()) if selected_paper_code and selected_paper_code.strip() else None

    try:
        for idx, row in enumerate(reader, start=2):
            clean = {
                (key.strip() if key else key): (value.strip() if isinstance(value, str) else value)
                for key, value in row.items()
            }

            csv_paper_code = normalize_english_paper_code(str(clean.get("paper_code") or "").strip())
            paper_code = selected_normalized_code or csv_paper_code
            question_number_raw = str(clean.get("question_number") or "").strip()
            correct_answer = _normalize_english_answer(clean.get("correct_answer"))

            if not paper_code or not question_number_raw or not correct_answer:
                row_errors.append(
                    {"row": idx, "detail": "paper_code, question_number and correct_answer are required"}
                )
                continue

            meta = get_english_paper_meta(paper_code, conn=conn)
            if not meta:
                row_errors.append({"row": idx, "detail": f"invalid paper_code {paper_code}"})
                continue

            try:
                question_number = int(question_number_raw)
            except ValueError:
                row_errors.append({"row": idx, "detail": "question_number must be an integer"})
                continue

            if question_number < 1 or question_number > ENGLISH_EXPECTED_QUESTION_COUNT:
                row_errors.append(
                    {
                        "row": idx,
                        "detail": f"question_number must be between 1 and {ENGLISH_EXPECTED_QUESTION_COUNT}",
                    }
                )
                continue

            if len(correct_answer) > 64:
                row_errors.append({"row": idx, "detail": "correct_answer is too long"})
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
                }
            )
            answer_rows.append(
                {
                    "paper_code": paper_code,
                    "question_number": question_number,
                    "correct_answer": correct_answer,
                    "answer_source": "admin_csv",
                }
            )

        if row_errors:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "CSV contains invalid rows",
                    "rows_failed": len(row_errors),
                    "errors": row_errors,
                },
            )

        if not answer_rows:
            raise HTTPException(status_code=400, detail="CSV contains no valid answer rows")

        paper_codes = sorted({row["paper_code"] for row in answer_rows})
        if len(paper_codes) != 1:
            raise HTTPException(status_code=400, detail="CSV must contain answer keys for exactly one paper at a time")

        expected_numbers = set(range(1, ENGLISH_EXPECTED_QUESTION_COUNT + 1))
        provided_numbers = {int(row["question_number"]) for row in answer_rows}
        if provided_numbers != expected_numbers:
            missing_numbers = sorted(expected_numbers - provided_numbers)
            extra_numbers = sorted(provided_numbers - expected_numbers)
            details = []
            if missing_numbers:
                details.append(f"missing questions: {', '.join(str(number) for number in missing_numbers)}")
            if extra_numbers:
                details.append(f"invalid questions: {', '.join(str(number) for number in extra_numbers)}")
            raise HTTPException(
                status_code=400,
                detail=f"English answer CSV must include exactly questions 1-{ENGLISH_EXPECTED_QUESTION_COUNT}; {'; '.join(details)}",
            )

        question_stats = bulk_upsert_english_questions(question_rows, conn=conn)
        answer_stats = bulk_upsert_english_answers(answer_rows, conn=conn)
        conn.commit()

        paper_code = paper_codes[0]
        return {
            "status": "success",
            "paper_code": paper_code,
            "rows": len(answer_rows),
            "rows_processed": len(answer_rows),
            "rows_failed": 0,
            "questions_inserted": question_stats["inserted"],
            "questions_existing": question_stats["existing"],
            "questions_updated": question_stats["updated"],
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

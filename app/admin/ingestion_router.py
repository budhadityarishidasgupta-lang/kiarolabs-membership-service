import logging
import os
import tempfile
import csv
import io

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel

from app.auth import get_current_user
from app.database import get_connection
from app.ingestion.comprehension.service import ingest_comprehension_file
from app.ingestion.maths.service import ingest_math_pdf
from app.ingestion.verbal_reasoning.parser import (
    convert_vr_pdf_to_review_rows,
    review_rows_to_csv,
)
from app.ingestion.verbal_reasoning.service import (
    import_verbal_reasoning_pdf_as_draft,
    upload_verbal_reasoning_answer_csv,
)
from app.repositories.vr_repository import (
    bulk_upsert_vr_answers,
    bulk_upsert_vr_questions,
    create_or_update_vr_paper,
    delete_vr_paper_content,
    get_active_vr_papers,
    get_vr_answers_for_paper,
    get_vr_paper_meta,
    get_vr_questions_for_paper,
    normalize_vr_paper_code,
)


router = APIRouter(prefix="/admin/ingestion", tags=["admin-ingestion"])
printable_router = APIRouter(tags=["math-printable"])
logger = logging.getLogger(__name__)


class AnswerKeyRequest(BaseModel):
    paper_code: str
    answers: list[str]


class PrintableAnswerUpdate(BaseModel):
    question_number: int
    correct_answer: str


class PrintableAnswerUpdateRequest(BaseModel):
    paper_code: str
    answers: list[PrintableAnswerUpdate]


class PrintablePaperRequest(BaseModel):
    paper_code: str


class VrPrintableQuestionUpdate(BaseModel):
    question_number: int
    question_text: str
    option_a: str | None = None
    option_b: str | None = None
    option_c: str | None = None
    option_d: str | None = None
    option_e: str | None = None


class VrPrintableQuestionUpdateRequest(BaseModel):
    paper_code: str
    questions: list[VrPrintableQuestionUpdate]


def require_admin(user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


def _paper_exists(cur, paper_code: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM math_printable_papers
        WHERE paper_code = %s
        """,
        (paper_code,),
    )
    return cur.fetchone() is not None


def _question_count(cur, paper_code: str) -> int:
    cur.execute(
        """
        SELECT COUNT(*)
        FROM math_printable_questions
        WHERE paper_code = %s
        """,
        (paper_code,),
    )
    return cur.fetchone()[0]


def _answer_count(cur, paper_code: str) -> int:
    cur.execute(
        """
        SELECT COUNT(*)
        FROM math_printable_answer_keys
        WHERE paper_code = %s
        """,
        (paper_code,),
    )
    return cur.fetchone()[0]


def _validate_answer_key_payload(cur, paper_code: str, answers: list[str]):
    if not _paper_exists(cur, paper_code):
        logger.warning("Printable maths answer save rejected for invalid paper_code")
        raise HTTPException(status_code=400, detail="Invalid paper_code")

    question_count = _question_count(cur, paper_code)
    if question_count == 0:
        raise HTTPException(status_code=400, detail="Upload questions before saving answers")

    if not answers:
        raise HTTPException(status_code=400, detail="Answers required")

    if len(answers) != question_count:
        logger.warning("Printable maths answer save rejected because answer count does not match question count")
        raise HTTPException(status_code=400, detail="Answer count must match question count")

    normalized_answers = [str(answer).strip() for answer in answers]
    if any(not answer for answer in normalized_answers):
        raise HTTPException(status_code=400, detail="Answers must not be empty")

    return normalized_answers


def _upload_math_answer_csv(file: UploadFile, selected_paper_code: str | None = None) -> dict:
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
    rows_processed = 0
    inserted = 0
    updated = 0
    unchanged = 0
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

            if not _paper_exists(cur, paper_code):
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

            cur.execute(
                """
                INSERT INTO math_printable_questions
                (paper_code, question_number, question_text)
                VALUES (%s, %s, %s)
                ON CONFLICT (paper_code, question_number) DO NOTHING
                """,
                (paper_code, question_number, f"Question {question_number}"),
            )

            cur.execute(
                """
                SELECT correct_answer
                FROM math_printable_answer_keys
                WHERE paper_code = %s
                  AND question_number = %s
                """,
                (paper_code, question_number),
            )
            existing = cur.fetchone()

            if not existing:
                cur.execute(
                    """
                    INSERT INTO math_printable_answer_keys
                    (paper_code, question_number, correct_answer)
                    VALUES (%s, %s, %s)
                    """,
                    (paper_code, question_number, correct_answer),
                )
                inserted += 1
            elif (existing[0] or "").strip() == correct_answer:
                unchanged += 1
            else:
                cur.execute(
                    """
                    UPDATE math_printable_answer_keys
                    SET correct_answer = %s
                    WHERE paper_code = %s
                      AND question_number = %s
                    """,
                    (correct_answer, paper_code, question_number),
                )
                updated += 1

            rows_processed += 1

        if rows_processed == 0:
            raise HTTPException(status_code=400, detail="CSV contains no valid answer rows")

        conn.commit()
        return {
            "status": "uploaded",
            "paper_code": selected_paper_code,
            "rows": rows_processed,
            "answers_inserted": inserted,
            "answers_updated": updated,
            "answers_unchanged": unchanged,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def _vr_paper_exists(cur, paper_code: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM vr_papers
        WHERE paper_code = %s
        """,
        (normalize_vr_paper_code(paper_code),),
    )
    return cur.fetchone() is not None


def _vr_question_count(cur, paper_code: str) -> int:
    meta = get_vr_paper_meta(paper_code, conn=cur.connection)
    return int((meta or {}).get("questions_count") or 0)


def _vr_answer_count(cur, paper_code: str) -> int:
    meta = get_vr_paper_meta(paper_code, conn=cur.connection)
    return int((meta or {}).get("answers_count") or 0)


def _validate_vr_answer_key_payload(cur, paper_code: str, answers: list[str]):
    normalized_code = normalize_vr_paper_code(paper_code)

    if not _vr_paper_exists(cur, normalized_code):
        raise HTTPException(status_code=400, detail="Invalid paper_code")

    question_count = _vr_question_count(cur, normalized_code)
    if question_count == 0:
        raise HTTPException(status_code=400, detail="Upload questions before saving answers")

    if not answers:
        raise HTTPException(status_code=400, detail="Answers required")

    if len(answers) != question_count:
        raise HTTPException(status_code=400, detail="Answer count must match question count")

    normalized_answers = [str(answer).strip() for answer in answers]
    if any(not answer for answer in normalized_answers):
        raise HTTPException(status_code=400, detail="Answers must not be empty")

    return normalized_code, normalized_answers


@printable_router.get("/practice/math/printable/papers")
def get_math_papers():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                p.paper_code,
                p.paper_name,
                COUNT(DISTINCT q.question_number) AS questions_count,
                COUNT(DISTINCT a.question_number) AS answers_count
            FROM math_printable_papers p
            LEFT JOIN math_printable_questions q
                ON q.paper_code = p.paper_code
            LEFT JOIN math_printable_answer_keys a
                ON a.paper_code = p.paper_code
            GROUP BY p.paper_code, p.paper_name, p.sort_order
            ORDER BY p.sort_order
            """
        )

        rows = cur.fetchall()
        return [
            {
                "paper_code": r[0],
                "paper_name": r[1],
                "questions_count": r[2],
                "answers_count": r[3],
                "ready": r[2] > 0 and r[3] == r[2],
            }
            for r in rows
        ]
    finally:
        cur.close()
        conn.close()


@printable_router.get("/practice/math/printable/questions/meta")
def get_math_printable_questions_meta(paper_code: str):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM math_printable_questions
            WHERE paper_code = %s
            """,
            (paper_code,),
        )

        count = cur.fetchone()[0]
        return {
            "paper_code": paper_code,
            "question_count": count,
        }
    finally:
        cur.close()
        conn.close()


@printable_router.get("/admin/math/printable/questions")
def get_admin_math_printable_questions(paper_code: str, _user=Depends(require_admin)):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT question_number, question_text
            FROM math_printable_questions
            WHERE paper_code = %s
            ORDER BY question_number
            """,
            (paper_code,),
        )

        rows = cur.fetchall()
        return {
            "paper_code": paper_code,
            "questions": [
                {
                    "question_number": row[0],
                    "question_text": row[1],
                }
                for row in rows
            ],
        }
    finally:
        cur.close()
        conn.close()


@printable_router.get("/admin/math/printable/answers")
def get_admin_math_printable_answers(paper_code: str, _user=Depends(require_admin)):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT question_number, correct_answer
            FROM math_printable_answer_keys
            WHERE paper_code = %s
            ORDER BY question_number
            """,
            (paper_code,),
        )

        rows = cur.fetchall()
        return {
            "paper_code": paper_code,
            "answers": [
                {
                    "question_number": row[0],
                    "correct_answer": row[1],
                }
                for row in rows
            ],
        }
    finally:
        cur.close()
        conn.close()


@printable_router.post("/admin/math/printable/answers/update")
def update_admin_math_printable_answers(
    payload: PrintableAnswerUpdateRequest,
    _user=Depends(require_admin),
):
    conn = get_connection()
    cur = conn.cursor()

    try:
        normalized_answers = _validate_answer_key_payload(
            cur,
            payload.paper_code,
            [answer.correct_answer for answer in payload.answers],
        )

        for answer, normalized_answer in zip(payload.answers, normalized_answers):
            cur.execute(
                """
                INSERT INTO math_printable_answer_keys
                (paper_code, question_number, correct_answer)
                VALUES (%s, %s, %s)
                ON CONFLICT (paper_code, question_number)
                DO UPDATE SET correct_answer = EXCLUDED.correct_answer
                """,
                (
                    payload.paper_code,
                    answer.question_number,
                    normalized_answer,
                ),
            )

        conn.commit()
        return {
            "status": "success",
            "paper_code": payload.paper_code,
            "answers_saved": len(payload.answers),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@printable_router.post("/admin/math/printable/delete")
def delete_admin_math_printable_content(
    payload: PrintablePaperRequest,
    _user=Depends(require_admin),
):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            DELETE FROM math_printable_questions
            WHERE paper_code = %s
            """,
            (payload.paper_code,),
        )
        questions_deleted = cur.rowcount

        cur.execute(
            """
            DELETE FROM math_printable_answer_keys
            WHERE paper_code = %s
            """,
            (payload.paper_code,),
        )
        answers_deleted = cur.rowcount

        conn.commit()
        return {
            "status": "deleted",
            "paper_code": payload.paper_code,
            "questions_deleted": questions_deleted,
            "answers_deleted": answers_deleted,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@printable_router.get("/admin/math/printable/validate")
def validate_admin_math_printable_paper(paper_code: str, _user=Depends(require_admin)):
    conn = get_connection()
    cur = conn.cursor()

    try:
        q_count = _question_count(cur, paper_code)
        a_count = _answer_count(cur, paper_code)
        issues = []

        if q_count == 0:
            issues.append("Questions not uploaded")

        if a_count == 0:
            issues.append("Answer key missing")

        if q_count > 0 and a_count > 0 and q_count != a_count:
            issues.append("Answer count does not match question count")

        has_complete_answer_key = q_count > 0 and a_count == q_count
        ready = q_count > 0 and has_complete_answer_key

        return {
            "paper_code": paper_code,
            "questions_count": q_count,
            "answers_count": a_count,
            "has_questions": q_count > 0,
            "has_complete_answer_key": has_complete_answer_key,
            "ready": ready,
            "issues": issues,
            # Backward-compatible aliases for existing admin UI/tests.
            "questions": q_count,
            "answers": a_count,
            "valid": ready,
        }
    finally:
        cur.close()
        conn.close()


@router.post("/maths/upload-pdf")
def upload_maths_pdf(
    paper_code: str = Form(...),
    file: UploadFile = File(...),
    _user=Depends(require_admin),
):
    if not (file.filename or "").lower().endswith(".pdf"):
        logger.warning("Printable maths PDF upload rejected because file is not a PDF")
        raise HTTPException(status_code=400, detail="PDF file required")

    suffix = os.path.splitext(file.filename or "upload.pdf")[1] or ".pdf"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(file.file.read())
        tmp_path = tmp.name

    try:
        count = ingest_math_pdf(tmp_path, paper_code)
        return {"status": "success", "questions": count}
    finally:
        os.unlink(tmp_path)


@router.post("/maths/answer-key")
def save_answer_key(payload: AnswerKeyRequest, current_user=Depends(get_current_user)):
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    paper_code = payload.paper_code
    answers = payload.answers

    conn = get_connection()
    cur = conn.cursor()

    try:
        normalized_answers = _validate_answer_key_payload(cur, paper_code, answers)

        for i, ans in enumerate(normalized_answers, start=1):
            cur.execute(
                """
                INSERT INTO math_printable_answer_keys
                (paper_code, question_number, correct_answer)
                VALUES (%s, %s, %s)
                ON CONFLICT (paper_code, question_number)
                DO UPDATE SET correct_answer = EXCLUDED.correct_answer
                """,
                (paper_code, i, ans),
            )

        conn.commit()

        return {
            "status": "success",
            "paper_code": paper_code,
            "answers_saved": len(answers),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@router.post("/maths/upload-answer-csv")
def upload_maths_answer_key_csv(
    paper_code: str = Form(""),
    file: UploadFile = File(...),
    _user=Depends(require_admin),
):
    if not (file.filename or "").lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="CSV file required")

    return _upload_math_answer_csv(file, paper_code or None)


@router.get("/maths/answer-key")
def get_answer_key(paper_code: str, current_user=Depends(get_current_user)):
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT correct_answer
            FROM math_printable_answer_keys
            WHERE paper_code = %s
            ORDER BY question_number
            """,
            (paper_code,),
        )

        rows = cur.fetchall()
        return {
            "paper_code": paper_code,
            "answers": [row[0] for row in rows],
        }
    finally:
        cur.close()
        conn.close()


@printable_router.get("/practice/verbal-reasoning/printable/papers")
def get_verbal_reasoning_papers():
    papers = get_active_vr_papers()
    return [
        {
            "paper_code": paper["paper_code"],
            "paper_name": paper["title"],
            "title": paper["title"],
            "description": paper["description"],
            "pdf_url": paper["pdf_url"],
            "questions_count": paper["questions_count"],
            "answers_count": paper["answers_count"],
            "ready": paper["ready"],
        }
        for paper in papers
    ]


@printable_router.get("/practice/verbal-reasoning/printable/questions/meta")
def get_verbal_reasoning_printable_questions_meta(paper_code: str):
    meta = get_vr_paper_meta(paper_code)
    if not meta:
        raise HTTPException(status_code=404, detail="Paper not found")
    effective_count = max(meta["questions_count"], meta["answers_count"])
    return {
        "paper_code": meta["paper_code"],
        "question_count": effective_count,
        "questions_count": meta["questions_count"],
        "answer_count": meta["answers_count"],
        "answers_count": meta["answers_count"],
        "ready": effective_count > 0 and effective_count == meta["answers_count"],
    }


@printable_router.get("/practice/verbal-reasoning/printable/questions")
def get_verbal_reasoning_printable_questions(paper_code: str):
    questions = get_vr_questions_for_paper(paper_code)
    return {
        "paper_code": normalize_vr_paper_code(paper_code),
        "questions": questions,
    }


@printable_router.get("/admin/verbal-reasoning/printable/questions")
def get_admin_verbal_reasoning_printable_questions(paper_code: str, _user=Depends(require_admin)):
    return get_verbal_reasoning_printable_questions(paper_code)


@printable_router.get("/admin/verbal-reasoning/printable/answers")
def get_admin_verbal_reasoning_printable_answers(paper_code: str, _user=Depends(require_admin)):
    return {
        "paper_code": normalize_vr_paper_code(paper_code),
        "answers": get_vr_answers_for_paper(paper_code),
    }


@printable_router.post("/admin/verbal-reasoning/printable/answers/update")
def update_admin_verbal_reasoning_printable_answers(
    payload: PrintableAnswerUpdateRequest,
    _user=Depends(require_admin),
):
    conn = get_connection()
    cur = conn.cursor()
    try:
        normalized_code, normalized_answers = _validate_vr_answer_key_payload(
            cur,
            payload.paper_code,
            [answer.correct_answer for answer in payload.answers],
        )
        stats = bulk_upsert_vr_answers(
            [
                {
                    "paper_code": normalized_code,
                    "question_number": answer.question_number,
                    "correct_answer": normalized_answer,
                    "answer_source": "admin_manual",
                }
                for answer, normalized_answer in zip(payload.answers, normalized_answers)
            ],
            conn=conn,
        )
        conn.commit()
        return {
            "status": "success",
            "paper_code": normalized_code,
            "answers_saved": len(payload.answers),
            "answers_inserted": stats["inserted"],
            "answers_updated": stats["updated"],
            "answers_unchanged": stats["unchanged"],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@printable_router.post("/admin/verbal-reasoning/printable/questions/update")
def update_admin_verbal_reasoning_printable_questions(
    payload: VrPrintableQuestionUpdateRequest,
    _user=Depends(require_admin),
):
    conn = get_connection()
    cur = conn.cursor()
    try:
        normalized_code = normalize_vr_paper_code(payload.paper_code)
        stats = bulk_upsert_vr_questions(
            [
                {
                    "paper_code": normalized_code,
                    "question_number": question.question_number,
                    "question_text": question.question_text.strip(),
                    "option_a": question.option_a,
                    "option_b": question.option_b,
                    "option_c": question.option_c,
                    "option_d": question.option_d,
                    "option_e": question.option_e,
                }
                for question in payload.questions
            ],
            conn=conn,
        )
        conn.commit()
        return {
            "status": "success",
            "paper_code": normalized_code,
            "questions_saved": len(payload.questions),
            "questions_inserted": stats["inserted"],
            "questions_updated": stats["updated"],
            "questions_existing": stats["existing"],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@printable_router.post("/admin/verbal-reasoning/printable/delete")
def delete_admin_verbal_reasoning_printable_content(
    payload: PrintablePaperRequest,
    _user=Depends(require_admin),
):
    conn = get_connection()
    try:
        result = delete_vr_paper_content(payload.paper_code, conn=conn)
        conn.commit()
        return {"status": "deleted", **result}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@printable_router.get("/admin/verbal-reasoning/printable/validate")
def validate_admin_verbal_reasoning_printable_paper(paper_code: str, _user=Depends(require_admin)):
    meta = get_vr_paper_meta(paper_code)
    if not meta:
        raise HTTPException(status_code=404, detail="Paper not found")

    q_count = meta["questions_count"]
    a_count = meta["answers_count"]
    issues = []
    if q_count == 0:
        issues.append("Questions not uploaded")
    if a_count == 0:
        issues.append("Answer key missing")
    if q_count > 0 and a_count > 0 and q_count != a_count:
        issues.append("Answer count does not match question count")

    has_complete_answer_key = q_count > 0 and a_count == q_count
    ready = q_count > 0 and has_complete_answer_key
    return {
        "paper_code": meta["paper_code"],
        "questions_count": q_count,
        "answers_count": a_count,
        "has_questions": q_count > 0,
        "has_complete_answer_key": has_complete_answer_key,
        "ready": ready,
        "issues": issues,
        "questions": q_count,
        "answers": a_count,
        "valid": ready,
    }


@router.post("/verbal-reasoning/upload-csv")
def upload_verbal_reasoning_csv(payload: AnswerKeyRequest, current_user=Depends(get_current_user)):
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    conn = get_connection()
    cur = conn.cursor()
    try:
        normalized_code, normalized_answers = _validate_vr_answer_key_payload(cur, payload.paper_code, payload.answers)
        stats = bulk_upsert_vr_answers(
            [
                {
                    "paper_code": normalized_code,
                    "question_number": index,
                    "correct_answer": ans,
                    "answer_source": "admin_manual",
                }
                for index, ans in enumerate(normalized_answers, start=1)
            ],
            conn=conn,
        )
        conn.commit()
        return {
            "status": "success",
            "paper_code": normalized_code,
            "answers_saved": len(normalized_answers),
            "answers_inserted": stats["inserted"],
            "answers_updated": stats["updated"],
            "answers_unchanged": stats["unchanged"],
        }
    finally:
        cur.close()
        conn.close()


@router.post("/verbal-reasoning/upload-answer-csv")
def upload_verbal_reasoning_answer_key_csv(
    paper_code: str = Form(""),
    file: UploadFile = File(...),
    _user=Depends(require_admin),
):
    if not (file.filename or "").lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="CSV file required")

    return upload_verbal_reasoning_answer_csv(file, paper_code or None)


@router.post("/admin/vr/upload-answer-key")
def upload_admin_vr_answer_key(
    paper_code: str = Form(""),
    file: UploadFile = File(...),
    _user=Depends(require_admin),
):
    if not (file.filename or "").lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="CSV file required")
    return upload_verbal_reasoning_answer_csv(file, paper_code or None)


@router.get("/admin/vr/papers")
def get_admin_vr_papers(_user=Depends(require_admin)):
    return get_verbal_reasoning_papers()


@router.get("/admin/vr/answer-key")
def get_admin_vr_answer_key_alias(paper_code: str, _user=Depends(require_admin)):
    answers = get_vr_answers_for_paper(paper_code)
    return {
        "paper_code": normalize_vr_paper_code(paper_code),
        "rows_processed": len(answers),
        "answers": answers,
    }


@router.get("/verbal-reasoning/answer-key")
def get_verbal_reasoning_answer_key(paper_code: str, current_user=Depends(get_current_user)):
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    answers = get_vr_answers_for_paper(paper_code)
    return {
        "paper_code": normalize_vr_paper_code(paper_code),
        "answers": [row["correct_answer"] for row in answers],
    }


@router.post("/verbal-reasoning/upload-reviewed-csv")
def upload_verbal_reasoning_reviewed_csv(
    file: UploadFile = File(...),
    _user=Depends(require_admin),
):
    try:
        content = file.file.read().decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(content))

        required_fields = {
            "paper_code",
            "question_number",
            "question_text",
            "correct_answer",
        }
        if not reader.fieldnames:
            raise HTTPException(status_code=400, detail="CSV has no header row")

        headers = {field.strip() for field in reader.fieldnames if field}
        missing = sorted(required_fields - headers)
        if missing:
            raise HTTPException(status_code=400, detail=f"CSV missing required columns: {', '.join(missing)}")

        conn = get_connection()
        cur = conn.cursor()
        rows_uploaded = 0
        try:
            for idx, row in enumerate(reader, start=1):
                clean = {
                    (key.strip() if key else key): (value.strip() if isinstance(value, str) else value)
                    for key, value in row.items()
                }

                if clean.get("review_status") and clean["review_status"].strip().lower() != "approved":
                    continue

                paper_code = normalize_vr_paper_code(clean.get("paper_code"))
                question_number = clean.get("question_number")
                question_text = clean.get("question_text")
                correct_answer = clean.get("correct_answer")

                if not paper_code or not question_number or not question_text or not correct_answer:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Row {idx}: paper_code, question_number, question_text and correct_answer are required",
                    )

                question_number_int = int(question_number)
                if not _vr_paper_exists(cur, paper_code):
                    raise HTTPException(status_code=400, detail=f"Row {idx}: invalid paper_code {paper_code}")

                bulk_upsert_vr_questions(
                    [
                        {
                            "paper_code": paper_code,
                            "question_number": question_number_int,
                            "question_type": clean.get("question_type"),
                            "question_text": question_text,
                            "option_a": clean.get("option_a"),
                            "option_b": clean.get("option_b"),
                            "option_c": clean.get("option_c"),
                            "option_d": clean.get("option_d"),
                            "option_e": clean.get("option_e"),
                        }
                    ],
                    conn=conn,
                )

                bulk_upsert_vr_answers(
                    [
                        {
                            "paper_code": paper_code,
                            "question_number": question_number_int,
                            "correct_answer": correct_answer,
                            "answer_source": "reviewed_csv",
                        }
                    ],
                    conn=conn,
                )
                rows_uploaded += 1

            conn.commit()
            return {"status": "uploaded", "rows": rows_uploaded}
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="CSV must be UTF-8 encoded")


@router.post("/verbal-reasoning/upload-pdf")
def upload_verbal_reasoning_pdf_for_review(
    paper_code: str = Form(""),
    file: UploadFile = File(...),
    _user=Depends(require_admin),
):
    if not (file.filename or "").lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDF file required")

    suffix = os.path.splitext(file.filename or "upload.pdf")[1] or ".pdf"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(file.file.read())
        tmp_path = tmp.name

    try:
        rows = convert_vr_pdf_to_review_rows(tmp_path, paper_code or None)
        if not rows:
            raise HTTPException(
                status_code=400,
                detail="Could not extract any verbal reasoning questions from this PDF",
            )

        csv_text = review_rows_to_csv(rows)
        output_paper_code = rows[0].paper_code
        filename = f"{output_paper_code}.review.csv"
        response = Response(content=csv_text.encode("utf-8"), media_type="text/csv; charset=utf-8")
        response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
        response.headers["X-VR-Paper-Code"] = output_paper_code
        response.headers["X-VR-Question-Count"] = str(len(rows))
        return response
    finally:
        os.unlink(tmp_path)


@router.post("/verbal-reasoning/import-pdf")
def import_verbal_reasoning_pdf(
    paper_code: str = Form(""),
    file: UploadFile = File(...),
    _user=Depends(require_admin),
):
    if not (file.filename or "").lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDF file required")

    suffix = os.path.splitext(file.filename or "upload.pdf")[1] or ".pdf"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(file.file.read())
        tmp_path = tmp.name

    try:
        result = import_verbal_reasoning_pdf_as_draft(tmp_path, paper_code or None)
        if result["questions_imported"] == 0:
            raise HTTPException(
                status_code=400,
                detail="Could not extract any verbal reasoning questions from this PDF",
            )
        return {
            "status": "draft-imported",
            **result,
            "message": "Questions imported. Review question text and add the answer key before student use.",
        }
    finally:
        os.unlink(tmp_path)


@router.post("/comprehension/upload")
def upload_comprehension(
    paper_code: str = Form(...),
    file: UploadFile = File(...),
    _user=Depends(require_admin),
):
    if (file.filename or "").lower().endswith(".pdf"):
        suffix = os.path.splitext(file.filename or "upload.pdf")[1] or ".pdf"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(file.file.read())
            tmp_path = tmp.name

        try:
            with open(tmp_path, "rb") as pdf_file:
                file.file = pdf_file
                ingest_comprehension_file(file, paper_code)
        finally:
            os.unlink(tmp_path)
    else:
        ingest_comprehension_file(file, paper_code)

    return {"status": "success"}

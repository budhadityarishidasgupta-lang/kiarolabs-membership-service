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


def _vr_paper_exists(cur, paper_code: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM verbal_reasoning_printable_papers
        WHERE paper_code = %s
        """,
        (paper_code,),
    )
    return cur.fetchone() is not None


def _vr_question_count(cur, paper_code: str) -> int:
    cur.execute(
        """
        SELECT COUNT(*)
        FROM verbal_reasoning_printable_questions
        WHERE paper_code = %s
        """,
        (paper_code,),
    )
    return cur.fetchone()[0]


def _vr_answer_count(cur, paper_code: str) -> int:
    cur.execute(
        """
        SELECT COUNT(*)
        FROM verbal_reasoning_printable_answer_keys
        WHERE paper_code = %s
        """,
        (paper_code,),
    )
    return cur.fetchone()[0]


def _validate_vr_answer_key_payload(cur, paper_code: str, answers: list[str]):
    if not _vr_paper_exists(cur, paper_code):
        raise HTTPException(status_code=400, detail="Invalid paper_code")

    question_count = _vr_question_count(cur, paper_code)
    if question_count == 0:
        raise HTTPException(status_code=400, detail="Upload questions before saving answers")

    if not answers:
        raise HTTPException(status_code=400, detail="Answers required")

    if len(answers) != question_count:
        raise HTTPException(status_code=400, detail="Answer count must match question count")

    normalized_answers = [str(answer).strip() for answer in answers]
    if any(not answer for answer in normalized_answers):
        raise HTTPException(status_code=400, detail="Answers must not be empty")

    return normalized_answers


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
            FROM verbal_reasoning_printable_papers p
            LEFT JOIN verbal_reasoning_printable_questions q
                ON q.paper_code = p.paper_code
            LEFT JOIN verbal_reasoning_printable_answer_keys a
                ON a.paper_code = p.paper_code
            WHERE p.is_active = TRUE
            GROUP BY p.paper_code, p.paper_name, p.sort_order
            ORDER BY p.sort_order
            """
        )

        rows = cur.fetchall()
        return [
            {
                "paper_code": row[0],
                "paper_name": row[1],
                "questions_count": row[2],
                "answers_count": row[3],
                "ready": row[2] > 0 and row[3] == row[2],
            }
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()


@printable_router.get("/practice/verbal-reasoning/printable/questions/meta")
def get_verbal_reasoning_printable_questions_meta(paper_code: str):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                COUNT(DISTINCT q.question_number) AS question_count,
                COUNT(DISTINCT a.question_number) AS answer_count
            FROM verbal_reasoning_printable_questions q
            LEFT JOIN verbal_reasoning_printable_answer_keys a
                ON a.paper_code = q.paper_code
               AND a.question_number = q.question_number
            WHERE q.paper_code = %s
            """,
            (paper_code,),
        )
        row = cur.fetchone()
        return {
            "paper_code": paper_code,
            "question_count": row[0] or 0,
            "answer_count": row[1] or 0,
            "answers_count": row[1] or 0,
            "ready": (row[0] or 0) > 0 and (row[0] or 0) == (row[1] or 0),
        }
    finally:
        cur.close()
        conn.close()


@printable_router.get("/practice/verbal-reasoning/printable/questions")
def get_verbal_reasoning_printable_questions(paper_code: str):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                question_number,
                question_text,
                option_a,
                option_b,
                option_c,
                option_d,
                option_e
            FROM verbal_reasoning_printable_questions
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
                    "option_a": row[2],
                    "option_b": row[3],
                    "option_c": row[4],
                    "option_d": row[5],
                    "option_e": row[6],
                }
                for row in rows
            ],
        }
    finally:
        cur.close()
        conn.close()


@printable_router.get("/admin/verbal-reasoning/printable/questions")
def get_admin_verbal_reasoning_printable_questions(paper_code: str, _user=Depends(require_admin)):
    return get_verbal_reasoning_printable_questions(paper_code)


@printable_router.get("/admin/verbal-reasoning/printable/answers")
def get_admin_verbal_reasoning_printable_answers(paper_code: str, _user=Depends(require_admin)):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT question_number, correct_answer
            FROM verbal_reasoning_printable_answer_keys
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


@printable_router.post("/admin/verbal-reasoning/printable/answers/update")
def update_admin_verbal_reasoning_printable_answers(
    payload: PrintableAnswerUpdateRequest,
    _user=Depends(require_admin),
):
    conn = get_connection()
    cur = conn.cursor()

    try:
        normalized_answers = _validate_vr_answer_key_payload(
            cur,
            payload.paper_code,
            [answer.correct_answer for answer in payload.answers],
        )

        for answer, normalized_answer in zip(payload.answers, normalized_answers):
            cur.execute(
                """
                INSERT INTO verbal_reasoning_printable_answer_keys
                (paper_code, question_number, correct_answer, updated_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (paper_code, question_number)
                DO UPDATE SET correct_answer = EXCLUDED.correct_answer,
                              updated_at = NOW()
                """,
                (payload.paper_code, answer.question_number, normalized_answer),
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


@printable_router.post("/admin/verbal-reasoning/printable/questions/update")
def update_admin_verbal_reasoning_printable_questions(
    payload: VrPrintableQuestionUpdateRequest,
    _user=Depends(require_admin),
):
    conn = get_connection()
    cur = conn.cursor()

    try:
        for question in payload.questions:
            cur.execute(
                """
                UPDATE verbal_reasoning_printable_questions
                SET question_text = %s,
                    option_a = %s,
                    option_b = %s,
                    option_c = %s,
                    option_d = %s,
                    option_e = %s,
                    updated_at = NOW()
                WHERE paper_code = %s
                  AND question_number = %s
                """,
                (
                    question.question_text.strip(),
                    question.option_a,
                    question.option_b,
                    question.option_c,
                    question.option_d,
                    question.option_e,
                    payload.paper_code,
                    question.question_number,
                ),
            )
        conn.commit()
        return {
            "status": "success",
            "paper_code": payload.paper_code,
            "questions_saved": len(payload.questions),
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
    cur = conn.cursor()

    try:
        cur.execute(
            """
            DELETE FROM verbal_reasoning_printable_questions
            WHERE paper_code = %s
            """,
            (payload.paper_code,),
        )
        questions_deleted = cur.rowcount

        cur.execute(
            """
            DELETE FROM verbal_reasoning_printable_answer_keys
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


@printable_router.get("/admin/verbal-reasoning/printable/validate")
def validate_admin_verbal_reasoning_printable_paper(paper_code: str, _user=Depends(require_admin)):
    conn = get_connection()
    cur = conn.cursor()

    try:
        q_count = _vr_question_count(cur, paper_code)
        a_count = _vr_answer_count(cur, paper_code)
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
            "questions": q_count,
            "answers": a_count,
            "valid": ready,
        }
    finally:
        cur.close()
        conn.close()


@router.post("/verbal-reasoning/upload-csv")
def upload_verbal_reasoning_csv(payload: AnswerKeyRequest, current_user=Depends(get_current_user)):
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    paper_code = payload.paper_code
    answers = payload.answers
    conn = get_connection()
    cur = conn.cursor()

    try:
        normalized_answers = _validate_vr_answer_key_payload(cur, paper_code, answers)
        for i, ans in enumerate(normalized_answers, start=1):
            cur.execute(
                """
                INSERT INTO verbal_reasoning_printable_answer_keys
                (paper_code, question_number, correct_answer, updated_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (paper_code, question_number)
                DO UPDATE SET correct_answer = EXCLUDED.correct_answer,
                              updated_at = NOW()
                """,
                (paper_code, i, ans),
            )
        conn.commit()
        return {"status": "success", "paper_code": paper_code, "answers_saved": len(answers)}
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


@router.get("/verbal-reasoning/answer-key")
def get_verbal_reasoning_answer_key(paper_code: str, current_user=Depends(get_current_user)):
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT correct_answer
            FROM verbal_reasoning_printable_answer_keys
            WHERE paper_code = %s
            ORDER BY question_number
            """,
            (paper_code,),
        )
        rows = cur.fetchall()
        return {"paper_code": paper_code, "answers": [row[0] for row in rows]}
    finally:
        cur.close()
        conn.close()


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

                paper_code = clean.get("paper_code")
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
                        paper_code,
                        question_number_int,
                        clean.get("section_title"),
                        clean.get("question_type"),
                        question_text,
                        clean.get("option_a"),
                        clean.get("option_b"),
                        clean.get("option_c"),
                        clean.get("option_d"),
                        clean.get("option_e"),
                        clean.get("source_block"),
                        clean.get("notes"),
                    ),
                )

                cur.execute(
                    """
                    INSERT INTO verbal_reasoning_printable_answer_keys
                    (paper_code, question_number, correct_answer, updated_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (paper_code, question_number)
                    DO UPDATE SET correct_answer = EXCLUDED.correct_answer,
                                  updated_at = NOW()
                    """,
                    (paper_code, question_number_int, correct_answer),
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

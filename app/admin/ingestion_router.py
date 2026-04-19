import logging
import os
import tempfile

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

from app.auth import get_current_user
from app.database import get_connection
from app.ingestion.comprehension.service import ingest_comprehension_file
from app.ingestion.maths.service import ingest_math_pdf


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

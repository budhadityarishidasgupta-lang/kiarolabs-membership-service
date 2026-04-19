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


@printable_router.get("/practice/math/printable/papers")
def get_math_papers():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT paper_code, paper_name
            FROM math_printable_papers
            ORDER BY sort_order
            """
        )

        rows = cur.fetchall()
        return [
            {
                "paper_code": r[0],
                "paper_name": r[1],
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
    if not payload.answers:
        raise HTTPException(status_code=400, detail="Answers required")

    conn = get_connection()
    cur = conn.cursor()

    try:
        for answer in payload.answers:
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
                    str(answer.correct_answer).strip(),
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
        cur.execute(
            """
            SELECT COUNT(*)
            FROM math_printable_questions
            WHERE paper_code = %s
            """,
            (paper_code,),
        )
        q_count = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COUNT(*)
            FROM math_printable_answer_keys
            WHERE paper_code = %s
            """,
            (paper_code,),
        )
        a_count = cur.fetchone()[0]

        return {
            "paper_code": paper_code,
            "questions": q_count,
            "answers": a_count,
            "valid": q_count == a_count,
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

    if not answers:
        raise HTTPException(status_code=400, detail="Answers required")

    conn = get_connection()
    cur = conn.cursor()

    try:
        for i, ans in enumerate(answers, start=1):
            cur.execute(
                """
                INSERT INTO math_printable_answer_keys
                (paper_code, question_number, correct_answer)
                VALUES (%s, %s, %s)
                ON CONFLICT (paper_code, question_number)
                DO UPDATE SET correct_answer = EXCLUDED.correct_answer
                """,
                (paper_code, i, str(ans).strip()),
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

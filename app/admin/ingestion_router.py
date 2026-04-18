import os
import tempfile

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from app.auth import get_current_user
from app.ingestion.comprehension.service import ingest_comprehension_file
from app.ingestion.maths.service import ingest_math_pdf


router = APIRouter(prefix="/admin/ingestion", tags=["admin-ingestion"])


def require_admin(user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


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

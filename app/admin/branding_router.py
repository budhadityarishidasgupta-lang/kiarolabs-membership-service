import httpx

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from app.auth import get_current_user
from app.services.branding_service import generate_branded_pdf


router = APIRouter(prefix="/admin/branding", tags=["admin-branding"])


def require_admin(user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


@router.post("/generate")
async def generate_branding_material(
    paper_code: str = Form(...),
    title: str = Form(...),
    subtitle: str = Form(""),
    logo_url: str = Form(""),
    footer_text: str = Form(""),
    branding_theme: str = Form("default"),
    source_pdf: UploadFile = File(...),
    logo_file: UploadFile | None = File(default=None),
    _user=Depends(require_admin),
):
    if not source_pdf.filename or not source_pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Source PDF is required")

    payload = {
        "paper_code": paper_code.strip(),
        "title": title.strip(),
        "subtitle": subtitle.strip(),
        "logo_url": logo_url.strip(),
        "footer_text": footer_text.strip(),
        "branding_theme": branding_theme.strip() or "default",
    }

    try:
        result = generate_branded_pdf(
            payload,
            source_pdf=(
                source_pdf.filename,
                await source_pdf.read(),
                source_pdf.content_type or "application/pdf",
            ),
            logo_file=(
                (
                    logo_file.filename,
                    await logo_file.read(),
                    logo_file.content_type or "application/octet-stream",
                )
                if logo_file and logo_file.filename
                else None
            ),
        )
        return result
    except httpx.HTTPStatusError as exc:
        detail = None
        try:
            detail = exc.response.json().get("detail")
        except Exception:
            detail = exc.response.text or "Branding service request failed"
        raise HTTPException(status_code=502, detail=detail) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Branding service unavailable: {exc}") from exc

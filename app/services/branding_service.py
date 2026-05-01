import os

import httpx


BRANDING_SERVICE_URL = os.getenv("PDF_BRANDING_SERVICE_URL", "http://localhost:8001").rstrip("/")


def generate_branded_pdf(
    payload: dict[str, str],
    *,
    source_pdf: tuple[str, bytes, str],
    logo_file: tuple[str, bytes, str] | None = None,
) -> dict:
    data = {
        "paper_code": payload.get("paper_code", ""),
        "title": payload.get("title", ""),
        "subtitle": payload.get("subtitle", ""),
        "logo_url": payload.get("logo_url", ""),
        "footer_text": payload.get("footer_text", ""),
        "branding_theme": payload.get("branding_theme", "default"),
    }

    files = {
        "source_pdf": source_pdf,
    }
    if logo_file is not None:
        files["logo_file"] = logo_file

    with httpx.Client(timeout=120.0) as client:
        response = client.post(
            f"{BRANDING_SERVICE_URL}/generate",
            data=data,
            files=files,
        )

    response.raise_for_status()
    return response.json()

from __future__ import annotations

import csv
import io
import re
from dataclasses import asdict, dataclass
from pathlib import Path

from pdfminer.high_level import extract_text


QUESTION_START_RE = re.compile(r"(?m)^\s*(\d{1,2})[\).\s]+")
OPTION_RE = re.compile(r"(?m)^\s*([A-E])[\)\.\s]+(.+)$")
PAGE_FOOTER_RE = re.compile(r"Page\s+\d+.*$", re.IGNORECASE | re.MULTILINE)
PAPER_NUMBER_RE = re.compile(r"(\d+)")


@dataclass
class VrReviewRow:
    paper_code: str
    question_number: int
    section_title: str
    question_type: str
    question_text: str
    option_a: str
    option_b: str
    option_c: str
    option_d: str
    option_e: str
    correct_answer: str
    review_status: str
    review_flags: str
    source_block: str
    notes: str


def _clean_text(text: str) -> str:
    text = text.replace("\x00", " ")
    text = PAGE_FOOTER_RE.sub("", text)
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split())
        if not line:
            continue
        if line.startswith("MR") and "essment" in line:
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def _extract_paper_number(name: str) -> int:
    match = PAPER_NUMBER_RE.search(Path(name).stem)
    return int(match.group(1)) if match else 0


def derive_vr_paper_code(name: str, selected_paper_code: str | None = None) -> str:
    if selected_paper_code and selected_paper_code.strip():
        return selected_paper_code.strip().lower()
    paper_number = _extract_paper_number(name)
    if paper_number > 0:
        return f"vr-{paper_number:02d}"
    return "vr-01"


def _extract_sections(text: str) -> list[tuple[str, str]]:
    sections: list[tuple[str, str]] = []
    current_title = "General"
    buffer: list[str] = []
    lines = text.splitlines()

    for idx, line in enumerate(lines):
        lowered = line.lower()
        if "read the following carefully" in lowered:
            continue

        if (
            lowered.startswith("in these question")
            or lowered.startswith("in these sentences")
            or lowered.startswith("choose the")
        ):
            if buffer:
                sections.append((current_title, "\n".join(buffer).strip()))
                buffer = []
            current_title = line.strip()
            continue

        if QUESTION_START_RE.match(line):
            buffer.append("\n".join(lines[idx:]))
            break

    if buffer:
        merged = "\n".join(buffer)
    else:
        merged = "\n".join(lines)

    if QUESTION_START_RE.search(merged):
        sections.append((current_title, merged.strip()))

    return sections


def _derive_question_type(section_title: str) -> str:
    title = section_title.lower()
    if "hidden" in title:
        return "hidden_word"
    if "same letter" in title:
        return "dual_letter_fit"
    if "numbers" in title:
        return "number_relationship"
    if "code" in title:
        return "code_breaking"
    if "letter" in title:
        return "letter_logic"
    return "verbal_reasoning"


def _split_question_blocks(section_text: str) -> list[tuple[int, str]]:
    matches = list(QUESTION_START_RE.finditer(section_text))
    if not matches:
        return []

    blocks: list[tuple[int, str]] = []
    for idx, match in enumerate(matches):
        number = int(match.group(1))
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(section_text)
        block = section_text[start:end].strip()
        if block:
            blocks.append((number, block))
    return blocks


def _extract_options(block: str) -> tuple[str, dict[str, str]]:
    options: dict[str, str] = {}
    matches = list(OPTION_RE.finditer(block))
    if not matches:
        return block.strip(), options

    question_text = block[: matches[0].start()].strip()
    for idx, match in enumerate(matches):
        label = match.group(1).upper()
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(block)
        option_text = (match.group(2) + " " + block[start:end]).strip()
        options[label] = " ".join(option_text.split())
    return question_text, options


def _normalize_inline_options(text: str) -> tuple[str, dict[str, str]]:
    options: dict[str, str] = {}
    matches = list(re.finditer(r"\b([A-E])[\)\.\s]+", text))
    if len(matches) < 2:
        return text, options

    question_text = text[: matches[0].start()].strip()
    for idx, match in enumerate(matches):
        label = match.group(1).upper()
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        options[label] = " ".join(text[start:end].split())
    return question_text or text, options


def _review_flags(question_text: str, options: dict[str, str], source_block: str) -> list[str]:
    flags: list[str] = []
    prompt = " ".join(question_text.split())
    if len(prompt) < 12:
        flags.append("short_prompt")
    if "Ã‚" in source_block or "ï¿½" in source_block:
        flags.append("encoding_noise")
    if re.search(r"[|]{2,}|[?]{3,}|[)]{3,}", source_block):
        flags.append("ocr_noise")
    if re.search(r"\b[A-E]\b", source_block) and not options:
        flags.append("missing_option_parse")
    if len(options) not in {0, 3, 4, 5}:
        flags.append("partial_option_set")
    if len(prompt) > 320:
        flags.append("long_block")
    return flags


def convert_vr_pdf_to_review_rows(pdf_path: str, selected_paper_code: str | None = None) -> list[VrReviewRow]:
    text = _clean_text(extract_text(pdf_path) or "")
    paper_code = derive_vr_paper_code(pdf_path, selected_paper_code)
    rows: list[VrReviewRow] = []

    for section_title, section_text in _extract_sections(text):
        question_type = _derive_question_type(section_title)
        for question_number, block in _split_question_blocks(section_text):
            question_text, options = _extract_options(block)
            if not options:
                question_text, options = _normalize_inline_options(question_text)
            if not question_text:
                question_text = block.strip()
            flags = _review_flags(question_text, options, block)
            rows.append(
                VrReviewRow(
                    paper_code=paper_code,
                    question_number=question_number,
                    section_title=section_title,
                    question_type=question_type,
                    question_text=question_text,
                    option_a=options.get("A", ""),
                    option_b=options.get("B", ""),
                    option_c=options.get("C", ""),
                    option_d=options.get("D", ""),
                    option_e=options.get("E", ""),
                    correct_answer="",
                    review_status="needs_review",
                    review_flags=",".join(flags),
                    source_block=" ".join(block.split()),
                    notes="Review OCR, clean options, and set correct_answer manually.",
                )
            )

    deduped: dict[int, VrReviewRow] = {}
    for row in rows:
        deduped[row.question_number] = row
    return [deduped[key] for key in sorted(deduped)]


def review_rows_to_csv(rows: list[VrReviewRow]) -> str:
    buffer = io.StringIO()
    fieldnames = list(asdict(rows[0]).keys()) if rows else list(asdict(VrReviewRow("", 0, "", "", "", "", "", "", "", "", "", "", "", "", "")).keys())
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(asdict(row))
    return buffer.getvalue()

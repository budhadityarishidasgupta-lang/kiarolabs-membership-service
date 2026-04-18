import csv
import io
import re

from pdfminer.high_level import extract_text

from app.ingestion.comprehension.schemas import (
    ParsedComprehensionPassage,
    ParsedComprehensionQuestion,
)


PASSAGE_SPLIT_RE = re.compile(r"\n\s*Passage\s+\d+[:.\s]*", re.IGNORECASE)
QUESTION_SPLIT_RE = re.compile(r"\n?\d+\.\s")


def parse_comprehension_csv(content: str, paper_code: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(content))
    passages = []
    current_passage = None
    passage_count = 0

    for row in reader:
        clean = {
            (key.strip() if key else key): (value.strip() if isinstance(value, str) else value)
            for key, value in row.items()
        }

        starts_new_passage = clean.get("new_passage") == "1" or current_passage is None

        if starts_new_passage:
            passage_count += 1
            current_passage = {
                "paper_code": paper_code,
                "title": clean.get("title") or f"{paper_code} Passage {passage_count}",
                "passage": clean.get("passage_text") or clean.get("passage") or "",
                "difficulty": clean.get("difficulty"),
                "questions": [],
            }
            passages.append(current_passage)

        if clean.get("question_text"):
            current_passage["questions"].append(
                ParsedComprehensionQuestion(
                    question_text=clean.get("question_text"),
                    option_a=clean.get("option_a"),
                    option_b=clean.get("option_b"),
                    option_c=clean.get("option_c"),
                    option_d=clean.get("option_d"),
                    correct_answer=clean.get("correct_answer"),
                    question_type=clean.get("question_type") or "comprehension",
                    sort_order=int(clean.get("sort_order") or len(current_passage["questions"]) + 1),
                ).dict()
            )

    return [ParsedComprehensionPassage(**passage).dict() for passage in passages]


def parse_comprehension_pdf(file_path: str, paper_code: str) -> list[dict]:
    text = extract_text(file_path)
    raw_passages = [part.strip() for part in PASSAGE_SPLIT_RE.split(text) if part.strip()]
    passages = []

    for passage_index, raw_passage in enumerate(raw_passages, start=1):
        parts = [part.strip() for part in QUESTION_SPLIT_RE.split(raw_passage) if part.strip()]
        passage_text = parts[0] if parts else raw_passage
        question_parts = parts[1:] if len(parts) > 1 else []

        questions = [
            ParsedComprehensionQuestion(
                question_text=question_text,
                sort_order=question_index,
            )
            for question_index, question_text in enumerate(question_parts, start=1)
        ]

        passages.append(
            ParsedComprehensionPassage(
                paper_code=paper_code,
                title=f"{paper_code} Passage {passage_index}",
                passage=passage_text,
                questions=questions,
            ).dict()
        )

    return passages

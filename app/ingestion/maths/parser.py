import re

from pdfminer.high_level import extract_text

from app.ingestion.maths.schemas import ParsedMathQuestion


QUESTION_SPLIT_RE = re.compile(r"\n?\d+\.\s")
QUESTION_NUMBER_RE = re.compile(r"^\s*(\d+)\.\s")


def parse_math_pdf(file_path: str, paper_code: str) -> list[dict]:
    text = extract_text(file_path)
    matches = list(QUESTION_NUMBER_RE.finditer(text))
    questions = []

    for index, match in enumerate(matches):
        question_number = int(match.group(1))
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        question_text = text[start:end].strip()

        questions.append(
            ParsedMathQuestion(
                paper_code=paper_code,
                question_number=question_number,
                question_text=question_text,
            ).dict()
        )

    if questions:
        return questions

    parts = [part.strip() for part in QUESTION_SPLIT_RE.split(text) if part.strip()]
    return [
        ParsedMathQuestion(
            paper_code=paper_code,
            question_number=index,
            question_text=part,
        ).dict()
        for index, part in enumerate(parts, start=1)
    ]

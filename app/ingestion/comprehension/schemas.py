from pydantic import BaseModel


class ComprehensionUploadRequest(BaseModel):
    paper_code: str


class ParsedComprehensionQuestion(BaseModel):
    question_text: str
    option_a: str | None = None
    option_b: str | None = None
    option_c: str | None = None
    option_d: str | None = None
    correct_answer: str | None = None
    question_type: str = "comprehension"
    sort_order: int = 0


class ParsedComprehensionPassage(BaseModel):
    paper_code: str
    title: str | None = None
    passage: str
    difficulty: str | None = None
    questions: list[ParsedComprehensionQuestion]


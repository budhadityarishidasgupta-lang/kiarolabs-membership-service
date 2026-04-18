from pydantic import BaseModel


class MathPdfUploadRequest(BaseModel):
    paper_code: str


class ParsedMathQuestion(BaseModel):
    paper_code: str
    question_number: int
    question_text: str


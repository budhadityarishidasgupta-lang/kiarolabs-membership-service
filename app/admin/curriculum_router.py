from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.admin.ingestion_router import require_admin
from app.admin.repositories.math_admin_repository import (
    create_math_lesson,
    delete_math_lesson,
    delete_e2e_math_lessons,
    get_math_overview,
    list_math_lesson_question_answers,
    list_math_courses,
    list_math_lessons,
    update_math_lesson,
    update_math_question_content,
    update_math_question_correct_answer,
)
from app.admin.repositories.spelling_admin_repository import (
    create_spelling_course,
    create_spelling_lesson,
    get_spelling_overview,
    list_spelling_lesson_content,
    list_spelling_courses,
    list_spelling_lessons,
    update_spelling_lesson,
    update_spelling_content_answer,
)
from app.admin.repositories.words_admin_repository import (
    create_words_course,
    create_words_lesson,
    get_words_overview,
    list_words_lesson_content,
    list_words_courses,
    list_words_lessons,
    update_words_lesson,
    update_words_content_answer,
)


router = APIRouter(prefix="/admin/curriculum", tags=["admin-curriculum"])


class CreateCourseRequest(BaseModel):
    name: str


class CreateLessonRequest(BaseModel):
    course_id: int | None = None
    lesson_name: str
    display_name: str | None = None
    topic: str | None = None
    difficulty: str | None = None
    is_active: bool = True


class UpdateMathCorrectAnswerRequest(BaseModel):
    correct_answer: str


class UpdateLessonContentAnswerRequest(BaseModel):
    answer: str


class UpdateMathQuestionContentRequest(BaseModel):
    option_a: str | None = None
    option_b: str | None = None
    option_c: str | None = None
    option_d: str | None = None
    option_e: str | None = None
    correct_option: str


class UpdateLessonRequest(BaseModel):
    lesson_name: str
    display_name: str | None = None


def _normalize_module(module: str) -> str:
    key = (module or "").strip().lower()
    if key == "math":
        return "maths"
    if key in {"maths", "spelling", "words"}:
        return key
    raise HTTPException(status_code=404, detail="Module not found")


@router.get("/modules")
def get_admin_modules(_user=Depends(require_admin)):
    return {
        "status": "ok",
        "data": [
            get_words_overview(),
            get_spelling_overview(),
            get_math_overview(),
        ],
    }


@router.get("/{module}/overview")
def get_module_overview(module: str, _user=Depends(require_admin)):
    normalized = _normalize_module(module)

    if normalized == "words":
        data = get_words_overview()
    elif normalized == "spelling":
        data = get_spelling_overview()
    else:
        data = get_math_overview()

    return {"status": "ok", "data": data}


@router.get("/{module}/courses")
def get_module_courses(module: str, _user=Depends(require_admin)):
    normalized = _normalize_module(module)

    if normalized == "words":
        data = list_words_courses()
    elif normalized == "spelling":
        data = list_spelling_courses()
    else:
        data = list_math_courses()

    return {"status": "ok", "data": data}


@router.post("/{module}/courses")
def create_module_course(module: str, payload: CreateCourseRequest, _user=Depends(require_admin)):
    normalized = _normalize_module(module)
    name = payload.name.strip()

    if not name:
        raise HTTPException(status_code=400, detail="Course name is required")

    if normalized == "words":
        data = create_words_course(name)
    elif normalized == "spelling":
        data = create_spelling_course(name)
    else:
        raise HTTPException(
            status_code=400,
            detail="Course creation is not supported for maths without schema changes",
        )

    return {"status": "ok", "data": data}


@router.get("/{module}/lessons")
def get_module_lessons(module: str, course_id: int | None = None, _user=Depends(require_admin)):
    normalized = _normalize_module(module)

    if normalized == "words":
        data = list_words_lessons(course_id)
    elif normalized == "spelling":
        data = list_spelling_lessons(course_id)
    else:
        data = list_math_lessons()

    return {"status": "ok", "data": data}


@router.post("/{module}/lessons")
def create_module_lesson(module: str, payload: CreateLessonRequest, _user=Depends(require_admin)):
    normalized = _normalize_module(module)
    lesson_name = payload.lesson_name.strip()

    if not lesson_name:
        raise HTTPException(status_code=400, detail="Lesson name is required")

    if normalized == "words":
        if payload.course_id is None:
            raise HTTPException(status_code=400, detail="course_id is required")
        data = create_words_lesson(payload.course_id, lesson_name)
    elif normalized == "spelling":
        if payload.course_id is None:
            raise HTTPException(status_code=400, detail="course_id is required")
        data = create_spelling_lesson(
            course_id=payload.course_id,
            lesson_name=lesson_name,
            display_name=payload.display_name,
            is_active=payload.is_active,
        )
    else:
        data = create_math_lesson(
            lesson_name=lesson_name,
            display_name=payload.display_name,
            topic=payload.topic,
            difficulty=payload.difficulty,
            is_active=payload.is_active,
        )

    return {"status": "ok", "data": data}


@router.patch("/{module}/lessons/{lesson_id}")
def update_module_lesson(
    module: str,
    lesson_id: int,
    payload: UpdateLessonRequest,
    _user=Depends(require_admin),
):
    normalized = _normalize_module(module)
    lesson_name = payload.lesson_name.strip()
    display_name = payload.display_name

    if not lesson_name:
        raise HTTPException(status_code=400, detail="Lesson name is required")

    try:
        if normalized == "words":
            data = update_words_lesson(
                lesson_id,
                lesson_name=lesson_name,
                display_name=display_name,
            )
        elif normalized == "spelling":
            data = update_spelling_lesson(
                lesson_id,
                lesson_name=lesson_name,
                display_name=display_name,
            )
        else:
            data = update_math_lesson(
                lesson_id,
                lesson_name=lesson_name,
                display_name=display_name,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if not data:
        raise HTTPException(status_code=404, detail="Lesson not found")

    return {"status": "ok", "data": data}


@router.delete("/maths/lessons/{lesson_id}")
def delete_maths_lesson(lesson_id: int, _user=Depends(require_admin)):
    data = delete_math_lesson(lesson_id)
    if not data:
        raise HTTPException(status_code=404, detail="Lesson not found")
    return {"status": "ok", "data": data}


@router.delete("/maths/test-fixtures")
def delete_maths_test_fixtures(_user=Depends(require_admin)):
    return {"status": "ok", "data": delete_e2e_math_lessons()}


@router.get("/maths/lessons/{lesson_id}/questions")
def get_maths_lesson_questions(lesson_id: int, _user=Depends(require_admin)):
    data = list_math_lesson_question_answers(lesson_id)
    if not data:
        raise HTTPException(status_code=404, detail="Lesson not found")
    return {"status": "ok", "data": data}


@router.patch("/maths/questions/{question_id}/correct-answer")
def patch_maths_question_correct_answer(
    question_id: int,
    payload: UpdateMathCorrectAnswerRequest,
    _user=Depends(require_admin),
):
    try:
        data = update_math_question_correct_answer(question_id, payload.correct_answer)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if not data:
        raise HTTPException(status_code=404, detail="Question not found")
    return {"status": "ok", "data": data}


@router.get("/{module}/lessons/{lesson_id}/content")
def get_module_lesson_content(module: str, lesson_id: int, _user=Depends(require_admin)):
    normalized = _normalize_module(module)

    if normalized == "maths":
        maths_data = list_math_lesson_question_answers(lesson_id)
        if not maths_data:
            raise HTTPException(status_code=404, detail="Lesson not found")
        data = {
            "lesson_id": maths_data["lesson_id"],
            "lesson_name": maths_data["lesson_name"],
            "display_name": maths_data.get("display_name"),
            "items": [
                {
                    "item_id": question["question_id"],
                    "prompt": question["stem"],
                    "answer": question["correct_answer"],
                    "options": question["options"],
                    "correct_option": question["correct_option"],
                }
                for question in maths_data.get("questions", [])
            ],
        }
    elif normalized == "spelling":
        data = list_spelling_lesson_content(lesson_id)
    else:
        data = list_words_lesson_content(lesson_id)

    if not data:
        raise HTTPException(status_code=404, detail="Lesson not found")

    return {"status": "ok", "data": data}


@router.patch("/maths/content/{item_id}")
def patch_maths_question_content(
    item_id: int,
    payload: UpdateMathQuestionContentRequest,
    _user=Depends(require_admin),
):
    try:
        updated = update_math_question_content(
            item_id,
            option_a=payload.option_a,
            option_b=payload.option_b,
            option_c=payload.option_c,
            option_d=payload.option_d,
            option_e=payload.option_e,
            correct_option=payload.correct_option,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if not updated:
        raise HTTPException(status_code=404, detail="Question not found")

    data = {
        "item_id": updated["question_id"],
        "prompt": updated["stem"],
        "answer": updated["correct_answer"],
        "options": updated["options"],
        "correct_option": updated["correct_option"],
    }
    return {"status": "ok", "data": data}


@router.patch("/{module}/content/{item_id}/answer")
def patch_module_content_answer(
    module: str,
    item_id: int,
    payload: UpdateLessonContentAnswerRequest,
    _user=Depends(require_admin),
):
    normalized = _normalize_module(module)

    try:
        if normalized == "maths":
            updated = update_math_question_correct_answer(item_id, payload.answer)
            data = {
                "item_id": updated["question_id"],
                "prompt": updated["stem"],
                "answer": updated["correct_answer"],
            } if updated else None
        elif normalized == "spelling":
            data = update_spelling_content_answer(item_id, payload.answer)
        else:
            data = update_words_content_answer(item_id, payload.answer)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if not data:
        raise HTTPException(status_code=404, detail="Item not found")

    return {"status": "ok", "data": data}

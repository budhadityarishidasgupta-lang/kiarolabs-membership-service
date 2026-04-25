from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.admin.ingestion_router import require_admin
from app.admin.repositories.math_admin_repository import (
    create_math_lesson,
    delete_math_lesson,
    get_math_overview,
    list_math_courses,
    list_math_lessons,
)
from app.admin.repositories.spelling_admin_repository import (
    create_spelling_course,
    create_spelling_lesson,
    get_spelling_overview,
    list_spelling_courses,
    list_spelling_lessons,
)
from app.admin.repositories.words_admin_repository import (
    create_words_course,
    create_words_lesson,
    get_words_overview,
    list_words_courses,
    list_words_lessons,
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


@router.delete("/maths/lessons/{lesson_id}")
def delete_maths_lesson(lesson_id: int, _user=Depends(require_admin)):
    data = delete_math_lesson(lesson_id)
    if not data:
        raise HTTPException(status_code=404, detail="Lesson not found")
    return {"status": "ok", "data": data}

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.auth import get_current_user, resolve_verified_learning_user_id
from app.database import get_connection
from app.vr_mvp.repository import UAT_EMAIL, create_session, get_mvp_paper, get_result, get_session, list_mvp_papers, save_answer, submit_session

router = APIRouter(prefix="/vr/mvp", tags=["vr-mvp"])

class StartRequest(BaseModel):
    paper_code: str
class AnswerRequest(BaseModel):
    question_number: int
    selected_answer: str


def require_uat_user(user=Depends(get_current_user)):
    email=str(user.get('sub') or user.get('email') or '').strip().lower()
    if email != UAT_EMAIL:
        raise HTTPException(status_code=404, detail='Not found')
    conn=get_connection();cur=conn.cursor()
    try:
        user_id=resolve_verified_learning_user_id(cur,user)
        if not user_id: raise HTTPException(status_code=403, detail='Learning user not resolved')
        return {**user,'learning_user_id':int(user_id)}
    finally:
        cur.close();conn.close()

@router.get('/papers')
def papers(_user=Depends(require_uat_user)):
    return {'levels':['BASIC','INTERMEDIATE','MASTERY'],'papers':list_mvp_papers()}

@router.get('/papers/{paper_code}')
def paper(paper_code:str,_user=Depends(require_uat_user)):
    try: return get_mvp_paper(paper_code)
    except ValueError as exc: raise HTTPException(status_code=409,detail=str(exc)) from exc

@router.post('/sessions')
def start(payload:StartRequest,user=Depends(require_uat_user)):
    try: return create_session(user['learning_user_id'],payload.paper_code)
    except ValueError as exc: raise HTTPException(status_code=409,detail=str(exc)) from exc

@router.get('/sessions/{session_id}')
def session(session_id:int,user=Depends(require_uat_user)):
    value=get_session(session_id,user['learning_user_id'])
    if not value: raise HTTPException(status_code=404,detail='Session not found')
    return value

@router.put('/sessions/{session_id}/answer')
def answer(session_id:int,payload:AnswerRequest,user=Depends(require_uat_user)):
    if payload.question_number < 1 or payload.question_number > 35: raise HTTPException(status_code=400,detail='Invalid question number')
    try: save_answer(session_id,user['learning_user_id'],payload.question_number,payload.selected_answer)
    except ValueError as exc: raise HTTPException(status_code=409,detail=str(exc)) from exc
    return {'status':'saved'}

@router.post('/sessions/{session_id}/submit')
def submit(session_id:int,user=Depends(require_uat_user)):
    try: return submit_session(session_id,user['learning_user_id'])
    except ValueError as exc: raise HTTPException(status_code=409,detail=str(exc)) from exc

@router.post('/sessions/{session_id}/timeout')
def timeout(session_id:int,user=Depends(require_uat_user)):
    session=get_session(session_id,user['learning_user_id'])
    if not session: raise HTTPException(status_code=404,detail='Session not found')
    if session['remaining_seconds'] > 0: raise HTTPException(status_code=409,detail='Session time remains')
    return submit_session(session_id,user['learning_user_id'],timed_out=True)

@router.get('/sessions/{session_id}/result')
def result(session_id:int,user=Depends(require_uat_user)):
    value=get_result(session_id,user['learning_user_id'])
    if value['status']=='IN_PROGRESS': raise HTTPException(status_code=409,detail='Session is still active')
    return value

from __future__ import annotations

from app.database import get_connection
from app.repositories.vr_repository import (
    get_active_vr_papers,
    get_vr_answers_for_paper,
    get_vr_questions_for_paper,
    normalize_vr_paper_code,
)

UAT_EMAIL = "rishi@test.com"


def init_vr_mvp_tables() -> None:
    """Add only VR-scoped test-session state; existing vr_* content remains canonical."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vr_test_sessions (
                id BIGSERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                paper_code TEXT NOT NULL REFERENCES vr_papers(paper_code),
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                submitted_at TIMESTAMPTZ,
                status TEXT NOT NULL DEFAULT 'IN_PROGRESS'
                    CHECK (status IN ('IN_PROGRESS', 'SUBMITTED', 'TIMED_OUT')),
                duration_minutes INTEGER NOT NULL DEFAULT 30,
                score INTEGER,
                total INTEGER NOT NULL DEFAULT 35
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vr_test_session_answers (
                session_id BIGINT NOT NULL REFERENCES vr_test_sessions(id),
                question_number INTEGER NOT NULL,
                selected_answer TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (session_id, question_number)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vr_test_sessions_user ON vr_test_sessions(user_id, started_at DESC)")
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _paper_level(paper: dict) -> str:
    code = str(paper.get('paper_code') or '').upper()
    title = str(paper.get('title') or '').upper()
    combined = f"{code} {title}"
    if 'MASTERY' in combined or '-M' in code:
        return 'MASTERY'
    if 'INTERMEDIATE' in combined or '-I' in code:
        return 'INTERMEDIATE'
    return 'BASIC'


def list_mvp_papers() -> list[dict]:
    result = []
    for paper in get_active_vr_papers():
        count = int(paper.get('questions_count') or 0)
        result.append({
            'paper_code': paper['paper_code'],
            'title': paper['title'],
            'level': _paper_level(paper),
            'question_count': count,
            'duration_minutes': 30,
            'ready': bool(paper.get('ready')) and count == 35,
        })
    return result


def get_mvp_paper(paper_code: str) -> dict:
    normalized = normalize_vr_paper_code(paper_code)
    questions = get_vr_questions_for_paper(normalized)
    if len(questions) != 35:
        raise ValueError('Paper must contain exactly 35 questions')
    return {'paper_code': normalized, 'questions': questions, 'duration_minutes': 30}


def create_session(user_id: int, paper_code: str) -> dict:
    paper = get_mvp_paper(paper_code)
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO vr_test_sessions(user_id, paper_code, duration_minutes, total)
            VALUES (%s, %s, 30, 35)
            RETURNING id, started_at
        """, (user_id, paper['paper_code']))
        row = cur.fetchone()
        conn.commit()
        return {'session_id': int(row[0]), 'started_at': row[1].isoformat(), **paper}
    finally:
        cur.close(); conn.close()


def get_session(session_id: int, user_id: int) -> dict | None:
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, paper_code, started_at, submitted_at, status, duration_minutes, score, total,
                   GREATEST(0, duration_minutes * 60 - EXTRACT(EPOCH FROM (NOW() - started_at)))::INTEGER
            FROM vr_test_sessions WHERE id=%s AND user_id=%s
        """, (session_id, user_id))
        row = cur.fetchone()
        if not row: return None
        return {'session_id':row[0], 'paper_code':row[1], 'started_at':row[2].isoformat(),
                'submitted_at':row[3].isoformat() if row[3] else None, 'status':row[4],
                'duration_minutes':row[5], 'score':row[6], 'total':row[7], 'remaining_seconds':row[8]}
    finally:
        cur.close(); conn.close()


def save_answer(session_id: int, user_id: int, question_number: int, selected_answer: str) -> None:
    conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("SELECT status FROM vr_test_sessions WHERE id=%s AND user_id=%s FOR UPDATE", (session_id,user_id))
        row=cur.fetchone()
        if not row or row[0] != 'IN_PROGRESS': raise ValueError('Session is not active')
        cur.execute("""
            INSERT INTO vr_test_session_answers(session_id, question_number, selected_answer)
            VALUES (%s,%s,%s)
            ON CONFLICT(session_id, question_number) DO UPDATE
            SET selected_answer=EXCLUDED.selected_answer, updated_at=NOW()
        """, (session_id,int(question_number),str(selected_answer).strip().upper()))
        conn.commit()
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def submit_session(session_id: int, user_id: int, timed_out: bool=False) -> dict:
    conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("SELECT paper_code,status FROM vr_test_sessions WHERE id=%s AND user_id=%s FOR UPDATE", (session_id,user_id))
        row=cur.fetchone()
        if not row: raise ValueError('Session not found')
        paper_code,status=row
        if status == 'IN_PROGRESS':
            answers={int(a['question_number']):str(a['correct_answer']).strip().upper() for a in get_vr_answers_for_paper(paper_code)}
            cur.execute("SELECT question_number,selected_answer FROM vr_test_session_answers WHERE session_id=%s", (session_id,))
            selected={int(q):str(a).strip().upper() for q,a in cur.fetchall()}
            score=sum(1 for q,correct in answers.items() if selected.get(q)==correct)
            cur.execute("UPDATE vr_test_sessions SET submitted_at=NOW(),status=%s,score=%s WHERE id=%s",
                        ('TIMED_OUT' if timed_out else 'SUBMITTED',score,session_id))
            # Append-only final attempts; no update/delete of history.
            for q in range(1,36):
                answer=selected.get(q,'')
                correct=answers.get(q,'')
                cur.execute("""INSERT INTO vr_attempts(user_id,paper_code,question_number,student_answer,is_correct)
                               VALUES(%s,%s,%s,%s,%s)""",
                            (user_id,paper_code,q,answer,bool(answer and answer==correct)))
            conn.commit()
        return get_result(session_id,user_id)
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def get_result(session_id:int,user_id:int)->dict:
    session=get_session(session_id,user_id)
    if not session: raise ValueError('Session not found')
    answers={int(a['question_number']):a for a in get_vr_answers_for_paper(session['paper_code'])}
    questions={int(q['question_number']):q for q in get_vr_questions_for_paper(session['paper_code'])}
    conn=get_connection();cur=conn.cursor()
    try:
        cur.execute("SELECT question_number,selected_answer FROM vr_test_session_answers WHERE session_id=%s",(session_id,))
        selected={int(q):a for q,a in cur.fetchall()}
    finally:
        cur.close();conn.close()
    review=[]
    for qn in range(1,36):
        q=questions.get(qn,{})
        a=answers.get(qn,{})
        choice=selected.get(qn)
        review.append({**q,'selected_answer':choice,'correct_answer':a.get('correct_answer'),
                       'is_correct':bool(choice and choice==a.get('correct_answer')),
                       'explanation':a.get('explanation') or ''})
    return {**session,'answered_count':len(selected),'unanswered_count':35-len(selected),'review':review}

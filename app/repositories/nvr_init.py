"""
NVR table initialisation — called from startup_event in main.py.
Creates nvr_lessons, nvr_questions, nvr_lesson_questions if they don't exist.
Safe to call multiple times (CREATE TABLE IF NOT EXISTS).
"""
from app.database import get_connection


def init_nvr_tables():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS nvr_lessons (
                id           SERIAL PRIMARY KEY,
                lesson_code  TEXT,
                lesson_name  TEXT NOT NULL,
                display_name TEXT,
                topic        TEXT,
                difficulty   TEXT,
                description  TEXT,
                is_active    BOOLEAN NOT NULL DEFAULT TRUE,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE UNIQUE INDEX IF NOT EXISTS nvr_lessons_lesson_name_uidx
                ON nvr_lessons (lesson_name);

            CREATE TABLE IF NOT EXISTS nvr_questions (
                id             SERIAL PRIMARY KEY,
                question_id    TEXT NOT NULL,
                stem           TEXT NOT NULL,
                option_a       TEXT,
                option_b       TEXT,
                option_c       TEXT,
                option_d       TEXT,
                option_e       TEXT,
                correct_option CHAR(1) NOT NULL,
                topic          TEXT,
                difficulty     TEXT,
                explanation    TEXT,
                hint           TEXT,
                geometry_schema JSONB,
                created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE UNIQUE INDEX IF NOT EXISTS nvr_questions_question_id_uidx
                ON nvr_questions (question_id);

            CREATE TABLE IF NOT EXISTS nvr_lesson_questions (
                lesson_id   INTEGER NOT NULL REFERENCES nvr_lessons(id) ON DELETE CASCADE,
                question_id INTEGER NOT NULL REFERENCES nvr_questions(id) ON DELETE CASCADE,
                position    INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (lesson_id, question_id)
            );
            """
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

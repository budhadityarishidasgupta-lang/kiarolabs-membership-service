def init_words_tables():
    from app.database import get_connection

    conn = get_connection()
    cur = conn.cursor()

    # -----------------------------
    # TABLES
    # -----------------------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS words_words (
        id SERIAL PRIMARY KEY,
        word TEXT NOT NULL,
        correct_answer TEXT NOT NULL,
        hint TEXT,
        example TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS words_lesson_words (
        id SERIAL PRIMARY KEY,
        lesson_id INT NOT NULL,
        word_id INT NOT NULL,
        UNIQUE (lesson_id, word_id)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS words_attempts (
        id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        word_id INT NOT NULL,
        answer TEXT,
        correct BOOLEAN,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS words_word_stats (
        id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        word_id INT NOT NULL,
        attempts_count INT DEFAULT 0,
        correct_count INT DEFAULT 0,
        wrong_count INT DEFAULT 0,
        accuracy FLOAT DEFAULT 0,
        last_attempt_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(user_id, word_id)
    );
    """)

    # -----------------------------
    # COURSES
    # -----------------------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS words_courses (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """)

    # -----------------------------
    # LESSONS
    # -----------------------------

    cur.execute("""
    CREATE TABLE IF NOT EXISTS words_lessons (
        id SERIAL PRIMARY KEY,
        course_id INT NOT NULL,
        name TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """)

    # -----------------------------
    # SEED DATA (SAFE INSERT)
    # -----------------------------

    # -----------------------------
    # SEED COURSES
    # -----------------------------

    cur.execute("""
    INSERT INTO words_courses (id, name)
    VALUES (1, 'WordSprint Basics')
    ON CONFLICT (id) DO NOTHING;
    """)

    # -----------------------------
    # SEED LESSONS
    # -----------------------------

    cur.execute("""
    INSERT INTO words_lessons (id, course_id, name)
    VALUES (1, 1, 'Starter Words')
    ON CONFLICT (id) DO NOTHING;
    """)

    cur.execute("""
    INSERT INTO words_words (id, word, correct_answer, hint, example)
    VALUES
        (1, 'happy', 'joyful', 'feeling good', 'She felt joyful after winning'),
        (2, 'big', 'large', 'size related', 'A large building')
    ON CONFLICT (id) DO NOTHING;
    """)

    cur.execute("""
    INSERT INTO words_lesson_words (lesson_id, word_id)
    VALUES
        (1, 1),
        (1, 2)
    ON CONFLICT DO NOTHING;
    """)

    cur.execute("""
    SELECT setval(
        pg_get_serial_sequence('words_words', 'id'),
        GREATEST((SELECT COALESCE(MAX(id), 1) FROM words_words), 1),
        true
    );
    """)

    cur.execute("""
    SELECT setval(
        pg_get_serial_sequence('words_courses', 'id'),
        GREATEST((SELECT COALESCE(MAX(id), 1) FROM words_courses), 1),
        true
    );
    """)

    cur.execute("""
    SELECT setval(
        pg_get_serial_sequence('words_lessons', 'id'),
        GREATEST((SELECT COALESCE(MAX(id), 1) FROM words_lessons), 1),
        true
    );
    """)

    conn.commit()
    cur.close()
    conn.close()

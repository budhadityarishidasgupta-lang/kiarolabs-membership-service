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
    # SEED DATA (SAFE INSERT)
    # -----------------------------

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

    conn.commit()
    cur.close()
    conn.close()

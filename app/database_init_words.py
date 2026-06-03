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

    def _bootstrap_grammar_support():
        try:
            import re

            from app import entitlements as entitlements_module
            from app import main as main_module
            from app import product_catalog as product_catalog_module
            from app.product_catalog import _build_seed_product
        except Exception as exc:
            print("grammar bootstrap import failed:", exc)
            return

        try:
            grammar_seed = _build_seed_product(
                product_code="GSM",
                page_section="Online Practice / Packs",
                frontend_card_name="GrammarSprint Module",
                provider_product_key="grammar",
                provider_product_name="GrammarSprint Module",
            )

            if not any(seed.product_code == "GSM" for seed in product_catalog_module.CATALOG_SEED_PRODUCTS):
                product_catalog_module.CATALOG_SEED_PRODUCTS.append(grammar_seed)

            original_entitlement_for_product_code = product_catalog_module._entitlement_for_product_code

            def _grammar_entitlement_for_product_code(product_code: str) -> tuple[str, str]:
                code = str(product_code or "").strip().upper()
                if code == "GSM":
                    return ("member_app", "grammar")
                return original_entitlement_for_product_code(product_code)

            product_catalog_module._entitlement_for_product_code = _grammar_entitlement_for_product_code

            original_infer_subject = product_catalog_module._infer_subject

            def _grammar_infer_subject(product_code: str) -> str:
                code = str(product_code or "").strip().upper()
                if code == "GSM":
                    return "grammar"
                return original_infer_subject(product_code)

            product_catalog_module._infer_subject = _grammar_infer_subject

            if not any(item.get("app_code") == "grammar" for item in main_module.AVAILABLE_APP_CATALOG):
                main_module.AVAILABLE_APP_CATALOG.append(
                    {
                        "app_code": "grammar",
                        "label": "GrammarSprint",
                        "description": "Grammar lesson access",
                        "group": "core",
                    }
                )

            original_resolve_gumroad_app_code = main_module._resolve_gumroad_app_code

            def _grammar_resolve_gumroad_app_code(identifiers, product_name: str = ""):
                app_code = original_resolve_gumroad_app_code(identifiers, product_name=product_name)
                if app_code:
                    return app_code

                normalized_name = re.sub(r"[^a-z0-9]+", "", (product_name or "").strip().lower())
                if normalized_name in {"grammar", "grammarsprint", "grammarprint"}:
                    return "grammar"
                return None

            main_module._resolve_gumroad_app_code = _grammar_resolve_gumroad_app_code

            original_legacy_product_code_from_entitlement = main_module._legacy_product_code_from_entitlement

            def _grammar_legacy_product_code_from_entitlement(app_code: str | None, mock_test_id: str | None) -> str | None:
                normalized_app_code = str(app_code or "").strip().lower()
                if normalized_app_code == "grammar":
                    return "GSM"
                return original_legacy_product_code_from_entitlement(app_code, mock_test_id)

            main_module._legacy_product_code_from_entitlement = _grammar_legacy_product_code_from_entitlement

            entitlements_module.ONLINE_PRACTICE_APP_CODES.add("grammar")
            entitlements_module.ACTIVE_ONLINE_PRACTICE_PERMALINK_APP_CODE.setdefault("grammar", "grammar")

            if not getattr(main_module.app.state, "grammar_router_registered", False):
                from app.practice.grammar_router import router as grammar_router

                main_module.app.include_router(grammar_router)
                main_module.app.state.grammar_router_registered = True

        except Exception as exc:
            print("grammar bootstrap failed:", exc)

    _bootstrap_grammar_support()

    conn.commit()
    cur.close()
    conn.close()

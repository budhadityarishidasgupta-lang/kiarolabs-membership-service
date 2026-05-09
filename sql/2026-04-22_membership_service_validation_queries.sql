-- Validation queries for membership-service additive parity prep.
-- Read-only checks only. Safe to run before or after the additive migration.

-- Verify new columns on public.attempts
SELECT
    table_schema,
    table_name,
    column_name,
    data_type,
    udt_name,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'attempts'
  AND column_name IN (
      'question_id',
      'session_id',
      'time_taken_ms',
      'submitted_at',
      'contract_version'
  )
ORDER BY column_name;

-- Verify new columns on public.spelling_attempts
SELECT
    table_schema,
    table_name,
    column_name,
    data_type,
    udt_name,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'spelling_attempts'
  AND column_name IN (
      'lesson_id',
      'question_id',
      'session_id',
      'submitted_at',
      'contract_version'
  )
ORDER BY column_name;

-- Sample contract rows from the compatibility view
SELECT *
FROM public.spelling_words_contract
LIMIT 10;

-- Row counts for current production tables
SELECT COUNT(*) AS attempts_count
FROM public.attempts;

SELECT COUNT(*) AS words_attempts_count
FROM public.words_attempts;

SELECT COUNT(*) AS spelling_attempts_count
FROM public.spelling_attempts;

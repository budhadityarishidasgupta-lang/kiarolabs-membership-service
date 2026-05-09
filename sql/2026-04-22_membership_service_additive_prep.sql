-- Membership-service parity prep
-- Additive only migration for shared production Postgres.
-- No renames, no drops, no archival, and no destructive backfills.
-- Existing production applications are expected to continue working unchanged.
-- public.words_attempts is intentionally untouched in this prep step.

BEGIN;

-- 1) Synonym attempt metadata on public.attempts
-- Add nullable/additive columns only. Existing columns remain unchanged.
ALTER TABLE IF EXISTS public.attempts
    ADD COLUMN IF NOT EXISTS question_id UUID,
    ADD COLUMN IF NOT EXISTS session_id UUID,
    ADD COLUMN IF NOT EXISTS time_taken_ms INTEGER,
    ADD COLUMN IF NOT EXISTS submitted_at TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS contract_version VARCHAR(10);

-- 2) Spelling attempt metadata on public.spelling_attempts
-- Add nullable/additive columns only.
-- Foreign keys are intentionally skipped for production safety until live data is validated.
ALTER TABLE IF EXISTS public.spelling_attempts
    ADD COLUMN IF NOT EXISTS lesson_id INTEGER,
    ADD COLUMN IF NOT EXISTS question_id UUID,
    ADD COLUMN IF NOT EXISTS session_id UUID,
    ADD COLUMN IF NOT EXISTS submitted_at TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS contract_version VARCHAR(10);

-- 3) Spelling compatibility view
-- Non-destructive contract view over the live spelling words table.
-- pattern remains pattern; it is not remapped to definition.
-- missing_letter_mask stays NULL here because it is engine-generated.
CREATE OR REPLACE VIEW public.spelling_words_contract AS
SELECT
    sw.word_id AS id,
    sw.word,
    sw.level AS difficulty,
    sw.hint AS pattern_hint,
    sw.pattern,
    sw.example_sentence AS sample_sentence,
    sw.course_id,
    sw.lesson_name,
    NULL::TEXT AS missing_letter_mask
FROM public.spelling_words AS sw;

COMMIT;

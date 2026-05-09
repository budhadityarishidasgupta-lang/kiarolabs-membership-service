import json
import logging
import random


logger = logging.getLogger(__name__)

MIN_VISIBLE_OPTIONS = 4
DISTRACTOR_COUNT = MIN_VISIBLE_OPTIONS - 1


def _clean_option(value, *, headword=None):
    text = str(value or "").strip()
    if not text:
        return None
    if text.isdigit():
        return None
    if headword and text.lower() == str(headword).strip().lower():
        return None
    return text


def normalize_synonym_list(synonyms, *, headword=None):
    if isinstance(synonyms, str):
        raw_values = synonyms.split(",")
    elif isinstance(synonyms, list):
        raw_values = synonyms
    else:
        return []

    normalized = []
    seen = set()
    for value in raw_values:
        cleaned = _clean_option(value, headword=headword)
        if not cleaned:
            continue
        lowered = cleaned.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        normalized.append(cleaned)
    return normalized


def validate_question_integrity(*, headword, correct_answer, options):
    issues = []

    if not isinstance(options, list):
        return False, ["options_not_list"]

    if not isinstance(correct_answer, str) or not correct_answer.strip():
        issues.append("missing_correct_answer")

    cleaned_options = []
    seen = set()
    for option in options:
        cleaned = _clean_option(option, headword=headword)
        if not cleaned:
            issues.append("empty_or_invalid_option")
            continue
        lowered = cleaned.lower()
        if lowered in seen:
            issues.append("duplicate_option")
            continue
        seen.add(lowered)
        cleaned_options.append(cleaned)

    if len(cleaned_options) < MIN_VISIBLE_OPTIONS:
        issues.append("too_few_options")

    if isinstance(correct_answer, str) and correct_answer.strip():
        correct_lower = correct_answer.strip().lower()
        if correct_lower not in {option.lower() for option in cleaned_options}:
            issues.append("correct_answer_missing_from_options")

    return len(issues) == 0, issues


def audit_synonym_question_integrity(*, headword, correct_answer, options):
    is_valid, issues = validate_question_integrity(
        headword=headword,
        correct_answer=correct_answer,
        options=options,
    )
    return {
        "ok": is_valid,
        "issues": issues,
        "headword": headword,
        "correct_answer": correct_answer,
        "options": options,
    }


def _build_distractor_pool(cur, *, word_id, correct_answer, headword, randomize):
    order_sql = "RANDOM()" if randomize else "word_id ASC"
    cur.execute(
        f"""
        SELECT synonyms
        FROM public.words
        WHERE word_id != %s
          AND synonyms IS NOT NULL
          AND TRIM(synonyms) <> ''
        ORDER BY {order_sql}
        LIMIT 100
        """,
        (word_id,),
    )

    distractor_pool = []
    seen = set()
    for row in cur.fetchall():
        for candidate in normalize_synonym_list(row[0], headword=headword):
            lowered = candidate.lower()
            if lowered == correct_answer.lower():
                continue
            if lowered in seen:
                continue
            seen.add(lowered)
            distractor_pool.append(candidate)
    return distractor_pool


def _select_options(correct_answer, distractor_pool, *, randomize):
    if len(distractor_pool) < DISTRACTOR_COUNT:
        return None

    selected_distractors = (
        random.sample(distractor_pool, DISTRACTOR_COUNT)
        if randomize
        else distractor_pool[:DISTRACTOR_COUNT]
    )
    options = selected_distractors + [correct_answer]
    if randomize:
        random.shuffle(options)
    return options


def _log_validation_failure(*, word_id, headword, correct_answer, options, issues, stage):
    logger.warning(
        "[WORDS_VALIDATION_FAIL] %s",
        json.dumps(
            {
                "stage": stage,
                "word_id": word_id,
                "headword": headword,
                "correct_answer": correct_answer,
                "options": options,
                "issues": issues,
            },
            sort_keys=True,
        ),
    )


def build_validated_synonym_question(cur, *, word_id, headword, synonyms):
    synonym_list = normalize_synonym_list(synonyms, headword=headword)
    if not synonym_list:
        return None

    correct_answer = synonym_list[0]

    random_pool = _build_distractor_pool(
        cur,
        word_id=word_id,
        correct_answer=correct_answer,
        headword=headword,
        randomize=True,
    )
    options = _select_options(correct_answer, random_pool, randomize=True)
    if options:
        ok, issues = validate_question_integrity(
            headword=headword,
            correct_answer=correct_answer,
            options=options,
        )
        if ok:
            return {"correct_answer": correct_answer, "options": options}
        _log_validation_failure(
            word_id=word_id,
            headword=headword,
            correct_answer=correct_answer,
            options=options,
            issues=issues,
            stage="primary",
        )

    rebuilt_pool = _build_distractor_pool(
        cur,
        word_id=word_id,
        correct_answer=correct_answer,
        headword=headword,
        randomize=False,
    )
    rebuilt_options = _select_options(correct_answer, rebuilt_pool, randomize=False)
    if rebuilt_options:
        ok, issues = validate_question_integrity(
            headword=headword,
            correct_answer=correct_answer,
            options=rebuilt_options,
        )
        if ok:
            return {"correct_answer": correct_answer, "options": rebuilt_options}
        _log_validation_failure(
            word_id=word_id,
            headword=headword,
            correct_answer=correct_answer,
            options=rebuilt_options,
            issues=issues,
            stage="rebuild",
        )

    safe_options = [correct_answer]
    for candidate in rebuilt_pool:
        if candidate.lower() == correct_answer.lower():
            continue
        safe_options.append(candidate)
        if len(safe_options) == MIN_VISIBLE_OPTIONS:
            break

    ok, issues = validate_question_integrity(
        headword=headword,
        correct_answer=correct_answer,
        options=safe_options,
    )
    if ok:
        return {"correct_answer": correct_answer, "options": safe_options}

    _log_validation_failure(
        word_id=word_id,
        headword=headword,
        correct_answer=correct_answer,
        options=safe_options,
        issues=issues,
        stage="safe_fallback",
    )
    return None

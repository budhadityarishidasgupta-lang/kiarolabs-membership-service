import json
import logging
import random


logger = logging.getLogger(__name__)

MIN_VISIBLE_OPTIONS = 4
MIN_DISTRACTOR_BUFFER = 2


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


def _normalize_correct_answers(*, correct_answer=None, correct_answers=None, headword=None):
    if correct_answers is not None:
        normalized = normalize_synonym_list(correct_answers, headword=headword)
    elif correct_answer is not None:
        normalized = normalize_synonym_list([correct_answer], headword=headword)
    else:
        normalized = []
    return normalized


def validate_question_integrity(*, headword, correct_answer=None, correct_answers=None, options=None):
    normalized_correct_answers = _normalize_correct_answers(
        correct_answer=correct_answer,
        correct_answers=correct_answers,
        headword=headword,
    )
    issues = []

    if not isinstance(options, list):
        return False, ["options_not_list"]

    if not normalized_correct_answers:
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

    option_lowers = {option.lower() for option in cleaned_options}
    for accepted_answer in normalized_correct_answers:
        if accepted_answer.lower() not in option_lowers:
            issues.append("correct_answer_missing_from_options")
            break

    return len(issues) == 0, issues


def audit_synonym_question_integrity(*, headword, correct_answer=None, correct_answers=None, options=None):
    normalized_correct_answers = _normalize_correct_answers(
        correct_answer=correct_answer,
        correct_answers=correct_answers,
        headword=headword,
    )
    is_valid, issues = validate_question_integrity(
        headword=headword,
        correct_answers=normalized_correct_answers,
        options=options,
    )
    return {
        "ok": is_valid,
        "issues": issues,
        "headword": headword,
        "correct_answer": normalized_correct_answers[0] if normalized_correct_answers else correct_answer,
        "correct_answers": normalized_correct_answers,
        "options": options,
    }


def _build_distractor_pool(cur, *, word_id, correct_answers, headword, randomize):
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
    correct_lowers = {value.lower() for value in correct_answers}
    for row in cur.fetchall():
        for candidate in normalize_synonym_list(row[0], headword=headword):
            lowered = candidate.lower()
            if lowered in correct_lowers:
                continue
            if lowered in seen:
                continue
            seen.add(lowered)
            distractor_pool.append(candidate)
    return distractor_pool


def _select_options(correct_answers, distractor_pool, *, randomize):
    normalized_correct_answers = list(correct_answers)
    distractor_count = max(MIN_VISIBLE_OPTIONS - len(normalized_correct_answers), MIN_DISTRACTOR_BUFFER)
    if len(distractor_pool) < distractor_count:
        return None

    selected_distractors = (
        random.sample(distractor_pool, distractor_count)
        if randomize
        else distractor_pool[:distractor_count]
    )
    options = selected_distractors + normalized_correct_answers
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
    correct_answers = normalize_synonym_list(synonyms, headword=headword)
    if not correct_answers:
        return None

    correct_answer = correct_answers[0]

    random_pool = _build_distractor_pool(
        cur,
        word_id=word_id,
        correct_answers=correct_answers,
        headword=headword,
        randomize=True,
    )
    options = _select_options(correct_answers, random_pool, randomize=True)
    if options:
        ok, issues = validate_question_integrity(
            headword=headword,
            correct_answers=correct_answers,
            options=options,
        )
        if ok:
            return {
                "correct_answer": correct_answer,
                "correct_answers": correct_answers,
                "options": options,
                "selection_mode": "multiple" if len(correct_answers) > 1 else "single",
                "required_answers_count": len(correct_answers),
            }
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
        correct_answers=correct_answers,
        headword=headword,
        randomize=False,
    )
    rebuilt_options = _select_options(correct_answers, rebuilt_pool, randomize=False)
    if rebuilt_options:
        ok, issues = validate_question_integrity(
            headword=headword,
            correct_answers=correct_answers,
            options=rebuilt_options,
        )
        if ok:
            return {
                "correct_answer": correct_answer,
                "correct_answers": correct_answers,
                "options": rebuilt_options,
                "selection_mode": "multiple" if len(correct_answers) > 1 else "single",
                "required_answers_count": len(correct_answers),
            }
        _log_validation_failure(
            word_id=word_id,
            headword=headword,
            correct_answer=correct_answer,
            options=rebuilt_options,
            issues=issues,
            stage="rebuild",
        )

    safe_options = list(correct_answers)
    for candidate in rebuilt_pool:
        if candidate.lower() in {value.lower() for value in correct_answers}:
            continue
        safe_options.append(candidate)
        if len(safe_options) == MIN_VISIBLE_OPTIONS:
            break

    ok, issues = validate_question_integrity(
        headword=headword,
        correct_answers=correct_answers,
        options=safe_options,
    )
    if ok:
        return {
            "correct_answer": correct_answer,
            "correct_answers": correct_answers,
            "options": safe_options,
            "selection_mode": "multiple" if len(correct_answers) > 1 else "single",
            "required_answers_count": len(correct_answers),
        }

    _log_validation_failure(
        word_id=word_id,
        headword=headword,
        correct_answer=correct_answer,
        options=safe_options,
        issues=issues,
        stage="safe_fallback",
    )
    return None

# AGENTS.md

This repository contains platform membership and entitlement logic for Kiarolabs / WordSprint / SprintHub.

## Agent Rules

- Treat [docs/GUMROAD_ENTITLEMENT_ARCHITECTURE.md](docs/GUMROAD_ENTITLEMENT_ARCHITECTURE.md) as the source of truth for purchase-to-access flow.
- Do not add direct Gumroad dependencies into learning apps.
- Do not introduce shared generic learning tables or cross-app learning SQL joins.
- Keep Maths, Spelling, Grammar, Words / Synonyms, Comprehension, and Verbal Reasoning isolated.
- Keep entitlement resolution in the platform layer, not in frontend state.
- If a proposed change alters access control, product mapping, or entitlement behavior, stop and obtain explicit approval first.

## Safe Change Scope

Allowed without a platform architecture review:

- Documentation updates.
- Bug fixes that do not alter the entitlement contract.
- Repository-level refactors that preserve existing access behavior.

## Review Reminder

Before shipping any change, verify that the implementation still matches the locked entitlement architecture and the relevant module remains isolated from the others.


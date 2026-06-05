# Gumroad Entitlement Architecture

## Kiarolabs / WordSprint / SprintHub

Status: LOCKED ARCHITECTURE CONTRACT

This document defines how Gumroad purchases map to Kiarolabs products, entitlements, and unlocked routes.

Any change to this architecture is a breaking platform decision and must be explicitly approved.

---

## 1. Core Principle

Gumroad handles checkout.

Kiarolabs handles access.

A Gumroad purchase grants a Kiarolabs platform entitlement.

The entitlement unlocks the relevant Kiarolabs product, printable pack, or practice module.

Learning apps must never directly depend on Gumroad.

---

## 2. Platform Responsibilities

### Gumroad

Gumroad is responsible for:

- Product checkout
- Payment collection
- Purchase confirmation
- Digital product sale record
- Buyer email capture
- Gumroad product identifier or permalink

### Kiarolabs

Kiarolabs is responsible for:

- User accounts
- Product catalogue
- Product access
- Entitlements
- Locked and unlocked dashboard states
- Learning app routing
- Printable paper access pages
- Online practice module access

---

## 3. Identity Model

The Kiarolabs user account is the source of identity.

A Gumroad buyer is linked to a Kiarolabs user by email address.

Conceptual flow:

```text
Gumroad buyer email
-> Kiarolabs user email
-> Kiarolabs user_id
-> active entitlements
-> unlocked product or module
```

The email address is the bridging key for purchase-to-access resolution.

The platform may resolve identity through login, membership records, or webhook-backed entitlement state, but the final unlock decision belongs to Kiarolabs.

---

## 4. Entitlement Model

A Gumroad purchase must resolve to a Kiarolabs product code first.

That product code then resolves to one or more entitlements.

Entitlements are the authoritative access unit inside Kiarolabs.

Examples:

- A practice module entitlement may unlock `math`, `spelling`, `grammar`, or `general`.
- A printable entitlement may unlock a paper pack or printable route.
- A future module must be added as a new Kiarolabs entitlement, not as a Gumroad-specific special case.

Gumroad identifiers are not the source of truth after the purchase is mapped.

---

## 5. Product Isolation Rules

Each learning app remains logically and data-isolated.

This means:

- Maths only reads and writes maths tables.
- Spelling only reads and writes spelling tables.
- Grammar only reads and writes grammar tables.
- Words / Synonyms only reads and writes its own namespace.
- Comprehension only reads and writes comprehension tables.
- Verbal Reasoning only reads and writes verbal reasoning tables.
- Future modules must follow the same pattern.

No cross-app SQL joins are permitted for learning data.

No shared generic learning attempts table is permitted.

No shared generic progress table is permitted.

---

## 6. Checkout To Unlock Flow

The intended flow is:

1. User completes checkout in Gumroad.
2. Gumroad emits or records the purchase identifier.
3. Kiarolabs maps the purchase to a product code.
4. Kiarolabs maps the product code to entitlements.
5. Entitlements are stored and resolved by Kiarolabs.
6. The UI shows the unlocked product, printable pack, or practice module.
7. Learning routes become available only when entitlement checks pass.

This flow keeps checkout separate from access control.

---

## 7. Learning App Boundary

Learning apps must treat access as an external platform concern.

They may:

- Ask Kiarolabs whether the current user has access.
- Read their own app-specific tables.
- Render locked or unlocked states based on entitlement results.

They must not:

- Query Gumroad directly for access decisions.
- Infer access from frontend state.
- Write entitlement data into app-specific learning tables.
- Create payment logic inside the learning engine.

---

## 8. Allowed Evolution

This architecture can evolve only by explicit platform decision.

Safe evolution examples:

- Add a new product code for a new module.
- Add a new entitlement mapping for a printable pack.
- Add a new learning app with its own isolated tables.
- Add a new unlock route after the entitlement contract is updated.

Breaking evolution examples:

- Making a learning app depend directly on Gumroad.
- Sharing one generic attempts table across modules.
- Reusing another app's learning tables.
- Bypassing entitlement resolution.
- Adding product logic inside frontend-only state.

---

## 9. Operational Notes

- Use Kiarolabs product codes as the stable internal access contract.
- Use Gumroad identifiers only as purchase inputs.
- Keep unlock logic centralized in Kiarolabs membership and product access services.
- Keep learning repositories and engines focused only on their own module data.
- Document any future entitlement changes before shipping them.

---

## 10. Governance Rule

If a proposed change conflicts with this document, the document wins until the platform owner explicitly approves the change.


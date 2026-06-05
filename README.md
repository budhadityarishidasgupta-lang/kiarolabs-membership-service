# kiarolabs-membership-service

Membership, entitlement, and access service for the Kiarolabs / WordSprint / SprintHub platform.

## Architecture

This repository follows the locked Gumroad to Kiarolabs entitlement contract described in:

- [docs/GUMROAD_ENTITLEMENT_ARCHITECTURE.md](docs/GUMROAD_ENTITLEMENT_ARCHITECTURE.md)

## Key principles

- Gumroad handles checkout.
- Kiarolabs handles access.
- Purchases map to Kiarolabs product codes first.
- Product codes map to entitlements.
- Entitlements unlock products, printable packs, and practice modules.
- Learning apps remain data-isolated and do not depend directly on Gumroad.

## Maintenance rule

Any change to purchase-to-access architecture is a platform decision and must be approved before implementation.


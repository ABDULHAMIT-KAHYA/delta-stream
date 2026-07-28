# Security

## Status

DeltaStream v0.30.0 has **not** undergone an independent professional security audit. It should be treated as a production-candidate/public-preview library, not as a cryptographic security boundary.

## Existing defensive behavior

The protocol currently includes or tests:

- wire-format version checks;
- known-flag validation;
- CRC32 corruption detection;
- schema-hash validation;
- sequence/base-state validation;
- duplicate/stale packet suppression;
- wire and logical payload limits;
- bounded decompression output;
- malformed-packet tests;
- property/randomized tests;
- recovery rather than unsafe application when the state chain breaks.

## What CRC32 does not provide

CRC32 detects accidental corruption. It does **not** provide authentication, confidentiality, or protection against a malicious peer that can construct packets.

Use the authentication, authorization, and encryption mechanisms of the underlying transport, and validate trust boundaries at the application level.

## Reporting vulnerabilities

Before a public repository is announced, replace this section with the maintainer's private security contact or GitHub private vulnerability-reporting instructions.

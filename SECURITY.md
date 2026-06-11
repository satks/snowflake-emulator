# Security Policy

## Scope

Snowflake Emulator is a **development and testing tool**. It intentionally has **no authentication or authorization** and must never be exposed to the public internet or used with production data. Run it only on localhost, in CI, or inside trusted private networks.

## Supported versions

Only the latest release receives fixes.

## Reporting a vulnerability

If you find a vulnerability that affects users beyond the documented no-auth design (e.g., container escape, path traversal via stage files, dependency CVEs), please report it privately via [GitHub Security Advisories](https://github.com/satks/snowflake-emulator/security/advisories/new) rather than a public issue. You can expect an initial response within a week.

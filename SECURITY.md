# Security Policy

## Supported Versions

Security fixes are applied to the latest minor release line. Older versions do
not receive backported fixes; upgrade to the newest release to stay covered.

| Version | Supported |
|---|---|
| Latest release (0.x latest) | Yes |
| Older releases | No |

## Reporting a Vulnerability

Please **do not** open a public GitHub issue for security problems.

Instead, report vulnerabilities privately through
[GitHub Security Advisories](https://github.com/plurima-io/plurima/security/advisories/new):

1. Go to the repository's **Security** tab.
2. Click **Report a vulnerability**.
3. Describe the issue: affected module/version, reproduction steps or a proof of
   concept, and the impact you believe it has.

You should receive an acknowledgement within a few business days. Once the
report is triaged, we will work with you on a fix and coordinate disclosure —
the advisory stays private until a fixed release is available.

## Scope

In scope:

- The published artifacts: `kafka-plurima-core`, `kafka-plurima-metrics`,
  `kafka-plurima-spring-boot-starter`.
- Vulnerabilities in Plurima's own code (e.g. record handling, DLT publishing,
  header propagation, Spring auto-configuration).

Out of scope:

- Vulnerabilities in Apache Kafka itself, the Kafka clients, Micrometer, or
  Spring — report those upstream.
- Issues requiring a malicious broker or an already-compromised application
  classpath, unless Plurima makes the impact meaningfully worse.

Thank you for helping keep Plurima users safe.

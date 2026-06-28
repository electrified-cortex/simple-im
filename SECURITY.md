# Security Policy

## Supported versions

The latest `1.x` release is supported. Fixes land on `main` and ship in the next tag.

## Reporting a vulnerability

Please report security issues **privately** — do not open a public issue.

Use GitHub's private vulnerability reporting: the **Security** tab → **Report a
vulnerability**. Include a description, affected version/commit, and a
reproduction if you have one. You'll get an acknowledgement and a fix timeline.

## Security model — read before deploying

simple-im is built for a **trusted internal network** (a LAN, a private Docker
network, or `localhost`). Operators are responsible for the perimeter:

- **No built-in TLS.** The hub speaks plain HTTP and requires `--insecure-http`
  to start. Terminate TLS at a reverse proxy (Caddy, nginx) and bind the hub to
  a private interface. Tokens travel in `Authorization` headers, so an
  unencrypted public network would expose them.
- **Tokens are bearer secrets.** Listen, participant, and governor tokens grant their
  holder's access. Treat them like passwords; never commit them. The
  participant skill writes them to `service.*` files that are gitignored.
- **Trust state is stored unencrypted.** `sim-tokens.db` (SQLite) holds tokens,
  grants, identities, and attachment blobs in plaintext. Protect it with
  filesystem permissions and keep it out of version control.
- **No rate limiting.** Enforce request limits at the reverse proxy for anything
  beyond a trusted network.
- **Trust model.** Messaging is grant-gated. With no governor, a grant requires
  the recipient's consent; with a governor, the governor approves grants. There
  is no owner/admin backdoor — governorship is obtained only by claim/election/
  transfer among participants.

Within that intended-use boundary, reports of auth bypass, privilege escalation,
grant-gate bypass, cross-participant data exposure, or denial-of-service via a
single request are in scope and appreciated.

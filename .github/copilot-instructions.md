# Code Review Criteria

When reviewing pull requests in this repository, check the following areas:

| Area | What to check |
| ---- | ------------- |
| Async error handling | Uncaught promise rejections, missing error callbacks, swallowed errors in streams, missing `.on('error')` handlers |
| Stream handling | Backpressure issues, proper cleanup on error, no leaked file descriptors, correct use of transform/pipeline |
| Dependency pinning | Git-based deps (`arsenal`, `vaultclient`, `bucketclient`, `werelogs`, `httpagent`) must pin to a tag, not a branch |
| Logging | Proper use of `werelogs` — no `console.log` in production code, log levels match severity |
| Async/await usage | Prefer `async`/`await` over raw promise chains (`.then`/`.catch`) and callbacks for new code; ensure `await` is not missing on async calls |
| Import placement | All `require()` statements must be at the top of the file, never inside functions, blocks, or `describe` scopes |
| Config & env vars | Backward compatibility of environment variables, sensible defaults, documented new variables |
| Production safety | Dry-run support preserved, resumption markers (`KEY_MARKER`, `VERSION_ID_MARKER`) handled correctly, batch limits respected |
| Security | No credentials or secrets in code, safe handling of user-supplied input, OWASP-relevant issues |
| Breaking changes | Changes to script CLI arguments, environment variable contracts, or client interfaces |
| Test coverage | New logic should have corresponding unit tests, mocks should be realistic |

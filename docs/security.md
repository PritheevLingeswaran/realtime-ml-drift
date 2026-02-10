# Security considerations

- Validate inputs strictly (Pydantic schemas).
- Emit structured logs but avoid leaking sensitive raw payloads.
- For production:
  - Add authn/authz to API endpoints
  - Use TLS and mTLS for internal traffic
  - Encrypt persisted snapshots
  - Add rate limiting and request size limits

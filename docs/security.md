# Security considerations

- Validate inputs strictly (Pydantic schemas).
- Emit structured logs but avoid logging full raw event payloads by default.
- Enforce request size limit (`api.max_request_bytes`) to prevent memory abuse.
- Apply basic per-IP request throttling (`api.max_requests_per_minute_per_ip`) as a first-line guard.
- Protect admin endpoints using API key (`security.admin_api_key`) in development.
- For production:
  - Add authn/authz to API endpoints
  - Use TLS and mTLS for internal traffic
  - Encrypt persisted snapshots
  - Move secrets to secret manager/KMS and rotate automatically
  - Add immutable audit sink for admin actions and threshold changes

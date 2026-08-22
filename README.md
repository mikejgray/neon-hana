# HANA
HANA (HTTP API for Neon Applications) provides a unified front-end for 
accessing services in a [Neon DIANA](https://github.com/NeonGeckoCom/neon-diana-utils) deployment. This API should generally 
be hosted as part of a Diana deployment to safely expose services to outside
traffic.

Full API documentation is automatically generated and accessible at `/docs`.

## Configuration
User configuration belongs in `diana.yaml`, mounted in the container path 
`/config/neon/`. An example user configuration could be:
```yaml
MQ:
  server: mq.mydomain.com
hana:
  server_host: '0.0.0.0'
  port: 8080
  mq_default_timeout: 10
  access_token_ttl: 86400  # 1 day
  refresh_token_ttl: 604800  # 1 week
  requests_per_minute: 60  # All other requests (auth, registration, etc) also count towards this limit
  auth_requests_per_minute: 6  # This counts valid and invalid requests from an IP address
  registration_requests_per_hour: 4  # This is low to prevent malicious user creation that will pollute the database
  access_token_secret: a800445648142061fc238d1f84e96200da87f4f9fa7835cac90db8b4391b117b
  refresh_token_secret: 833d369ac73d883123743a44b4a7fe21203cffc956f4c8fec712e71aafa8e1aa
  jwt_issuer: neon.ai  # Used in the `iss` field of generated JWT tokens.
  fastapi_title: "My HANA API Host"
  fastapi_summary: "Personal HTTP API to access my DIANA backend."
  disable_auth: True  # If true, no authentication will be attempted; all connections will be allowed
  stt_max_length_encoded: 500000  # Arbitrary limit that is larger than any expected voice command
  tts_max_words: 128  # Arbitrary limit that is longer than any default LLM token limit
  enable_email: True  # Disabled by default; anyone with access to the API will be able to send emails from the configured address
  max_streaming_clients: -1  # Maximum audio streaming clients allowed (including 0). Default unset value allows infinite clients
  node_registry_path: /config/neon/hana/node_registry.json  # Persistent Node registry used to route notifications. Defaults to `$XDG_CONFIG_HOME/neon/hana/node_registry.json`
```
It is recommended to generate unique values for configured tokens, these are 32
bytes in hexadecimal representation.

## Deployment
You can build a Docker container from this repository, or pull a built container
from the GitHub Container Registry. Start Hana via:
```shell
docker run -p 8080:8080 -v ~/.config/neon:/config/neon ghcr.io/neongeckocom/neon-hana
```
> This assumes you have configuration defined in `~/.config/neon/diana.yaml` and
  are using the default port 8080

## Usage
Full API documentation is available at `/docs`.

### Registration
The `/auth/register` endpoint may be used to create a new user if auth is enabled.
If auth is disabled, any login requests will return a successful response.

### Token Generation
The `/auth/login` endpoint should  be used to generate a `client_id`, 
`access_token`, and `refresh_token`. The `access_token` should be included in 
every request and upon expiration of the `access_token`, a new token can be 
obtained from the `auth/refresh` endpoint. Tokens are client-specific and clients
are expected to include the same `client_id` and valid tokens for that client
with every request.

### Token Management
`access_token`s should not be saved to persistent storage; they are only valid
for a short period of time and a new `access_token` should be generated for
every new session.

`refresh_token`s should be saved to persistent storage and used to generate a new
`access_token` and `refresh_token` at the beginning of a session, or when the
current `access_token` expires. A `refresh_token` may only be used once; a new
`refresh_token` returned from the `/auth/refresh` endpoint will replace the one
included in the request.

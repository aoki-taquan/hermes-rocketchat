# Live end-to-end test (Rocket.Chat)

Spins up a real Rocket.Chat + MongoDB stack and exercises the gateway against
it: realtime receive (DDP `@mention` + DM), reply send, and the standalone cron
send path.

```sh
# 1. Start the stack (first run pulls ~1.5 GB; boot takes a few minutes)
docker compose -f tests/e2e/docker-compose.yml up -d

# 2. Run the live test (waits for the server, provisions a tester user)
HERMES_AGENT=~/.hermes/hermes-agent PYTHONPATH=$HERMES_AGENT \
  "$HERMES_AGENT/venv/bin/python" tests/e2e/test_live.py

# 3. Tear down
docker compose -f tests/e2e/docker-compose.yml down -v
```

Admin credentials default to `admin` / `admin1234`; the server is on
`http://localhost:13000`. Override with `RC_URL`, `RC_ADMIN_USER`,
`RC_ADMIN_PASS`.

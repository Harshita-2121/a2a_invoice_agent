# TDS GA3: A2A Invoice Action Agent

A deployable Python/FastAPI agent that exposes the assignment's A2A 1.0 HTTP+JSON routes. It proposes evidence-backed invoice actions, waits for the grader's verdict, and creates receipts only for accepted proposals. It does not connect to a bank or make real payments.

## Important before you submit

- The grader's URL field must contain your real public HTTPS base URL, ending in `/a2a/`, for example `https://YOUR-SERVICE.onrender.com/a2a/`.
- Only the Agent Card is public. The card is discovered at the origin-level path `https://YOUR-SERVICE.onrender.com/.well-known/agent-card.json`.
- Do not leave `https://delegate.example/a2a/` in the assignment; that is only an example.
- Configure `GEMINI_API_KEY` in the hosting provider's environment settings. A free Google AI Studio API key can be created at https://aistudio.google.com/apikey. Do not commit the key to GitHub.
- `GEMINI_MODEL` defaults to `gemini-3.8-flash`; set it to another available Gemini API model if your account does not have access.

## Deploy on Render

1. Download and extract this project, then create a new **private** GitHub repository and upload the files in this folder. Keep `.env` and API keys out of the repository.
2. In Render, create a new Web Service from that repository. Build command: `pip install -r requirements.txt`. Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`. Choose the Free plan for a short assignment run.
3. Create a Render PostgreSQL database and set the service's `DATABASE_URL` to that database's internal connection string. This preserves idempotency, task history, decision cache, and paid-state across service restarts. Render free PostgreSQL has a limited lifetime, so it is appropriate only for a short assignment window.
4. Add environment variables:
   - `A2A_BASE_PATH` = `/a2a`
   - `PUBLIC_BASE_URL` = `https://YOUR-ACTUAL-SERVICE.onrender.com/a2a/`
   - `GEMINI_API_KEY` = your Google AI Studio key
   - `GEMINI_MODEL` = `gemini-3.8-flash`
5. Wait until the service deploy is live. Open `https://YOUR-ACTUAL-SERVICE.onrender.com/.well-known/agent-card.json`. Confirm its `supportedInterfaces[0].url` is exactly `https://YOUR-ACTUAL-SERVICE.onrender.com/a2a/` and the page returns JSON.
6. Paste that exact base URL ending with `/a2a/` into the TDS GA3 field. Use the grader's `Check` only after this test works. Remember the last attempt is reserved for Save.

Render currently offers free web services and free PostgreSQL, but free web services can sleep and local files are ephemeral. Use PostgreSQL rather than SQLite for the deployed task state. See https://render.com/docs/free and https://render.com/docs/deploy-fastapi.

## Local smoke test

```bash
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# Linux/WSL:
# source .venv/bin/activate
pip install -r requirements.txt
# Set GEMINI_API_KEY and PUBLIC_BASE_URL as environment variables, then:
uvicorn main:app --host 127.0.0.1 --port 8000
```

A local URL is **not** suitable for the grader because it requires public HTTPS.

## Routes

- `GET /.well-known/agent-card.json` (public)
- `POST /a2a/message:send`
- `GET /a2a/tasks`
- `GET /a2a/tasks/{id}`
- `POST /a2a/tasks/{id}:cancel`

API calls require `A2A-Version: 1.0` and a Bearer token. The canonical POST request media type is `application/a2a+json`; for interoperability the server also accepts `application/json` request envelopes. Bodyless GET routes do not require a `Content-Type`. Responses use `application/a2a+json`.

## Design notes

- Message idempotency is keyed by `(principal, messageId)` and the hash of the canonical message only. Reordered JSON keys and changes to `configuration` do not trigger a second model call. Reusing a message ID with changed message content returns `409 IDEMPOTENCY_CONFLICT`.
- Valid model-backed package decisions are cached by canonical package content and relevant policy context, excluding batch/message/task IDs and identity-only package IDs. Heuristic fallbacks are not cached, so an API outage will not poison later attempts.
- Every task is scoped to a hash of its Bearer token. A different user cannot read, continue, or cancel it.
- Only an `ACCEPTED` grader result can become an execution receipt. A settlement is recorded in the per-user paid-invoice ledger only after acceptance, so an ordinary retry cannot repeat settlement.
- The model reads the whole uncached part of each invoice batch in one request. It is instructed to ignore instructions embedded in invoice documents and cite exact bracketed references.
- If the model API is not configured or fails, the agent uses a conservative heuristic fallback. That fallback is for protocol resilience only and may score worse than the hosted model path.


## Deployed endpoint smoke test (Windows PowerShell)

Run `smoke-test.ps1` after deployment to send one synthetic invoice package and verify that the endpoint returns HTTP 200. It creates a test task only; it does not execute payments and does not consume a GA3 Check. If you changed the Render domain, edit the `$base` variable at the top of the script to your actual Agent Card `supportedInterfaces[0].url`.

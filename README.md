# Free-Deepseek-API

Self-hosted OpenAI-compatible API proxy for [chat.deepseek.com](https://chat.deepseek.com).
Runs a real browser session via **CloakBrowser** (stealth Chromium), so the site sees a genuine
browser fingerprint and the WAF / PoW checks pass on their own — no third-party captcha services,
no manual solving.

> Built for use with [opencode](https://opencode.ai) and any other OpenAI-compatible client.

## How it works

```
client (opencode)                proxy (main.py)                  chat.deepseek.com
      │  POST /v1/chat/completions     │                               │
      │  {tools, messages, ...}        │                               │
      ├───────────────────────────────>│  CloakBrowser session         │
      │                                ├──────────────────────────────>│
      │                                │   localStorage auth injected  │
      │                                │   PoW solved by page worker   │
      │                                │   POST /api/v0/chat/completion│
      │                                │<──────────────────────────────┤
      │<───────────────────────────────┤  SSE stream (XHR.responseText)│
      │  OpenAI chunks                 │  (reasoning_content + content)│
      │                                │                               │
      │                                │  token invalid? ── yes ──┐    │
      │                                │   probe chat_session/create   │
      │                                │   POST /sign_in (real form)  │
      │<───────────────────────────────┤   save new userToken      │   │
      │                                │<───────────────────────────┘   │
```

Replaying the HTTP request from Python is not viable: `/api/v0/chat/completion` requires a
`DeepSeekHashV1` proof-of-work header (solved by the page's own Web Worker in ~150 ms) plus a
short-lived AWS WAF cookie. Driving the real page gets both for free.

- **Tool calling** — DeepSeek web chat has no native function-calling, so the proxy injects tool
  schemas into the prompt as **DSML** (DeepSeek Markup Language, the format the web app itself
  uses) and converts the model's DSML output into standard OpenAI `tool_calls`. See
  [Tool calls](#tool-calls) below.
- **Images** — `image_url` parts (data URL or remote http) are uploaded through the site's own file
  picker, which fills in `ref_file_ids`. Verified working: the model reads the picture, and answers
  "I can't see the image" when the attachment is withheld.
- **Account rotation** — round-robin between accounts from `accounts.json`.
- **Auto-refresh tokens** — DeepSeek rotates `userToken` server-side and offers no refresh endpoint,
  so a stale token is repaired by logging in again with the stored email/password. See
  [Token refresh](#token-refresh).
- **Cooldown** — configurable delay between requests.

## Models

| id | Behaviour |
|---|---|
| `deepseek-chat` | site's **Instant** mode, DeepThink off — no `reasoning_content` |
| `deepseek-reasoner` | same model with **DeepThink** on — `reasoning_content` streamed |

There is deliberately no third model. DeepSeek's own backend config
(`GET /api/v0/client/settings?scope=model` → `settings.model_configs.value`) returns three entries,
but only one is live:

| model_type | name | enabled | switchable |
|---|---|---|---|
| `default` | Instant | **true** | **true** |
| `expert` | Expert | false | false |
| `vision` | Vision | false | false |

The web UI filters on `enabled && switchable`, which is why the site shows no model picker at all —
and why every request carries `model_type: "default"`. Forcing `model_type: "expert"` does reach the
server and is accepted, but produces byte-identical behaviour to `default` (0 vs 0 reasoning
characters with DeepThink off, 77 vs 76 with it on), so there is nothing to expose. Vision adds
nothing either: the same image was read correctly through all three ids.

Reasoning is a **toggle, not a model**, and its state follows the model you asked for:
`deepseek-reasoner` turns DeepThink on, `deepseek-chat` leaves it off. The site exposes no
reasoning-effort control, so the proxy does not accept a `reasoning_effort` parameter — sending one
has no effect. Web search is the composer's other switch, and it is always off; a request cannot
turn it on.

## Tool calls

Tools are advertised to the model in **DSML**, DeepSeek's own markup — the same encoding the web
app uses, and the one in DeepSeek's own `encoding.py`. The proxy parses it back out and emits
ordinary OpenAI `tool_calls`, so clients need no knowledge of it.

Two details are easy to get wrong, and the official prompt spells both out:

- the separator is `｜` (U+FF5C **fullwidth** vertical line), not ASCII `|`;
- every tag name carries a **leading space** — `" invoke"`, `" parameter"`, `" calls"`.

```
<｜DSML｜ calls>
<｜DSML｜ invoke name="get_weather">
<｜DSML｜ parameter name="city" string="true">Paris</｜DSML｜ parameter>
</｜DSML｜ invoke>
</｜DSML｜ calls>
```

| construct | meaning |
|---|---|
| `<｜DSML｜ calls>` | opens a calls block |
| `<｜DSML｜ invoke name="FN">` | one call to function `FN`; ends at `</｜DSML｜ invoke>` |
| `<｜DSML｜ parameter name="K" string="true">` | value is a **raw string**, passed through untouched |
| `<｜DSML｜ parameter name="K" string="false">` | value is **JSON** (number, bool, array, object) |

Closing tags are the norm, not an exception: the official format closes every tag, ending a value at
`</｜DSML｜ parameter>`, a call at `</｜DSML｜ invoke>` and the block at `</｜DSML｜ calls>`. A model
that omits one should still not break the call, so the parser also accepts a value running to the
next `parameter` or `invoke`, and a block closed by the next `<｜DSML｜ calls>` or by end of stream.

`string="true"` versus `string="false"` is the only distinction that matters: a non-string argument
has to arrive as valid JSON, so any surrounding prose is trimmed to the first complete JSON value
before parsing. A block that never parses as a tool call is released back into the visible answer, so
nothing is lost when the model merely mentions DSML in prose.

Arguments are re-serialised with `json.dumps` into the `arguments` **string**, as OpenAI expects.

Verified live against `chat.deepseek.com`: a request carrying a `get_weather` schema comes back as

```json
{"finish_reason": "tool_calls",
 "tool_calls": [{"id": "call_...", "type": "function",
                 "function": {"name": "get_weather", "arguments": "{\"city\": \"Paris\"}"}}]}
```

## Token refresh

DeepSeek has **no refresh endpoint**. A `userToken` is an opaque 64-character string that the site
rotates server-side, and when it dies it stays dead — a request with a stale token comes back as

```json
{"code": 40003, "msg": "Authorization Failed (invalid token)"}
```

The only way back in is a full credential login. So if an account in `accounts.json` carries an
`email` and `password`, the proxy repairs itself: before every fresh chat it checks the session
against `POST /api/v0/chat_session/create` (a dead token answers `code 40003`, a live one `code 0`),
and on failure drives the real sign-in form at `https://chat.deepseek.com/sign_in`, reads the new
`userToken` out of `localStorage`, and writes it back to `accounts.json`.

Toggle it with menu option `2`. With it off, a stale token is reported instead of repaired.

Two things worth knowing:

- **Every login kills the previous token.** Only the most recent one is valid. Logins are therefore
  serialised, and a worker that waited for the lock re-checks the shared token first — otherwise two
  workers recovering one account at startup would invalidate each other's fresh session.
- **A token's presence proves nothing.** The proxy re-injects the account token into `localStorage`
  on every navigation, so a dead token is always non-null and the page still renders a composer. The
  check has to be the server-side probe above, not a DOM test.

### Suspended accounts

If DeepSeek has suspended an account it replaces the whole chat UI with a notice — no composer is
rendered at all, and no amount of logging in will help:

> Due to violation of user policies, your account has been suspended until &lt;date&gt;.

The proxy detects this and says so, instead of the useless "never rendered the composer". A
suspension is tied to the account, so rotation moves to the next one.

## Install

```bash
git clone https://github.com/lothiann/Free-Deepseek-API.git
cd Free-Deepseek-API
pip install -r requirements.txt
```

CloakBrowser ships its own Chromium, so there is no separate `playwright install` step.

## Configure accounts

chat.deepseek.com keeps the session in **`localStorage`**, not a cookie. Open
https://chat.deepseek.com → DevTools (`F12`) → Console:

```js
copy(JSON.stringify({
  token:    JSON.parse(localStorage.userToken).value,
  user_id:  JSON.parse(localStorage['__appKit_userInfo'] || '{}').value?.id || '',
  web_id:   JSON.parse(localStorage['__tea_cache_tokens_20006317'] || '{}').web_id || '',
  device_id: localStorage['deepseek-device-id:chat'] || ''
}, null, 2))
```

Paste into `accounts.json`:

```json
{
  "rotate_every": 10,
  "accounts": [
    {
      "name": "main",
      "token": "64-char opaque userToken",
      "user_id": "optional",
      "web_id": "optional",
      "device_id": "optional",
      "email": "optional - enables auto-refresh",
      "password": "optional - enables auto-refresh"
    }
  ]
}
```

| Field | Description |
|---|---|
| `token` | `userToken` from localStorage (**required**; opaque, *not* a JWT) |
| `user_id` | `__appKit_userInfo.id` — optional, the app fills it in after the token authenticates |
| `web_id` | from `__tea_cache_tokens_20006317` — optional, same |
| `device_id` | `deepseek-device-id:chat` — optional, same |
| `email` | login — optional, but required for [token refresh](#token-refresh) |
| `password` | login — optional, same |
| `rotate_every` | requests per account before switching to the next one |

`email` and `password` are plain credentials in plain text. They only ever get used to re-login your
own account, and the proxy keeps them in this file alone — but that makes `accounts.json` a secret.
It is a local runtime file; do not commit real credentials. The published template ships with an
empty `accounts` list.

## Run

```bash
python main.py
```

Interactive menu: `1` start, `2` toggle auto-refresh tokens, `3` toggle account rotation, `4` change
API port, `5` open `accounts.json`, `6` toggle window hiding, `7` GitHub, `8` exit.

The banner is drawn at 124 columns; the terminal is resized to fit via XTWINOPS
(`CSI 8 ; rows ; cols t`). Terminals that ignore the sequence keep their width and the banner wraps.

Server starts on `http://127.0.0.1:8493/v1`.

### Test

```bash
curl http://127.0.0.1:8493/v1/models

curl -N http://127.0.0.1:8493/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d "{\"model\":\"deepseek-reasoner\",\"stream\":true,\"messages\":[{\"role\":\"user\",\"content\":\"say OK\"}]}"

# account status
curl http://127.0.0.1:8493/accounts
```

## opencode

Add to `~/.config/opencode/opencode.jsonc` (or `opencode.json`):

```jsonc
{
  "provider": {
    "deepseek": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "DeepSeek (web proxy)",
      "options": {
        "baseURL": "http://127.0.0.1:8493/v1",
        "apiKey": "sk-nothing"
      },
      "models": {
        "deepseek-chat":     { "name": "DeepSeek Chat",     "limit": { "context": 131072, "output": 32768 } },
        "deepseek-reasoner": { "name": "DeepSeek Reasoner", "limit": { "context": 131072, "output": 32768 } }
      }
    }
  }
}
```

`apiKey` is ignored — the proxy binds to `127.0.0.1` and does not check it. The value is there
only to satisfy the client.

## Endpoints

| Endpoint | Description |
|---|---|
| `GET /v1/models` | model list |
| `POST /v1/chat/completions` | chat, streaming & non-streaming, with tools and images |
| `GET /accounts` | rotation status |
| `GET /debug/last` | last upstream response, for troubleshooting |

## Notes

- Each request opens a fresh site chat; full conversation history is forwarded as a single prompt.
- Reasoning is streamed via `delta.reasoning_content`, the answer via `delta.content`.
- Tool calls require the client to send standard OpenAI `tools`; results come back as
  `role: "tool"` messages. See [Tool calls](#tool-calls).
- `usage` counts **characters** (1 token := 1 char). The site's real limit is
  `input_character_limit: 2621440`, so the declared `context` is left at a conservative 131072.
- The two switches are the real `div.ds-toggle-button` controls in the composer — **DeepThink** and
  **Search** — clicked only when the current `aria-pressed` state differs from the requested one, so
  the site's own persistence is respected. DeepThink is requested from the model id, and Search is
  always requested off, so the proxy never enables web search.
- Stale remote feature caches are dropped on every page load (`__ds_remote_feature_store*`), which
  is what lets a newly enabled `expert` entry take effect without a code change.
- Logs are written to `logs/`, the last raw response to `last_response.json`.
- Captcha handling is present but **commented out**. It was tripping on pages that were merely
  mid-reload and caused an unbounded retry loop. Uncomment the marked blocks in `main.py` if a
  genuine challenge ever shows up.

### Detection

The proxy is designed to be hard to detect, and there is no evidence of it being fingerprinted: every
account gets a real Chromium profile with its own persistent fingerprint, patched at the browser
level, instead of a plain headless run. The proxy drives the actual site UI, so the request that
reaches DeepSeek carries the same shape as a normal human session. There is no replayed HTTP, no
hand-rolled client signature, nothing that stands out as automation.

**That is not the same as "your account is safe", and it is worth being precise about it.**
Authentication failures, an invalid-token response and a policy suspension are three different
things. An account can log in perfectly, hold a valid token, pass every probe — and still be
suspended. This was observed in practice: a working account was suspended by DeepSeek for "violation
of user policies", with a notice that replaced the entire chat UI until a date weeks away. No token
refresh, rotation or detection countermeasure can prevent that; only appealing can, and rotation is
the only in-proxy mitigation.

So: keep `rotate_every` sane rather than pushing one account hard, keep more than one account
configured, and treat [suspension](#suspended-accounts) as a normal operational event rather than a
bug. This project is an observation about the transport, not a promise about your account.

## Disclaimer

For personal / educational use. Automating chat.deepseek.com may violate its Terms of Service — use
your own accounts at your own risk.

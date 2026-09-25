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
```

Replaying the HTTP request from Python is not viable: `/api/v0/chat/completion` requires a
`DeepSeekHashV1` proof-of-work header (solved by the page's own Web Worker in ~150 ms) plus a
short-lived AWS WAF cookie. Driving the real page gets both for free.

- **Tool calling** — DeepSeek web chat has no native function-calling, so the proxy injects tool
  schemas into the prompt and streams the model's `<tc>NAME<ak>key</ak><av>value</av></tc>` output
  back as standard OpenAI `delta.tool_calls`.
- **Images** — `image_url` parts (data URL or remote http) are uploaded through the site's own file
  picker, which fills in `ref_file_ids`. Verified working: the model reads the picture, and answers
  "I can't see the image" when the attachment is withheld.
- **Account rotation** — round-robin between accounts from `accounts.json`.
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

Reasoning is a **toggle, not a model**. It maps from `reasoning_effort`:

| `reasoning_effort` | DeepThink |
|---|---|
| `none` / `off` / `disabled` / `minimal` / `low` | off |
| `medium` / `high` / `max` | on |
| omitted | on for `deepseek-reasoner`, off for `deepseek-chat` |

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
      "device_id": "optional"
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
| `rotate_every` | requests per account before switching to the next one |

## Run

```bash
python main.py
```

Interactive menu: `1` start, `3` toggle account rotation, `4` change API port, `5` open
`accounts.json`, `6` toggle window hiding, `8` exit.

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
  `role: "tool"` messages.
- `usage` counts **characters** (1 token := 1 char). The site's real limit is
  `input_character_limit: 2621440`, so the declared `context` is left at a conservative 131072.
- The two switches are the real `div.ds-toggle-button` controls in the composer — **DeepThink** and
  **Search** — clicked only when the current `aria-pressed` state differs from the requested one, so
  the site's own persistence is respected.
- Stale remote feature caches are dropped on every page load (`__ds_remote_feature_store*`), which
  is what lets a newly enabled `expert` entry take effect without a code change.
- Logs are written to `logs/`, the last raw response to `last_response.json`.
- Captcha handling is present but **commented out** — see the `CAPTCHA DISABLED` markers in
  `main.py`. It was tripping on pages that were merely mid-reload and caused an unbounded retry
  loop. Uncomment the marked blocks if a genuine challenge ever shows up.

### Detection

This proxy is **not detected** by DeepSeek, so accounts are very unlikely to get banned.

The reason is [CloakBrowser](https://github.com/ACoderDream/CloakBrowser) — every account gets a
real Chromium profile with its own persistent fingerprint, patched at the browser level, instead of
a plain headless run. The proxy drives the actual site UI, so the request that reaches DeepSeek
carries the same shape as a normal human session. There is no replayed HTTP, no hand-rolled
client signature, nothing that stands out as automation.

Practically: you can run many requests per account and rotate through several accounts without
worrying about sudden bans. This is an observation, not a promise — DeepSeek can always change
their detection, so keep `rotate_every` set to something sane rather than pushing one account hard.

## Disclaimer

For personal / educational use. Automating chat.deepseek.com may violate its Terms of Service — use
your own accounts at your own risk.

import asyncio
import base64
import json
import re
import secrets
import subprocess
import tempfile
import time
import urllib.request
import uuid
import sys
import os
import webbrowser
from urllib.parse import quote
from datetime import datetime

from cloakbrowser import launch_context_async
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
import uvicorn

# ===== CONFIG =====
TOKEN = ""
HOST = "127.0.0.1"
PORT = 8493
CHAT_URL = "https://chat.deepseek.com/"
SIGN_IN_URL = "https://chat.deepseek.com/sign_in"
FALLBACK_MODEL = "deepseek-chat"

# NOTE: there is deliberately no model_type table here. DeepSeek merged
# Instant/Expert/Vision into one multimodal model, and its own model picker
# sends model_type="default" for every option, so the field carries no
# information. Reasoning is a separate switch ("DeepThink"), not a model.
# See MODELS below for the OpenAI-facing ids and map_thinking() for the
# thinking/search switches. MODELS below lists the OpenAI-facing ids.
DEFAULT_MODEL_TYPE = "default"

# ===== STARTUP MENU STATE (toggled from the console before launch) =====
ACCOUNT_ROTATE = True              # [3] rotate between accounts after rotate_every requests
HEADLESS = True                    # [6] hide the browser window (True = hidden, default on)
AUTO_REFRESH = True                # [2] re-login with email/password when a token goes stale
REQUEST_COOLDOWN = 0               # seconds between requests
TOOL_CALL_DELAY = 0.5              # seconds between parallel tool-call chunks, avoids Busy errors in the client
MAX_REQUEST_RETRIES = 4            # max retries per request before giving up
ACCOUNTS_FILE = "accounts.json"

# ===== STREAM TIMING =====
STREAM_POLL_MS = 80                # how often we pull the XHR buffer out of the page
STREAM_IDLE_TIMEOUT = 300          # seconds without any new bytes before we call it dead
CHAT_URL_READY_TIMEOUT = 45        # seconds to wait for the composer textarea
IMAGE_UPLOAD_WAIT = 4.0            # seconds to let the site finish uploading attachments

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOG_DIR, f"server_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")


ANSI = {
    "INFO": "\x1b[36m",   # cyan
    "WARN": "\x1b[33m",   # yellow
    "ERROR": "\x1b[31m",  # red
    "OK": "\x1b[32m",     # green
    "BOLD": "\x1b[1m",
    "DIM": "\x1b[2m",
    "RESET": "\x1b[0m",
}


_ansi_enabled = False


def log(msg, level="INFO"):
    global _ansi_enabled
    if not _ansi_enabled:
        try:
            _enable_ansi()
        except Exception:
            pass
        _ansi_enabled = True
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    line = f"[{ts}] [{level}] {msg}"
    try:
        color = ANSI.get(level, "") + ANSI.get("BOLD", "")
        ts_col = f"{ANSI['DIM']}{ts}{ANSI['RESET']}"
        lvl_col = f"{color}[{level}]{ANSI['RESET']}"
        if level == "OK":
            print(f"{ts_col} {lvl_col} {ANSI['OK']}{msg}{ANSI['RESET']}", flush=True)
        else:
            print(f"{ts_col} {lvl_col} {msg}", flush=True)
    except UnicodeEncodeError:
        print(line.encode("ascii", errors="replace").decode(), flush=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")


_BG_TASKS = set()


def spawn_bg(coro):
    """create_task that is guaranteed to survive. The loop only keeps a weak
    reference to a task, so a bare create_task() result can be collected before
    it ever gets to run."""
    t = asyncio.create_task(coro)
    _BG_TASKS.add(t)
    t.add_done_callback(_BG_TASKS.discard)
    return t


async def _stream_relay(gen_factory):
    """Feed the client from a queue while a background task drives the browser.

    Why not hand the real generator to StreamingResponse: on a client hangup
    Starlette cancels the task group, and a generator suspended inside
    page.evaluate() never sees GeneratorExit - its except/finally simply do
    not run, so the site's stop button is never clicked. This relay touches no
    browser calls, so its finally is guaranteed to run and flag the disconnect;
    the poll loop inside stream_tokens() then does the click itself.
    """
    queue = asyncio.Queue(maxsize=64)
    client_gone = asyncio.Event()

    async def producer():
        try:
            agen = gen_factory(client_gone)
            async for chunk in agen:
                if client_gone.is_set():
                    # Keep draining after the hangup: the generator has to run
                    # to its own end so its finally releases the worker. The
                    # bytes are dropped - nobody is listening.
                    continue
                await queue.put(chunk)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log(f"[stream] producer error: {e}", level="ERROR")
        finally:
            queue.put_nowait(None)

    spawn_bg(producer())
    try:
        while True:
            chunk = await queue.get()
            if chunk is None:
                break
            yield chunk
    finally:
        client_gone.set()


# ===== JS SNIPPETS =====

ACCOUNT_COOKIE = "ds_proxy_acct"


def _account_cookie_value(acc):
    return quote(json.dumps({
        "token": acc.get("token", ""),
        "userId": acc.get("user_id", ""),
        "webId": acc.get("web_id", ""),
        "deviceId": acc.get("device_id", ""),
        "locale": "en_US",
    }))


# The account is handed to the page through a short-lived cookie rather than a
# per-account init script, because init scripts ACCUMULATE on a context: adding
# one per rotation would leave stale tokens running after every switch.
ACCOUNT_SHIM_JS = """
    (() => {
      let CFG = { token: '', userId: '', webId: '', deviceId: '', locale: 'en_US' };
      const m = document.cookie.match(/(?:^|;\\s*)ds_proxy_acct=([^;]*)/);
      if (m) {
        try { CFG = Object.assign(CFG, JSON.parse(decodeURIComponent(m[1]))); } catch (e) {}
        document.cookie = 'ds_proxy_acct=; Max-Age=0; path=/';
      }
      const COMPLETION = '/api/v0/chat/completion';

      // ---- 1. auth state -------------------------------------------------
      // chat.deepseek.com keeps the session in localStorage, not a cookie:
      //   userToken -> {"value": "<opaque token>", "__version": "0"}
      // It has to exist before the app bundle boots, hence the init script.
      try {
        if (CFG.token) {
          localStorage.setItem('userToken',
            JSON.stringify({ value: CFG.token, __version: '0' }));
        }
        localStorage.setItem('__appKit_@deepseek/chat_localePreference',
          JSON.stringify({ value: CFG.locale, __version: '0' }));
        if (CFG.userId) {
          localStorage.setItem('__appKit_userInfo',
            JSON.stringify({ value: { id: CFG.userId }, __version: '0' }));
          localStorage.setItem('__tea_cache_tokens_20006317', JSON.stringify({
            web_id: CFG.webId, user_unique_id: CFG.userId,
            timestamp: Date.now(), _type_: 'default'
          }));
        }
        if (CFG.deviceId) {
          localStorage.setItem('deepseek-device-id:chat', CFG.deviceId);
        }
        // the remote feature store is refetched by the app; a stale copy would
        // pin us to old feature flags, so drop it and let it refresh
        for (const k of ['__ds_remote_feature_store', '__ds_remote_feature_store_model',
                         '__ds_remote_feature_store_provider', '__ds_remote_feature_did']) {
          try { localStorage.removeItem(k); } catch (e) {}
        }
      } catch (e) {}

      // ---- 2. XHR tap ----------------------------------------------------
      // The site does NOT stream over fetch(): chat/completion is an
      // XMLHttpRequest whose growing responseText the app reads from
      // onDownloadProgress. We mirror that buffer into window.__dsx so Python
      // can poll it, without ever stealing bytes from the page itself.
      //
      // window.__dsForce is the payload backstop (thinking_enabled /
      // search_enabled) applied in send() below. The real UI control for both
      // is a pair of `div.ds-toggle-button` switches labelled "DeepThink" and
      // "Search", which SET_TOGGLES_JS clicks; this override only matters if
      // that markup ever changes. model_type is NOT overridden: DeepSeek
      // unified Instant/Expert/Vision into one model, and the site sends
      // "default" for every option in its picker.
      window.__dsForce = window.__dsForce || {};
      window.__dsx = {
        active: false, raw: '', body: null, headers: null,
        status: null, done: false, error: null, responseType: null, started: 0
      };

      const openOrig = XMLHttpRequest.prototype.open;
      const sendOrig = XMLHttpRequest.prototype.send;
      const hdrOrig = XMLHttpRequest.prototype.setRequestHeader;

      XMLHttpRequest.prototype.open = function (method, url) {
        this.__dsUrl = String(url);
        this.__dsSeen = 0;
        return openOrig.apply(this, arguments);
      };

      XMLHttpRequest.prototype.setRequestHeader = function (k, v) {
        if (this.__dsUrl && this.__dsUrl.indexOf(COMPLETION) !== -1) {
          if (!window.__dsx.headers) window.__dsx.headers = {};
          try { window.__dsx.headers[k] = v; } catch (e) {}
        }
        return hdrOrig.apply(this, arguments);
      };

      XMLHttpRequest.prototype.send = function (body) {
        const url = this.__dsUrl || '';
        if (url.indexOf(COMPLETION) !== -1) {
          const S = window.__dsx;
          const self = this;
          S.active = true; S.raw = ''; S.body = body ? String(body) : null;
          S.status = null; S.done = false; S.error = null; S.started = Date.now();

          // Backstop only. set_prefs() has already clicked the real DeepThink
          // and Search switches; if one of them was missing from the DOM we
          // still want the request to carry the right flags. model_type is
          // deliberately left alone: the picker no longer changes it.
          // The PoW header was solved by the page for this URL and is not a
          // hash of the body, so editing the payload afterwards is safe.
          const O = window.__dsForce;
          if (O && body) {
            try {
              const j = JSON.parse(body);
              S.request = j;
              S.rewritten = false;
              if (typeof O.thinking === 'boolean'
                  && j.thinking_enabled !== O.thinking) {
                j.thinking_enabled = O.thinking; S.rewritten = true;
              }
              if (typeof O.search === 'boolean' && j.search_enabled !== O.search) {
                j.search_enabled = O.search; S.rewritten = true;
              }
              if (S.rewritten) body = JSON.stringify(j);
            } catch (e) { /* malformed body: send the original untouched */ }
          }

          const tap = () => {
            try { S.responseType = self.responseType; } catch (e) {}
            let text = null;
            try {
              const rt = self.responseType;
              if (rt === '' || rt === 'text') text = self.responseText;
              else if (rt === 'json') text = JSON.stringify(self.response);
            } catch (e) {}
            if (text != null && text.length > self.__dsSeen) {
              S.raw = text;
              self.__dsSeen = text.length;
            }
            if (self.readyState === 4) { S.status = self.status; S.done = true; }
          };
          this.addEventListener('progress', tap);
          this.addEventListener('load', tap);
          this.addEventListener('loadend', tap);
          this.addEventListener('error', () => { S.error = 'xhr network error'; S.done = true; });
          this.addEventListener('abort', () => { S.error = 'xhr aborted'; S.done = true; });
        }
        return sendOrig.apply(this, arguments);
      };
    })();
"""

POPUP_KILLER_JS = """
    () => {
        let acted = false;
        // close-button heuristics shared by every dialog-ish overlay
        const overlays = document.querySelectorAll(
            '[role="dialog"], [data-state="open"][role], [class*="modal"], [class*="Modal"]'
        );
        for (const o of overlays) {
            if (o.querySelector('textarea, [contenteditable="true"]')) continue;  // never nuke the composer
            for (const b of o.querySelectorAll('button')) {
                const label = (b.getAttribute('aria-label') || '') + ' ' + (b.textContent || '');
                const hasX = !!b.querySelector(
                    'svg.lucide-x, svg[data-icon="x"], svg[data-icon="close"], [class*="close"]');
                const looksClose = /close|dismiss|закрыть|×|✕/i.test(label);
                const tiny = (b.textContent || '').trim().length <= 1;
                if (hasX || looksClose || tiny) { b.click(); acted = true; break; }
            }
        }
        // the "Instant, Expert and Vision are now unified" promo banner
        const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
        let node;
        while ((node = walker.nextNode())) {
            const t = (node.textContent || '');
            if (!/now unified|unified and upgraded|Now Available/i.test(t)) continue;
            let el = node.parentElement;
            for (let i = 0; el && el !== document.body && i < 8; i++, el = el.parentElement) {
                for (const b of el.querySelectorAll('button')) {
                    const label = (b.getAttribute('aria-label') || '') + ' ' + (b.textContent || '');
                    if (/close|dismiss|×|✕/i.test(label) || (b.textContent || '').trim().length <= 1) {
                        b.click(); acted = true; break;
                    }
                }
            }
        }
        document.body.style.pointerEvents = '';
        document.documentElement.style.pointerEvents = '';
        document.body.style.overflow = '';
        return acted;
    }
"""

# The composer. DeepSeek hashes its class names per build (ds-* is stable,
# the _27c9245 part is not), so we match on the stable attributes only.
TEXTAREA_SEL = 'textarea[name="search"], textarea[placeholder], textarea.ds-scroll-area'

INPUT_READY_JS = """
    () => {
        const tas = [...document.querySelectorAll('textarea')].filter(t => t.offsetParent);
        return tas.length > 0;
    }
"""

# Read the mirrored XHR buffer. Cheap enough to poll every ~80ms.
TAP_POLL_JS = """
    () => {
        const S = window.__dsx;
        if (!S) return { active: false, raw: '' };
        return {
            active: !!S.active, raw: S.raw || '', body: S.body,
            headers: S.headers, status: S.status, done: !!S.done,
            error: S.error, responseType: S.responseType
        };
    }
"""

# Last-resort reader if the XHR tap never fires: scrape the rendered answer.
DOM_ANSWER_JS = """
    () => {
        const cands = [...document.querySelectorAll(
            '[class*="markdown"], [class*="fbb737a4"], [data-message-role="assistant"]'
        )].filter(e => e.offsetParent);
        if (!cands.length) return '';
        const last = cands[cands.length - 1];
        return (last.innerText || '').trim();
    }
"""

# DeepSeek's composer has two switches, both `div.ds-toggle-button` with an
# aria-pressed attribute and a visible label: "DeepThink" and "Search". The site
# keeps their state between requests, so a click is only issued when the switch
# is not already in the wanted position.
SET_TOGGLES_JS = """
    ({thinking, search}) => {
        const vis = (e) => !!(e && e.offsetParent);
        const pick = (re) => [...document.querySelectorAll('div.ds-toggle-button')]
            .filter(vis).find((e) => re.test((e.innerText || '').trim()));
        const set = (re, want) => {
            const el = pick(re);
            if (!el) return 'missing';
            const now = el.getAttribute('aria-pressed') === 'true';
            if (now === want) return 'ok';
            el.click();
            return 'clicked';
        };
        return {
            thinking: set(/deepthink/i, !!thinking),
            search: set(/^search$/i, !!search)
        };
    }
"""

# Stage the payload backstop read by ACCOUNT_SHIM_JS in send().
SET_FORCE_JS = """
    (fields) => {
        const F = window.__dsForce = window.__dsForce || {};
        if (!fields) return F;
        if ('thinking' in fields) F.thinking = !!fields.thinking;
        if ('search' in fields) F.search = !!fields.search;
        return F;
    }
"""

# What the site itself put in the request, as observed by /debug/last.
FORCE_STATE_JS = """
    () => ({
        force: window.__dsForce || {},
        sent: (window.__dsx && window.__dsx.request) || null,
        rewritten: !!(window.__dsx && window.__dsx.rewritten)
    })
"""


# DeepSeek fronts the app with AWS WAF (formerly Cloudflare) and can drop an
# hCaptcha / Turnstile frame on top. Nothing here can be solved from Python, so
# we only detect them and let the retry path reload the session.
# CAPTCHA_JS = """
#     () => {
#         if (document.querySelector('iframe[src*="hcaptcha"], iframe[src*="turnstile"], iframe[title*="challenge"]')) {
#             return true;
#         }
#         if (document.querySelector('#aliyunCaptcha-window-popup')) return true;
#         const t = (document.body.innerText || '');
#         return /verify you are human|checking your browser|just a moment/i.test(t);
#     }
# """

# Token rejected / signed out: the app lands on the marketing page.
LOGIN_WALL_JS = """
    () => {
        if (document.querySelector('textarea[name="search"]')) return false;
        const t = (document.body.innerText || '');
        return /log in to deepseek|sign in to continue|\u767b\u5f55 DeepSeek/i.test(t);
    }
"""

# DeepSeek has no refresh_token endpoint: a token is simply rotated server-side
# and the old one dies. The only reliable "is it still good?" probe is asking an
# authenticated endpoint. chat_session/create answers HTTP 200 with
# {"code":40003,"msg":"Authorization Failed (invalid token)"} for a dead token,
# and a real session id for a live one - so code === 0 means valid.
TOKEN_PROBE_JS = """
    async (token) => {
        try {
            const r = await fetch('/api/v0/chat_session/create', {
                method: 'POST',
                headers: {'content-type': 'application/json',
                          'authorization': 'Bearer ' + token},
                body: '{}',
            });
            const j = await r.json();
            return {ok: j.code === 0, code: j.code, msg: j.msg || ''};
        } catch (e) {
            return {ok: false, code: -1, msg: String(e)};
        }
    }
"""

# The session lives in localStorage, not a cookie: userToken -> {"value": ...}
SESSION_TOKEN_JS = """
    () => {
        try {
            const raw = localStorage.getItem('userToken');
            if (!raw) return '';
            const v = JSON.parse(raw).value;
            return typeof v === 'string' ? v : '';
        } catch (e) { return ''; }
    }
"""

# True once the composer is on screen AND we are not looking at the sign-in
# wall - i.e. the browser really is signed in.
AUTHED_CHAT_JS = """
    () => !!(document.querySelector('textarea[name="search"]')
             || document.querySelector('textarea'))
"""

# DeepSeek replaces the whole chat UI with a policy notice when an account is
# suspended, so there is no composer at all. Detecting it turns a baffling
# "never rendered the composer" into the actual reason.
SUSPENDED_JS = """
    () => {
        const t = (document.body.innerText || '').replace(/\\s+/g, ' ');
        const m = t.match(/account has been suspended[^.]*\\.?/i);
        return m ? m[0].trim() : '';
    }
"""

EMAIL_INPUT_SEL = 'input[placeholder="Phone number / email address"]'
PASSWORD_INPUT_SEL = 'input[type="password"]'
LOGIN_BUTTON_SEL = 'div.ds-button--primary:has-text("Log in")'
LOGIN_FORM_READY_JS = """
    () => {
        const e = document.querySelector('input[placeholder="Phone number / email address"]');
        const p = document.querySelector('input[type=password]');
        return !!(e && e.offsetParent && p && p.offsetParent);
    }
"""

# Every login rotates the account token and kills the previous one, so two
# workers recovering the same account at the same time would invalidate each
# other's fresh session. Serialise logins and re-check under the lock: whoever
# waits picks up the token the winner just saved.
_AUTH_LOCK = None


def _auth_lock():
    global _AUTH_LOCK
    if _AUTH_LOCK is None:
        _AUTH_LOCK = asyncio.Lock()
    return _AUTH_LOCK

STOP_GENERATION_JS = """
    () => {
        // The Stop control is NOT a <button>: it is a div[role="button"] whose
        // only child is a filled rounded-square svg glyph. It carries no
        // aria-label and no text, so scanning <button> labels can never find
        // it - match the glyph's path data instead. The label scan stays as a
        // fallback in case the icon ever changes.
        const SQUARE_GLYPH = /M2\\s*4\\.88C2\\s*3\\.68009/;
        const visible = (el) => !!(el.offsetWidth || el.offsetHeight);
        for (const el of document.querySelectorAll('div[role="button"]')) {
            const path = el.querySelector('svg path');
            if (path && SQUARE_GLYPH.test(path.getAttribute('d') || '') && visible(el)) {
                el.click();
                return true;
            }
        }
        for (const b of document.querySelectorAll('button')) {
            const label = ((b.getAttribute('aria-label') || '') + ' ' + (b.textContent || '')).trim();
            if (/stop|\\u505c\\u6b62|halt/i.test(label) && visible(b)) {
                b.click();
                return true;
            }
        }
        return false;
    }
"""

# ===== TOOL CALLING (text-based protocol, Hermes JSON scheme) =====

# Injected right after "# History ..." header as a final system line when tools
# are used. Edit the text freely - the proxy injects it verbatim.
FINAL_SYSTEM_MESSAGE = """All other tool call instructions, formats and tags are PERMANENTLY disabled and WRONG - the ONLY valid tool call format is DSML: a <｜DSML｜ calls> block holding <｜DSML｜ invoke name="TOOL"> and <｜DSML｜ parameter name="KEY" string="true|false">VALUE. See <tool_call_format>. NEVER write anything after the <｜DSML｜ calls> block; it must ALWAYS be at the end of your response. NEVER output JSON keys role, content, thinking, name, tool_call_id, tool, user, ... as your reply. Your reply is plain text, optionally with a <｜DSML｜ calls> block."""

SYSTEM_CONTINUE = 'This is a forwarded conversation.'

TOOL_PROMPT_TEMPLATE = """ # You have access to these tools:

{tool_details}

{instructions}"""

TOOL_INSTRUCTIONS = """# Tool Call Instructions
<tool_call_format>
The ONLY tool-call format is DSML. Ignore every other format you may know - markup tags, bare JSON objects with a "name" key, and any other invented scheme are all WRONG here. A tool call is a <｜DSML｜ calls> block.

One call, one string parameter:
<｜DSML｜ calls>
<｜DSML｜ invoke name="$TOOL_NAME">
<｜DSML｜ parameter name="$PARAMETER_NAME" string="true">$PARAMETER_VALUE</｜DSML｜ parameter>
</｜DSML｜ invoke>
</｜DSML｜ calls>

One call, several parameters:
<｜DSML｜ calls>
<｜DSML｜ invoke name="$TOOL_NAME">
<｜DSML｜ parameter name="param_name" string="true">value</｜DSML｜ parameter>
<｜DSML｜ parameter name="count" string="false">5</｜DSML｜ parameter>
</｜DSML｜ invoke>
</｜DSML｜ calls>

Several calls in one turn = one <｜DSML｜ invoke> per tool, all inside ONE <｜DSML｜ calls> block:
<｜DSML｜ calls>
<｜DSML｜ invoke name="TOOL_NAME_1">
<｜DSML｜ parameter name="p" string="true">v</｜DSML｜ parameter>
</｜DSML｜ invoke>
<｜DSML｜ invoke name="TOOL_NAME_2">
<｜DSML｜ parameter name="p" string="true">v</｜DSML｜ parameter>
</｜DSML｜ invoke>
</｜DSML｜ calls>

A tool with no parameters:
<｜DSML｜ calls>
<｜DSML｜ invoke name="clear">
</｜DSML｜ invoke>
</｜DSML｜ calls>

<rules>
# Rules:
- You may write ONLY: (1) normal prose/answer text, and (2) <｜DSML｜ calls> blocks. Nothing else in any structured format.
- A string parameter is written RAW, exactly as-is, with string="true".
- Every other type (number, boolean, array, object, null) is written as JSON with string="false": <｜DSML｜ parameter name="n" string="false">42, <｜DSML｜ parameter name="b" string="false">true, <｜DSML｜ parameter name="a" string="false">[1, 2], <｜DSML｜ parameter name="o" string="false">{"a": 1}.
- A tool with no parameters is just <｜DSML｜ invoke name="clear">.
- Parameter names MUST match that tool's schema exactly, and the tool name MUST be one of the <allowed_tools>.
- If the previous tool didn't show result, it means you violated some rules of the tools from <bad_examples>.
- If no suitable tool exists, pick an alternative from the EXISTING list; do not even mention other tools.
- Paths: use forward slashes / (recommended). If you must use backslashes, double them (\\) - raw backslashes no longer break anything, but keep writing them doubled.
- Raw inner quotes in string values are fine, they are plain text between the opening and the closing parameter tag: <｜DSML｜ parameter name="command" string="true">rg -n "pattern" src/</｜DSML｜ parameter>.
- Don't break anything, even if you've already broken it in the chat history.
- Don't write "The user reported ..." and similar phrases.
- NEVER write anything after the <｜DSML｜ calls> block.
- NEVER write \\n outside the tool call - this does NOT work.
- Tool results arrive as <tool_result>...</tool_result> history lines.
- It is recommended to use a colon to indicate that you are calling the tool:

Now I will read:
<｜DSML｜ calls>
<｜DSML｜ invoke name="read">
<｜DSML｜ parameter name="filePath" string="true">/project/file.txt</｜DSML｜ parameter>
</｜DSML｜ invoke>
</｜DSML｜ calls>

</rules>

<bad_examples>
<｜DSML｜ calls><｜DSML｜ invoke name="bash"><｜DSML｜ parameter name="command" string="true">dir</｜DSML｜ calls>   <- missing </｜DSML｜ parameter> and </｜DSML｜ invoke>
<｜DSML｜ calls><｜DSML｜ invoke name="bash"><｜DSML｜ parameter name="command" string="true">dir</｜DSML｜ parameter></｜DSML｜ calls>   <- missing </｜DSML｜ invoke>
<｜DSML｜ calls><｜DSML｜ invoke name="bash"><｜DSML｜ parameter name="command" string="true">dir</｜DSML｜ parameter></｜DSML｜ invoke>   <- missing </｜DSML｜ calls>
<｜DSML｜ calls><｜DSML｜ invoke><｜DSML｜ parameter name="command" string="true">dir</｜DSML｜ parameter></｜DSML｜ invoke></｜DSML｜ calls>   <- missing name= on invoke
<｜DSML｜ calls><｜DSML｜ invoke name="bash"><｜DSML｜ parameter name="command" string="false">"dir"</｜DSML｜ parameter></｜DSML｜ invoke></｜DSML｜ calls>   <- a string value must not be JSON-quoted
I'll read it now...                                                                  <- narrated instead of calling
<｜DSML｜ calls><｜DSML｜ invoke name="bash"><｜DSML｜ parameter name="command" string="true">rg -n "p</｜DSML｜ parameter></｜DSML｜ invoke></｜DSML｜ calls>   <- unterminated value, inner quote never closed
</bad_examples>

<good_examples>
single call:
<｜DSML｜ calls>
<｜DSML｜ invoke name="read">
<｜DSML｜ parameter name="filePath" string="true">/project/file.txt</｜DSML｜ parameter>
</｜DSML｜ invoke>
</｜DSML｜ calls>

parallel calls:
<｜DSML｜ calls>
<｜DSML｜ invoke name="glob">
<｜DSML｜ parameter name="pattern" string="true">**/*.ts</｜DSML｜ parameter>
</｜DSML｜ invoke>
<｜DSML｜ invoke name="grep">
<｜DSML｜ parameter name="pattern" string="true">TODO</｜DSML｜ parameter>
</｜DSML｜ invoke>
</｜DSML｜ calls>

non-string values:
<｜DSML｜ calls>
<｜DSML｜ invoke name="todowrite">
<｜DSML｜ parameter name="todos" string="false">[{"content": "make init", "status": "in_progress", "priority": "high"}]</｜DSML｜ parameter>
</｜DSML｜ invoke>
</｜DSML｜ calls>
</good_examples>

<critic>
Before you send: is it a <｜DSML｜ calls> block? one <｜DSML｜ invoke> per tool? does every parameter carry name= and string=? are strings raw with string="true" and everything else JSON with string="false"? do the names match the schema and <allowed_tools>? is there nothing after the block?
</critic>

<priorities>
1. The <rules>
2. The <bad_examples>, <good_examples> and <critic>
3. Purpose/User Message
</priorities>
</tool_call_format>"""


DSML_TOKEN = "\uff5cDSML\uff5c"
DSML_CALLS_OPEN = "<" + DSML_TOKEN + " calls>"
DSML_CALLS_CLOSE = "</" + DSML_TOKEN + " calls>"
_INVOKE_RE = re.compile(r"<" + re.escape(DSML_TOKEN) + r' invoke(?:\s+name="([^"]*)")?\s*>')
_PARAM_RE = re.compile(r"<" + re.escape(DSML_TOKEN) + r' parameter\s+name="([^"]*)"\s+string="([^"]*)"\s*>')


def _strip_cdata(v):
    return re.sub(r"<!\[CDATA\[([\s\S]*?)\]\]>", r"\1", v)


def render_tools_block(tools):
    """DeepSeek expects one JSON function schema per line under
    '### Available Tool Schemas' - not an OpenAI {"tools": [...]} wrapper."""
    return "\n".join(
        json.dumps(t.get("function") or t, ensure_ascii=False) for t in tools
    )


def _trim_to_json_prefix(raw):
    """A string="false" value must be JSON, but a model that ignores the
    'nothing after the block' rule appends prose. Keep the longest prefix that
    still parses, so the trailing sentence does not corrupt the argument."""
    raw = raw.strip()
    if not raw:
        return raw
    try:
        json.loads(raw)
        return raw
    except Exception:
        pass
    for end in range(len(raw), 0, -1):
        try:
            json.loads(raw[:end])
            return raw[:end]
        except Exception:
            continue
    return raw


def _dsml_arg_to_json(key, raw, is_str):
    """One 'key: value' pair. string="true" -> the value is a raw string and gets
    JSON-encoded; string="false" -> the value is already JSON and is used as-is.
    Mirrors decode_dsml_to_arguments() in DeepSeek's own encoding.py."""
    if is_str:
        raw = json.dumps(raw, ensure_ascii=False)
    return f"{json.dumps(key, ensure_ascii=False)}: {raw}"


_CLOSE_TAG_RE = re.compile(r"</" + re.escape(DSML_TOKEN) + r"\s*(?:calls|invoke|parameter)\s*>")
_INVOKE_CLOSE_RE = re.compile(r"</" + re.escape(DSML_TOKEN) + r"\s*invoke\s*>")


def _parse_dsml_block(block):
    """Parse the inside of one <DSML calls> block: one <DSML invoke> per call,
    each followed by <DSML parameter name= string=>value lines. Every tag is
    closed, so a value ends at its </DSML parameter> and a call at
    </DSML invoke>; a missing closer is tolerated and the value then runs until
    the next parameter/invoke or block end."""
    calls = []
    invocations = list(_INVOKE_RE.finditer(block))
    for i, inv in enumerate(invocations):
        name = (inv.group(1) or "").strip()
        if not name:
            continue
        stop = invocations[i + 1].start() if i + 1 < len(invocations) else len(block)
        closing = _INVOKE_CLOSE_RE.search(block, inv.end(), stop)
        if closing is not None:
            stop = closing.start()
        segment = block[inv.end():stop]
        pairs = []
        params = list(_PARAM_RE.finditer(segment))
        for j, prm in enumerate(params):
            pend = params[j + 1].start() if j + 1 < len(params) else len(segment)
            closing = _CLOSE_TAG_RE.search(segment, prm.end())
            if closing is not None:
                pend = min(pend, closing.start())
            raw = _strip_cdata(segment[prm.end():pend]).strip("\r\n")
            is_str = prm.group(2).lower() == "true"
            if not is_str:
                raw = _trim_to_json_prefix(raw)
            pairs.append(_dsml_arg_to_json(prm.group(1), raw, is_str))
        arguments = "{" + ", ".join(pairs) + "}" if pairs else "{}"
        calls.append({"name": name, "arguments": arguments})
    return calls


def parse_tool_call_blocks(text):
    """Parse every <DSML calls> block in the text into OpenAI tool_calls.
    A block ends at its </DSML calls> tag when the model wrote one, otherwise at
    the next block or the end of the text. Parallel calls are several <DSML
    invoke> entries inside one block."""
    calls = []
    for m in re.finditer(re.escape(DSML_CALLS_OPEN), text):
        end = len(text)
        nxt = text.find(DSML_CALLS_OPEN, m.end())
        if nxt != -1:
            end = nxt
        close = text.find(DSML_CALLS_CLOSE, m.end())
        if close != -1:
            end = min(end, close)
        calls.extend(_parse_dsml_block(text[m.end():end]))
    return calls


def tool_call_dsml(name, arguments):
    """History re-injection: an OpenAI tool_call rendered back as DSML."""
    lines = [f'<{DSML_TOKEN} invoke name="{name}">']
    if isinstance(arguments, str):
        try:
            arguments = json.loads(arguments)
        except Exception:
            arguments = {"arguments": arguments}
    for k, v in (arguments or {}).items():
        is_str = isinstance(v, str)
        value = v if is_str else json.dumps(v, ensure_ascii=False)
        lines.append(f'<{DSML_TOKEN} parameter name="{k}" string="{"true" if is_str else "false"}">{value}</{DSML_TOKEN} parameter>')
    lines.append(f"</{DSML_TOKEN} invoke>")
    return DSML_CALLS_OPEN + "\n" + "\n".join(lines) + "\n" + DSML_CALLS_CLOSE


class ToolStreamBuffer:
    """Streams visible text and captures <DSML calls> blocks, converting them to
    OpenAI tool_calls. A block normally ends at </DSML calls>, so it is finished
    as soon as that tag arrives; an unterminated block is finished at flush()
    (end of stream). A block that does not parse as a tool call is released back
    as plain text, so nothing is lost when the model merely mentions the markup
    in prose."""

    def __init__(self):
        self.buf = ""
        self.capturing = False

    def feed(self, delta):
        """Returns (visible_text, newly_completed_calls_or_None)."""
        self.buf += delta
        visible_out = ""
        completed = None

        while True:
            if not self.capturing:
                idx = self.buf.find(DSML_CALLS_OPEN)
                if idx == -1:
                    hold = self._partial_hold_len()
                    if hold:
                        visible_out += self.buf[: len(self.buf) - hold]
                        self.buf = self.buf[len(self.buf) - hold:]
                    else:
                        visible_out += self.buf
                        self.buf = ""
                    break
                visible_out += self.buf[:idx]
                self.buf = self.buf[idx:]
                self.capturing = True

            close = self.buf.find(DSML_CALLS_CLOSE)
            reopen = self.buf.find(DSML_CALLS_OPEN, len(DSML_CALLS_OPEN))
            if close != -1 and (reopen == -1 or close < reopen):
                block = self.buf[:close]
                self.buf = self.buf[close + len(DSML_CALLS_CLOSE):]
                self.capturing = False
            elif reopen != -1:
                block = self.buf[:reopen]
                self.buf = self.buf[reopen:]
            else:
                break                      # unterminated, wait for more data

            calls = parse_tool_call_blocks(block)
            if calls:
                completed = (completed or []) + calls   # real call -> not visible
            else:
                visible_out += block                     # false positive -> text

        return visible_out, completed

    def _partial_hold_len(self):
        """If the buffer ends with a prefix of the opening tag, hold it back."""
        for l in range(min(len(DSML_CALLS_OPEN) - 1, len(self.buf)), 0, -1):
            if DSML_CALLS_OPEN.startswith(self.buf[-l:]):
                return l
        return 0

    def flush(self):
        """Final drain: returns (leftover_visible_text, calls_or_None)."""
        leftover, self.buf = self.buf, ""
        self.capturing = False
        if not leftover:
            return "", None
        calls = parse_tool_call_blocks(leftover)
        if not calls:
            return leftover, None
        head = leftover[: leftover.find(DSML_CALLS_OPEN)].lstrip("\r\n")
        return head, calls


# DeepSeek has no "Deep Think" dropdown: the think/search toggles live in
# what the site itself put in the request body, i.e. its own toggle state.

async def poll_js(page, expr, arg=None, timeout_s=10, poll_ms=100):
    deadline = time.time() + timeout_s
    result = None
    while time.time() < deadline:
        try:
            result = await page.evaluate(expr, arg) if arg is not None else await page.evaluate(expr)
            if result:
                return result
        except Exception:
            pass
        await asyncio.sleep(poll_ms / 1000)
    return None


def save_accounts(accounts, rotate_every=None):
    try:
        data = {"rotate_every": int(rotate_every if rotate_every is not None else 10), "accounts": accounts}
        with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        log(f"[accounts] saved {len(accounts)} account(s) to {ACCOUNTS_FILE}")
        return True
    except Exception as e:
        log(f"[accounts] failed to save: {e}", level="ERROR")
        return False


def load_accounts():
    """accounts.json format"""
    try:
        with open(ACCOUNTS_FILE, encoding="utf-8") as f:
            data = json.load(f)
        accounts = data["accounts"] if isinstance(data, dict) else data
        rotate_every = data.get("rotate_every", 10) if isinstance(data, dict) else 10
    except FileNotFoundError:
        log(f"{ACCOUNTS_FILE} not found, using fallback token", level="WARN")
        accounts = [{"name": "default", "token": TOKEN, "email": "", "password": ""}]
        rotate_every = 10
    # allow accounts even without token - we'll refresh them
    log(f"Loaded {len(accounts)} account(s): "
        f"{[a.get('name') or a.get('email') or '?' for a in accounts]}")
    return accounts, int(rotate_every)


import psutil  # noqa: E402


# ===== WINDOW HIDING (Windows only) ========
# CloakBrowser is a real (non-headless) Chromium: DeepSeek's AWS WAF layer and
# the proof-of-work worker both behave differently under true headless, so we
# keep a GUI browser and just hide its window with Win32 when HEADLESS is on.
# SW_HIDE clears the screen, WS_EX_TOOLWINDOW drops the taskbar / Alt+Tab entry.
import psutil  # noqa: E402

CLOAK_CACHE_DIRS = None


def _cloak_chrome_pids():
    """PIDs of Chromium processes running out of the CloakBrowser cache dir."""
    global CLOAK_CACHE_DIRS
    if CLOAK_CACHE_DIRS is None:
        home = os.path.expanduser("~")
        base = os.path.join(home, ".cloakbrowser")
        try:
            CLOAK_CACHE_DIRS = {
                os.path.normcase(os.path.join(base, d))
                for d in os.listdir(base)
                if d.startswith("chromium")
            }
        except OSError:
            CLOAK_CACHE_DIRS = set()
    if not CLOAK_CACHE_DIRS:
        return set()
    pids = set()
    for proc in psutil.process_iter(["exe"]):
        try:
            exe = proc.info.get("exe") or ""
            if not exe:
                continue
            exe_dir = os.path.normcase(os.path.dirname(exe))
            if exe_dir in CLOAK_CACHE_DIRS and exe.lower().endswith("chrome.exe"):
                pids.add(proc.pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return pids


def _hide_windows_for_pids(pids):
    """Hide top-level windows owned by the given PIDs (no-op on non-Windows)."""
    if os.name != "nt" or not pids:
        return
    import ctypes
    from ctypes import wintypes
    user32 = ctypes.windll.user32
    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    kernel32 = ctypes.windll.kernel32

    def _pid_of_window(hwnd):
        pid = wintypes.DWORD()
        user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
        return pid.value

    def _hide(hwnd):
        GCL_EXSTYLE = -20
        WS_EX_TOOLWINDOW = 0x80
        WS_EX_APPWINDOW = 0x40000
        ex = user32.GetWindowLongW(hwnd, GCL_EXSTYLE)
        user32.SetWindowLongW(hwnd, GCL_EXSTYLE, (ex | WS_EX_TOOLWINDOW) & ~WS_EX_APPWINDOW)
        user32.ShowWindow(hwnd, 0)

    @ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
    def _enum_cb(hwnd, lparam):
        if user32.IsWindowVisible(hwnd) and _pid_of_window(hwnd) in pids:
            try:
                _hide(hwnd)
            except Exception:
                pass
        return True

    user32.EnumWindows(_enum_cb, 0)


# ===== STREAM PARSING =====

def collect_image_parts(messages):
    """Pull image_url parts out of the OpenAI-style messages.

    build_prompt deliberately keeps only the text parts (it is the z.ai
    function, unchanged), so images travel to DeepSeek the way the site does
    it: as uploaded attachments, which land in the completion payload as
    ref_file_ids. Only data: URLs and local paths can be handed to the file
    input; remote URLs are fetched here.
    """
    out = []
    for m in messages:
        content = m.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict) or part.get("type") != "image_url":
                continue
            url = (part.get("image_url") or {}).get("url") or ""
            try:
                if url.startswith("data:"):
                    head, _, b64 = url.partition(",")
                    mime = head[5:].split(";")[0] or "image/png"
                    out.append({"data": base64.b64decode(b64), "mime": mime})
                elif url.startswith("http://") or url.startswith("https://"):
                    with urllib.request.urlopen(url, timeout=30) as r:
                        blob = r.read()
                    ctype = (r.headers.get("Content-Type") or "").split(";")[0]
                    if not ctype.startswith("image/"):
                        continue
                    out.append({"data": blob, "mime": ctype})
            except Exception as e:
                log(f"[vision] skipped image ({e})", level="WARN")
    return out


def map_thinking(model=None):
    """Pick DeepSeek's two independent site toggles.

    The site has no reasoning-effort concept at all: DeepThink is a plain
    on/off switch in the composer, and which model you asked for decides it -
    deepseek-reasoner thinks, deepseek-chat does not. Web search is always
    off: the request field is ignored and the toggle is never enabled."""
    return (model == "deepseek-reasoner"), False


# History rendering is kept 1:1 with the z.ai original (build_prompt): OpenAI
# messages become JSONL lines under a "# History" header, tool calls are folded
# into the assistant content as a DSML block, tool results are keyed by
# display label and wrapped in <tool_result>, and the first system message is
# lifted into a role=system line right after the header. The prompt is pasted
# verbatim into DeepSeek's composer, which is single-turn, so the whole
# conversation has to travel as text.
def build_prompt(messages, tools=None):
    last_user = ""
    call_label_by_id = {}

    # pass 1: id -> display label ("name", repeats get "name #2")
    for m in messages:
        tcs = m.get("tool_calls") or []
        if not tcs:
            continue
        counts = {}
        for tc in tcs:
            name = (tc.get("function") or {}).get("name", "unknown")
            counts[name] = counts.get(name, 0) + 1
            label = name if counts[name] == 1 else f"{name} #{counts[name]}"
            cid = tc.get("id")
            if cid:
                call_label_by_id[cid] = label

    def _content_str(c):
        if isinstance(c, list):
            return "\n".join(
                x.get("text", "") for x in c if isinstance(x, dict) and x.get("type") == "text"
            )
        if isinstance(c, str):
            return c
        return json.dumps(c, ensure_ascii=False)

    def _reasoning_str(m):
        """opencode sends assistant reasoning back as content parts
        {"type": "reasoning", "text": ...} (or a reasoning_content field)."""
        c = m.get("content")
        if isinstance(c, list):
            parts = [x.get("text", "") for x in c
                     if isinstance(x, dict) and x.get("type") == "reasoning"]
            return "\n".join(p for p in parts if p)
        rc = m.get("reasoning_content") or m.get("reasoning")
        return str(rc) if rc else ""

    hist_lines = []
    for m in messages:
        role = m.get("role")
        content = _content_str(m.get("content", ""))
        if role == "system":
            hist_lines.append({"role": "system", "content": content})
        elif role == "user":
            last_user = content
            hist_lines.append({"role": "user", "content": content})
        elif role == "assistant":
            reasoning = _reasoning_str(m)
            calls = []
            for tc in m.get("tool_calls") or []:
                fn = tc.get("function") or {}
                raw_args = fn.get("arguments", {})
                if isinstance(raw_args, str):
                    try:
                        args = json.loads(raw_args)
                    except json.JSONDecodeError:
                        args = {"_raw": str(raw_args)}
                else:
                    args = raw_args or {}
                calls.append({"name": fn.get("name", "unknown"), "arguments": args})
            # Tool calls are folded into the SAME content field as native
            # DSML blocks at the end (the shape the model
            # itself must emit), instead of a separate tool_calls key.
            if calls:
                tc_text = "\n".join(
                    tool_call_dsml(c["name"], c.get("arguments") or {}) for c in calls
                )
                if content:
                    content += "\n"
                content += tc_text
            line = {"role": "assistant", "content": content or ""}
            if reasoning:
                line["thinking"] = reasoning
            hist_lines.append(line)
        elif role == "tool":
            label = call_label_by_id.get(m.get("tool_call_id"), "unknown")
            hist_lines.append({"role": "tool", "name": label,
                               "content": f"<tool_result>{content}</tool_result>"})

    # The first system message (if any) becomes the "[System instructions]"
    # block and is placed right after the History header below.
    system_instr = None
    if hist_lines and hist_lines[0]["role"] == "system":
        system_instr = hist_lines.pop(0)["content"]

    parts = []
    tool_names = []

    # 1. tool block first (if tools)
    if tools:
        tool_names = [t.get("function", {}).get("name", "?") for t in tools]
        details = render_tools_block(tools)
        parts.append(TOOL_PROMPT_TEMPLATE.format(tool_details=details, instructions=TOOL_INSTRUCTIONS))
    # 2. Allowed tools (only if tools)
    if tools:
        parts.append("<allowed_tools>\n Allowed tools: " + ", ".join(tool_names) + "\n</allowed_tools>")
    # 3. History header
    parts.append("# History (oldest first), each line is one message:")
    # [System instructions] directly after the history header, as a proper
    # role=system message line
    if system_instr:
        parts.append(json.dumps({"role": "system", "content": system_instr}, ensure_ascii=False))
    # 4. FINAL_SYSTEM_MESSAGE as a separate line (only if tools)
    if tools:
        final_line = json.dumps({"role": "system", "content": FINAL_SYSTEM_MESSAGE}, ensure_ascii=False)
        # The convo text is pasted verbatim into the site's input, so the
        # model reads the JSONL escapes literally (\\n shows as two
        # backslashes). Collapse one layer so \\n / \\" in the source
        # reach the model as the intended single backslash.
        final_line = final_line.replace("\\\\", "\\").replace("\\\\\"", "\\\"")
        parts.append(final_line)
    # 5. Each history line as JSONL
    parts.extend(json.dumps(h, ensure_ascii=False) for h in hist_lines)

    convo = "\n\n".join(parts)

    return (
        f"{convo}\n\n"
        f"---\n"
        f"{SYSTEM_CONTINUE}\n"
        f"Write ONLY the Assistant's response. (Content block)"
    ), last_user


# DeepSeek's wire format, as captured from chat.deepseek.com (build 2.5.0):
#
#   event: ready
#   data: {"request_message_id":1,"response_message_id":2,"model_type":"default"}
#
#   data: {"v":{"response":{"status":"WIP","fragments":[
#            {"id":2,"type":"RESPONSE","content":"ок","references":[]}]}}}
#
#   data: {"p":"response","o":"BATCH","v":[{"p":"accumulated_token_usage","v":38},
#                                          {"p":"quasi_status","v":"FINISHED"}]}
#
#   data: {"p":"response/status","o":"SET","v":"FINISHED"}
#
# Three things this parser has to get right:
#   1. answer text lives in response.fragments[].content, not in a flat
#      "content" field; fragment.type tells thinking from answer;
#   2. a BATCH's inner {"p": ...} paths are RELATIVE to the batch path, so
#      "response" + "quasi_status" is what ends the stream;
#   3. the first snapshot can already contain the full text, so every field is
#      diffed against what was already emitted. That means a snapshot arriving
#      after the incremental APPENDs never re-sends text.
#
# Exactly-once is guaranteed one level up, in stream_tokens(): it only ever
# feeds the bytes appended since the last poll. Feeding an overlapping byte
# range twice is not a supported input.
#
# Older/other builds used OpenAI-style {"choices":[{"delta":{"content":...}}]};
# that shape is still accepted as a fallback.

FINISH_STATES = {"FINISHED", "COMPLETED", "SUCCESS", "DONE",
                 "finished", "completed", "success", "done"}

# fragment.type -> which channel its text belongs to
FRAGMENT_KINDS = {
    "RESPONSE": "answer",
    "THINKING": "thinking",
    "THINK": "thinking",          # what the site actually emits
    "REASONING": "thinking",
    "SEARCH": "search",
    "REFORMATTING": "answer",
}


class StreamState:
    def __init__(self):
        self.status = None
        self.quasi_status = None
        self.error = None
        self.finished = False
        self.usage = None
        self.model_type = None
        # canonical field -> text emitted so far
        self.full = {"answer": "", "thinking": "", "search": ""}
        self.sent = {"answer": 0, "thinking": 0, "search": 0}
        # fragment index -> (kind, text); snapshots reset these
        self.fragments = {}

    # -- emit -----------------------------------------------------------
    def _diff(self, kind, text):
        """Publish only what is new since the last call for this field."""
        if not text:
            return None
        self.full[kind] = text
        n = self.sent[kind]
        if len(text) < n:
            # field shrank (regenerate / snapshot overwrote): resend whole thing
            self.sent[kind] = len(text)
            return (kind, text)
        if len(text) == n:
            return None
        self.sent[kind] = len(text)
        return (kind, text[n:])

    def _note(self, obj):
        """Pick up status/error/usage/model_type from any dict-shaped frame."""
        if not isinstance(obj, dict):
            return
        for key in ("status", "quasi_status", "quasiStatus"):
            v = obj.get(key)
            if isinstance(v, str):
                if key == "status":
                    self.status = v
                else:
                    self.quasi_status = v
        for key in ("model_type", "modelType"):
            v = obj.get(key)
            if isinstance(v, str) and v:
                self.model_type = v
        usage = obj.get("accumulated_token_usage")
        if isinstance(usage, (int, float)):
            self.usage = usage
        elif isinstance(usage, dict):
            self.usage = usage
        err = obj.get("error") or obj.get("error_info")
        if err:
            self.error = err

    def _maybe_finish(self):
        for s in (self.status, self.quasi_status):
            if isinstance(s, str) and s in FINISH_STATES:
                self.finished = True
        return self.finished

    def _active_kind(self):
        """Which channel the site is currently writing to: the kind of the
        newest fragment, falling back to the answer channel."""
        if self.fragments:
            return self.fragments[max(self.fragments)][0]
        return "answer"

    def _rebuild(self, kind):
        """Recompute a channel from its fragments and emit only the new tail."""
        joined = "".join(t for k, t in
                         sorted((self.fragments[i] for i in self.fragments
                                 if self.fragments[i][0] == kind),
                                key=lambda x: x[0]))
        return self._diff(kind, joined)

    def _append_to_active(self, text):
        """Path-less {"v": "tok"}: append to the newest fragment, then rebuild
        that channel. Writing straight into the channel total instead would
        desync it from the fragments, and the next snapshot would then either
        drop these tokens or replay them.

        Ignored once the response is terminal: the site closes the XHR with a
        final snapshot plus status, and a straggling frame after that would
        otherwise append onto text that is already complete."""
        out = []
        if self.finished:
            return out
        if self.fragments:
            idx = max(self.fragments)
            kind, cur = self.fragments[idx]
            self.fragments[idx] = (kind, cur + text)
        else:
            idx, kind = 0, "answer"
            self.fragments[0] = (kind, text)
        got = self._rebuild(kind)
        if got:
            out.append(got)
        return out

    # -- fragments ------------------------------------------------------
    def _ingest_fragments(self, frags):
        """fragments[] is the authoritative list of text blocks. Each entry is
        keyed by index so a BATCH patch to one fragment does not disturb the
        others."""
        out = []
        if not isinstance(frags, list):
            return out
        for idx, frag in enumerate(frags):
            if not isinstance(frag, dict):
                continue
            kind = FRAGMENT_KINDS.get(str(frag.get("type") or "").upper())
            if kind is None:
                continue
            text = frag.get("content")
            if not isinstance(text, str):
                continue
            # A fragment re-announced with SHORTER content than we already have
            # is a stale duplicate of the snapshot the site keeps resending, not
            # a retraction. Blindly overwriting here silently ate characters
            # ("Париж" -> "Па" -> "Паж"), so keep the longer text and only accept
            # a genuine replacement that is a different, longer string.
            prev = self.fragments.get(idx)
            if prev is not None and prev[0] == kind and len(text) < len(prev[1]):
                if prev[1].startswith(text) or text in prev[1]:
                    continue
            self.fragments[idx] = (kind, text)
        # rebuild each channel by concatenating its fragments in index order
        for kind in ("answer", "thinking", "search"):
            joined = "".join(t for k, t in
                             sorted((self.fragments[i]
                                     for i in self.fragments
                                     if self.fragments[i][0] == kind),
                                    key=lambda x: x[0]))
            got = self._diff(kind, joined)
            if got:
                out.append(got)
        return out

    # -- public ---------------------------------------------------------
    def feed_line(self, line):
        """Apply one raw SSE line; return a list of (kind, delta) tuples."""
        line = (line or "").strip()
        if not line or line.startswith(":"):
            return []
        if line.startswith("event:"):
            return []
        if line.startswith("data:"):
            line = line[5:].strip()
        if not line:
            return []
        if line == "[DONE]":
            self.finished = True
            return []
        try:
            obj = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            return []
        if not isinstance(obj, dict):
            return []
        return self._apply(obj)

    def _apply(self, obj):
        out = []

        # (1) legacy OpenAI-style delta
        choices = obj.get("choices")
        if isinstance(choices, list) and choices:
            ch = choices[0] if isinstance(choices[0], dict) else {}
            delta = ch.get("delta") if isinstance(ch.get("delta"), dict) else {}
            for key, kind in (("reasoning_content", "thinking"),
                              ("thinking_content", "thinking"),
                              ("content", "answer")):
                val = delta.get(key)
                if isinstance(val, str) and val:
                    got = self._diff(kind, self.full[kind] + val)
                    if got:
                        out.append(got)
            if ch.get("finish_reason"):
                self.finished = True
            return out

        self._note(obj)

        # (2) full snapshot: {"v": {"response": {...}}} - carries fragments[]
        if "p" not in obj and "o" not in obj:
            v = obj.get("v")
            if isinstance(v, dict):
                resp = v.get("response") if isinstance(v.get("response"), dict) else v
                out += self._ingest_fragments(resp.get("fragments"))
                # tolerate a flat content field too
                for key, kind in (("reasoning_content", "thinking"),
                                  ("thinking_content", "thinking"),
                                  ("content", "answer")):
                    if isinstance(resp.get(key), str) and resp[key]:
                        got = self._diff(kind, resp[key])
                        if got:
                            out.append(got)
                self._note(resp)
                self._maybe_finish()
                return out
            if isinstance(v, str) and v:
                # {"v":"tok"} with no path at all: mid-stream token trickle into
                # whatever fragment the site is currently filling. It has to
                # land IN that fragment, not just in the channel total -
                # otherwise the next snapshot re-derives the channel from
                # fragments and the tokens are lost (or replayed).
                out += self._append_to_active(v)
            return out

        # (3) JSON-Patch operations, possibly batched
        path = (obj.get("p") or "").strip()
        op = (obj.get("o") or "").upper()
        val = obj.get("v")

        if op == "BATCH" or (isinstance(val, list) and not isinstance(path, str)):
            items = val if isinstance(val, list) else []
            for item in items:
                if not isinstance(item, dict):
                    continue
                # inner paths are relative to the batch's own path
                inner = dict(item)
                ip = inner.get("p") or ""
                if path and ip:
                    inner["p"] = f"{path}/{ip}"
                out += self._apply(inner)
            self._maybe_finish()
            return out

        if path in ("status", "quasi_status", "response/status",
                    "response/quasi_status"):
            if isinstance(val, str):
                if "quasi" in path:
                    self.quasi_status = val
                else:
                    self.status = val
            self._maybe_finish()
            return out
        if path.endswith("/status") or path.endswith("/quasi_status"):
            if isinstance(val, str):
                if "quasi" in path:
                    self.quasi_status = val
                else:
                    self.status = val
            self._maybe_finish()
            return out
        if "error" in path:
            self.error = val if val is not None else obj.get("v")
            return out

        # fragments[i]/content and friends
        if "fragments/" in path and path.endswith("/content"):
            try:
                idx = int(path.split("fragments/")[1].split("/")[0])
            except (ValueError, IndexError):
                return out
            # The site writes to index -1 to mean "the last fragment appended",
            # which is how the token trickle arrives mid-stream.
            if idx == -1:
                idx = max(self.fragments) if self.fragments else 0
            prev_kind, prev_text = self.fragments.get(idx, ("answer", ""))
            kind = prev_kind
            text = prev_text
            if isinstance(val, str):
                # The /content patch arrives with a varying shape:
                #   "o":"APPEND"      -> a pure delta, always append
                #   no "o"            -> usually the fragment's FULL text, but
                #                       sometimes only the newer part ("ся"
                #                       right after the fragment was seeded with
                #                       "де"), which used to drop characters
                #   growing full text -> "де" then "деся" then "десять"
                # So a no-"o" frame is merged rather than blindly assigned.
                if op in ("APPEND", "ADD"):
                    text = prev_text + val
                elif not prev_text:
                    text = val
                elif val == prev_text:
                    text = prev_text            # plain re-send
                elif val.startswith(prev_text):
                    text = val                   # site grew the full text
                elif prev_text.endswith(val):
                    text = prev_text              # suffix, nothing new
                else:
                    text = prev_text + val
                self.fragments[idx] = (kind, text)
            elif isinstance(val, dict):
                ftype = str(val.get("type") or "").upper()
                kind = FRAGMENT_KINDS.get(ftype, kind)
                if isinstance(val.get("content"), str):
                    text = val["content"]
                self.fragments[idx] = (kind, text)
            got = self._rebuild(kind)
            if got:
                out.append(got)
            return out

        # a whole fragment object arrived
        if "fragments" in path and isinstance(val, list):
            out += self._ingest_fragments(val)
            return out
        if "fragments" in path and isinstance(val, dict):
            out += self._ingest_fragments([val])
            return out

        # flat content-style paths
        if isinstance(val, str):
            kind = ("thinking" if ("thinking" in path or "reasoning" in path)                    else "answer" if "content" in path else None)
            if kind:
                text = val if op in ("SET", "REPLACE") else self.full[kind] + val
                got = self._diff(kind, text)
                if got:
                    out.append(got)
            return out

        if isinstance(val, dict):
            out += self._ingest_fragments(val.get("fragments"))
            for key, kind in (("reasoning_content", "thinking"),
                              ("thinking_content", "thinking"),
                              ("content", "answer")):
                if isinstance(val.get(key), str) and val[key]:
                    got = self._diff(kind, val[key])
                    if got:
                        out.append(got)
            self._note(val)
            self._maybe_finish()
            return out

        return out


# Type into the composer. DeepSeek drives the textarea from React, so a plain
# el.value = x is swallowed; the native setter + input event is what it listens
# to. Playwright's fill() works too, this is the explicit version.
FILL_JS = """
    (text) => {
        const ta = [...document.querySelectorAll('textarea')].find(t => t.offsetParent);
        if (!ta) return false;
        const desc = Object.getOwnPropertyDescriptor(
            Object.getPrototypeOf(ta), 'value');
        if (desc && desc.set) desc.set.call(ta, text);
        else ta.value = text;
        ta.dispatchEvent(new Event('input', { bubbles: true }));
        ta.dispatchEvent(new Event('change', { bubbles: true }));
        return true;
    }
"""


class DeepSeekSession:
    """One CloakBrowser context pinned to one DeepSeek account.

    Why drive the real UI instead of replaying HTTP: /api/v0/chat/completion
    requires a proof-of-work header (DeepSeekHashV1, solved by the page's own
    Web Worker in ~150ms) and a short-lived AWS WAF cookie. Both come for free
    if the real page issues the request, and re-implementing them in Python is
    both slower and far more brittle.
    """

    def __init__(self, worker_id=0, accounts=None, rotate_every=None):
        self.worker_id = worker_id
        self.busy = False
        self.page = None
        # Set by the streaming relay when the client goes away. The browser
        # poll loop watches this itself: it is the only place that ticks
        # reliably while the page sits there generating.
        self.client_gone = None
        self.context = None
        self.lock = asyncio.Lock()
        self.last_activity = 0.0
        self.hide_pids = set()
        self.last_debug = None
        self._search = False
        self._model_type = None
        if accounts is None:
            accounts, rotate_every = load_accounts()
        self.accounts = accounts
        self.rotate_every = rotate_every if rotate_every is not None else 10
        self.account_idx = 0
        self.requests_on_account = 0

    @property
    def current_account(self):
        return self.accounts[self.account_idx]

    def _label(self, idx):
        a = self.accounts[idx]
        return a.get("name") or a.get("email") or "unnamed"

    # ---------------- lifecycle ----------------
    async def start(self):
        # headless=False on purpose: true headless is fingerprinted and the WAF
        # layer notices. HEADLESS only means "hide the window" (see below).
        self.context = await launch_context_async(
            headless=False,
            viewport={"width": 1536, "height": 735},
            locale="en-US",
            color_scheme="light",
        )
        await self.context.add_init_script(ACCOUNT_SHIM_JS)
        if HEADLESS:
            self.hide_pids = _cloak_chrome_pids()
            _hide_windows_for_pids(self.hide_pids)
            asyncio.create_task(self._early_hide())
        self.page = await self.context.new_page()
        self.page.on("pageerror", self._on_pageerror)
        await self._apply_account(self.current_account)
        await self._open_chat()
        # _open_chat raises unless the page is genuinely signed in, so
        # reaching this line really does mean we have a session
        log(f"[browser] worker #{self.worker_id} up")

    async def _early_hide(self):
        # the top-level window materialises a few ms after launch returns
        for _ in range(60):
            _hide_windows_for_pids(self.hide_pids)
            await asyncio.sleep(0.05)

    def _on_pageerror(self, err):
        s = str(err).lower()
        if any(k in s for k in ("xhr", "network", "fetch", "timeout", "err_")):
            log(f"[page error] {err}", level="ERROR")

    async def _apply_account(self, acc):
        """Stage the account for the next navigation (picked up and erased by
        ACCOUNT_SHIM_JS before the app bundle runs)."""
        await self.context.add_cookies([{
            "name": ACCOUNT_COOKIE,
            "value": _account_cookie_value(acc),
            "domain": "chat.deepseek.com",
            "path": "/",
        }])

    async def _is_authenticated(self):
        """Composer on screen AND a token the server still accepts.

        Presence is not enough: ACCOUNT_SHIM_JS re-injects the account token
        into localStorage on every navigation, so a dead one is always
        non-null and the page still renders a textarea on the sign-in wall.
        The only trustworthy signal is chat_session/create answering code 0.
        """
        try:
            token = await self.page.evaluate(SESSION_TOKEN_JS)
            if not token:
                return False
            if not await self.page.evaluate(AUTHED_CHAT_JS):
                return False
            probe = await self.page.evaluate(TOKEN_PROBE_JS, token)
            return bool(probe and probe.get("ok"))
        except Exception:
            return False

    async def _browser_login(self, acc):
        """Sign in through the real form and return the fresh userToken.

        DeepSeek rotates tokens with no refresh endpoint, so the only way back
        in is a full credential login. Returns "" on any failure.
        """
        email = (acc.get("email") or "").strip()
        password = acc.get("password") or ""
        if not (email and password):
            log("[auth] account has no email/password - cannot log in", level="ERROR")
            return ""
        log(f"[auth] signing in as {email}")
        try:
            await self.page.goto(SIGN_IN_URL, wait_until="domcontentloaded", timeout=90000)
            if not await poll_js(self.page, LOGIN_FORM_READY_JS, timeout_s=30):
                log("[auth] sign-in form never rendered", level="ERROR")
                return ""
            await self.page.fill(EMAIL_INPUT_SEL, email)
            await self.page.fill(PASSWORD_INPUT_SEL, password)
            await asyncio.sleep(0.5)
            btn = self.page.locator(LOGIN_BUTTON_SEL)
            if await btn.count() and await btn.first.is_visible():
                await btn.first.click()
            else:
                await self.page.press(PASSWORD_INPUT_SEL, 'Enter')
            for _ in range(20):
                await asyncio.sleep(1.5)
                if "/sign_in" not in (self.page.url or ""):
                    break
            token = await self.page.evaluate(SESSION_TOKEN_JS)
        except Exception as e:
            log(f"[auth] login failed: {e}", level="ERROR")
            return ""
        if not token:
            log("[auth] login returned no token", level="ERROR")
        return token or ""

    async def _ensure_signed_in(self):
        """Make sure this worker is on a genuinely authenticated chat.

        Returns False (rather than raising) so the caller decides how to fail.
        """
        if await self._is_authenticated():
            return True
        acc = self.current_account
        if not AUTO_REFRESH:
            log("[auth] not signed in and 'Auto Refresh Tokens' is OFF", level="ERROR")
            return False
        log("[auth] not signed in (stale token?) - logging in", level="WARN")
        async with _auth_lock():
            # another worker may have rotated the shared token while we waited
            await self._apply_account(acc)
            if await self._is_authenticated():
                log("[auth] token was already refreshed by another worker")
                return True
            token = await self._browser_login(acc)
        if not token:
            return False
        if token != acc.get("token"):
            acc["token"] = token
            save_accounts(self.accounts, self.rotate_every)
            log("[auth] token refreshed and saved to accounts.json")
        # the account shim reads the token from a cookie, so re-stage it before
        # the next navigation
        await self._apply_account(acc)
        # the app needs a beat to mount the composer on a brand-new session
        if not await poll_js(self.page, AUTHED_CHAT_JS, timeout_s=CHAT_URL_READY_TIMEOUT):
            log(f"[auth] {await self._no_composer_reason()}", level="ERROR")
            return False
        return await self._is_authenticated()

    async def _open_chat(self):
        """Load a brand-new chat. DeepSeek pre-creates a chat session on every
        page load, so navigating to '/' gives each API call a clean context."""
        await self.page.goto(CHAT_URL, wait_until="domcontentloaded", timeout=90000)
        await poll_js(self.page, INPUT_READY_JS, timeout_s=CHAT_URL_READY_TIMEOUT)
        if not await self._ensure_signed_in():
            raise RuntimeError("chat.deepseek.com is not signed in and login failed")
        if not await poll_js(self.page, INPUT_READY_JS, timeout_s=CHAT_URL_READY_TIMEOUT):
            raise RuntimeError(await self._no_composer_reason())
        return True

    async def _no_composer_reason(self):
        """Explain a missing composer. A suspended account renders a notice
        instead of the chat UI, and no amount of logging in will fix that."""
        try:
            suspended = await self.page.evaluate(SUSPENDED_JS)
        except Exception:
            suspended = ''
        if suspended:
            log(f"[auth] account is blocked by DeepSeek: {suspended}", level="ERROR")
            return f"account unavailable: {suspended}"
        return "chat.deepseek.com never rendered the composer"

    # ---------------- rotation / retry ----------------
    async def switch_account(self, idx):
        log(f"[rotate] -> account #{idx} ({self._label(idx)})")
        await self._wipe()
        self.account_idx = idx
        self.requests_on_account = 0
        self._model_type = None
        await self._apply_account(self.current_account)
        await self._open_chat()

    async def reload_current(self):
        """Wipe browser state and reload the SAME account, to shed a stale
        AWS WAF cookie. Never touches the rotation counters."""
        acc = self.current_account
        await self._wipe()
        await self._apply_account(acc)
        try:
            await self.page.reload(wait_until="domcontentloaded", timeout=90000)
        except Exception:
            await self.page.goto(CHAT_URL, wait_until="domcontentloaded", timeout=90000)
        if "chat.deepseek.com" not in (self.page.url or ""):
            await self.page.goto(CHAT_URL, wait_until="domcontentloaded", timeout=90000)
        await poll_js(self.page, INPUT_READY_JS, timeout_s=CHAT_URL_READY_TIMEOUT)
        if not await self._ensure_signed_in():
            raise RuntimeError("page is not signed in and login failed after reload")
        if not await poll_js(self.page, INPUT_READY_JS, timeout_s=CHAT_URL_READY_TIMEOUT):
            raise RuntimeError("page never became ready after reload")

    async def _wipe(self):
        for fn in ("evaluate", "clear_cookies"):
            try:
                if fn == "evaluate":
                    await self.page.evaluate(
                        "try{localStorage.clear();sessionStorage.clear();}catch(e){}")
                else:
                    await self.context.clear_cookies()
            except Exception:
                pass

    # async def is_captcha(self):
    #     try:
    #         return bool(await self.page.evaluate(CAPTCHA_JS))
    #     except Exception:
    #         return False

    async def is_logged_out(self):
        try:
            return bool(await self.page.evaluate(LOGIN_WALL_JS))
        except Exception:
            return False

    async def before_request(self):
        """Called inside the lock: rotate if this account has had its turn."""
        if ACCOUNT_ROTATE and self.requests_on_account >= self.rotate_every:
            if len(self.accounts) > 1:
                await self.switch_account((self.account_idx + 1) % len(self.accounts))
            else:
                self.requests_on_account = 0
        self.requests_on_account += 1
        return self.current_account

    async def rate_limit(self):
        wait = REQUEST_COOLDOWN - (time.time() - self.last_activity)
        if wait > 0:
            log(f"[rate-limit] cooldown {wait:.1f}s before next request")
            await asyncio.sleep(wait)
        self.last_activity = time.time()

    # ---------------- models / prefs ----------------
    # The model picker exists, but it is decorative: DeepSeek unified
    # Instant/Expert/Vision into a single model, and every option in the menu
    # still sends model_type="default". The only controls that change the
    # request are the two `div.ds-toggle-button` switches in the composer
    # ("DeepThink" and "Search"), so `deepseek-reasoner` is expressed as
    # DeepThink on, exactly as the web UI does it.
    async def get_models(self):
        return MODELS

    async def _set_force(self, **fields):
        """Stage the payload backstop; the init script applies it on send()."""
        try:
            await self.page.evaluate(SET_FORCE_JS, fields)
        except Exception as e:
            log(f"[prefs] could not stage override: {e}", level="WARN")

    async def set_prefs(self, thinking, search):
        """Flip the composer's DeepThink / Search switches to the wanted state.

        The site persists them between requests, so SET_TOGGLES_JS only clicks
        a switch that is not already correct. The payload override is staged
        as well, in case the markup is missing on this page.
        """
        thinking, search = bool(thinking), bool(search)
        self._search = search
        try:
            await self.page.evaluate(
                SET_TOGGLES_JS, {"thinking": thinking, "search": search})
        except Exception as e:
            log(f"[prefs] toggle click failed: {e}", level="WARN")
        await asyncio.sleep(0.4)
        await self._set_force(thinking=thinking, search=search)

    async def set_model(self, model_type):
        """Accepted for API compatibility with the z.ai session, but inert:
        DeepSeek serves one merged model and the picker sends the same
        model_type="default" for every option, so there is nothing to click.
        Model selection actually happens in map_thinking(), via DeepThink."""
        self._model_type = model_type or "default"

    async def stop_generation(self):
        """Click the stop button. Returns True if it was actually found."""
        try:
            return bool(await self.page.evaluate(STOP_GENERATION_JS))
        except Exception:
            return False

    # ---------------- send / stream ----------------
    async def _attach_images(self, images):
        """Hand image files to the site's own file input.

        The composer owns an <input type=file> whose accepted types are listed
        in its `accept` attribute. Filling it makes the page upload the file
        and put the resulting id into the completion payload as ref_file_ids,
        which is how DeepSeek does vision. The prompt text itself is unchanged
        (build_prompt keeps only the text parts, as it always did)."""
        paths = []
        try:
            for i, blob in enumerate(images):
                suffix = {"image/png": ".png", "image/jpeg": ".jpg",
                          "image/webp": ".webp", "image/gif": ".gif"}.get(
                              blob.get("mime", ""), ".png")
                fd, path = tempfile.mkstemp(prefix="dsimg", suffix=suffix)
                with os.fdopen(fd, "wb") as fh:
                    fh.write(blob["data"])
                paths.append(path)
            await self.page.locator("input[type=file]").first.set_input_files(paths)
            # the site uploads asynchronously; ref_file_ids lands in the payload
            await asyncio.sleep(IMAGE_UPLOAD_WAIT)
        finally:
            for p_ in paths:
                try:
                    os.unlink(p_)
                except OSError:
                    pass

    async def send_message(self, prompt, thinking=False, search=False,
                           model_type=DEFAULT_MODEL_TYPE, images=None):
        """Fresh page -> apply prefs -> attach files -> type -> Enter. The site
        then creates the session, solves PoW and opens the completion XHR on
        its own."""
        self._model_type = None
        await self._open_chat()
        await self.set_prefs(thinking, search)
        await self.set_model(model_type)

        ta = self.page.locator(TEXTAREA_SEL).first
        await ta.wait_for(state="visible", timeout=20000)
        if images:
            await self._attach_images(images)
        if not await self.page.evaluate(FILL_JS, prompt):
            raise RuntimeError("composer disappeared before the prompt was typed")
        await asyncio.sleep(0.1)
        await ta.press("Enter")

    async def stop_if_client_gone(self):
        """Click the site's stop button if the client hung up. Safe to call
        every poll tick; it no-ops until the relay flags a disconnect."""
        if not (self.client_gone and self.client_gone.is_set()):
            return False
        self.client_gone = None          # one shot: don't click twice
        stopped = await self.stop_generation()
        log(f"[stream] client disconnected -> stop button "
            f"{'clicked' if stopped else 'NOT found'}")
        return True

    async def stream_tokens(self, timeout_s=STREAM_IDLE_TIMEOUT):
        """Poll the mirrored XHR buffer, yield (kind, delta) where kind is
        'thinking' | 'answer' | 'error' | 'done'."""
        st = StreamState()
        consumed = 0
        last_len = 0
        last_growth = time.time()
        started = False
        dom_mode = False
        # last_captcha_check = 0.0

        while True:
            if await self.stop_if_client_gone():
                return

            snap = await self.page.evaluate(TAP_POLL_JS)
            self.last_debug = snap
            raw = snap.get("raw") or ""

            if len(raw) > last_len:
                if not started:
                    started = True
                last_len = len(raw)
                last_growth = time.time()
                for line in raw[consumed:].splitlines():
                    for kind, delta in st.feed_line(line):
                        yield kind, delta
                consumed = last_len

            if st.error:
                yield "error", str(st.error)
                return
            if st.finished:
                yield "done", ""
                return

            now = time.time()
            # if now - last_captcha_check > 1.0:
            #     last_captcha_check = now
            #     if CAPTCHA_BYPASS and await self.is_captcha():
            #         yield "error", "captcha"
            #         return

            if not started:
                # the tap stayed silent: responseType we cannot read, or the
                # site moved transports. Fall back to scraping the answer.
                if not dom_mode and now - last_growth > 12:
                    dom_mode = True
                    log("[stream] XHR tap silent, falling back to DOM scraping",
                        level="WARN")
                if dom_mode:
                    dom = (await self.page.evaluate(DOM_ANSWER_JS)) or ""
                    if len(dom) > st.sent.get("answer", 0):
                        got = st._diff("answer", dom)
                        if got:
                            yield got
                        last_growth = time.time()
                    elif dom and time.time() - last_growth > 3:
                        yield "done", ""
                        return
                    await asyncio.sleep(0.25)
                    continue

            if now - last_growth > timeout_s:
                yield "error", f"no stream data for {timeout_s}s"
                return
            await asyncio.sleep(STREAM_POLL_MS / 1000)

    async def close(self):
        try:
            if self.context is not None:
                await self.context.close()
        except Exception:
            pass


# ===== WORKER POOL =====

class WorkerPool:
    def __init__(self):
        accounts, rotate_every = load_accounts()
        self._accounts = accounts
        self._rotate_every = rotate_every
        self._workers = []            # all DeepSeekSession ever created
        self._idle = []               # free workers (queue discipline)
        self._counter = 0
        self._hider_task = None

    async def acquire(self):
        """Return a worker, spawning a fresh browser if none is idle."""
        while True:
            if self._idle:
                wk = self._idle.pop(0)
                wk.busy = True
                return wk
            wk = DeepSeekSession(
                worker_id=self._counter,
                accounts=self._accounts,
                rotate_every=self._rotate_every,
            )
            self._counter += 1
            self._workers.append(wk)
            wk.busy = True
            try:
                await wk.start()
            except Exception:
                self._workers.remove(wk)
                raise
            log(f"[pool] spawned worker #{wk.worker_id} ({len(self._workers)} browsers alive, {len(self._idle)} idle)")
            return wk

    def release(self, wk):
        """Return a busy worker to the idle pool."""
        if wk is None or wk not in self._workers:
            return
        wk.busy = False
        self._idle.append(wk)

    def start_hider(self):
        """Background loop: keep all worker browser windows hidden (HEADLESS only)."""
        if not HEADLESS or os.name != "nt" or self._hider_task is not None:
            return
        async def _hider():
            while True:
                pids = set()
                for wk in self._workers:
                    pids.update(getattr(wk, "hide_pids", set()))
                _hide_windows_for_pids(pids)
                await asyncio.sleep(0.1)
        self._hider_task = asyncio.create_task(_hider())

    @property
    def active_count(self):
        return sum(1 for w in self._workers if w.busy)

    def usage_stats(self):
        """Aggregate counters for /accounts (worst-case: first busy worker)."""
        for wk in self._workers:
            if wk.busy:
                return wk
        return self._workers[0] if self._workers else None

    async def shutdown(self):
        for wk in list(self._workers):
            await wk.close()
        self._workers.clear()
        self._idle.clear()


pool = WorkerPool()

# ===== FASTAPI APP =====

app = FastAPI(title="chat.deepseek.com -> OpenAI compatible proxy")


@app.get("/")
async def root():
    return {"status": "ok", "endpoints": ["/v1/models", "/v1/chat/completions", "/accounts"]}


@app.get("/accounts")
async def accounts_status():
    stats = pool.usage_stats()
    return {
        "active_requests": pool.active_count,
        "browsers": len(pool._workers),
        "current_index": stats.account_idx if stats else 0,
        "requests_on_account": stats.requests_on_account if stats else 0,
        "rotate_every": pool._rotate_every,
        "accounts": [
            {
                "index": i,
                "name": a.get("name") or a.get("email") or "unnamed",
                "active": i == (pool.usage_stats().account_idx if pool.usage_stats() else -1),
                "token_preview": (a.get("token") or "")[:8] + "...",
            }
            for i, a in enumerate(pool._accounts)
        ],
    }


# DeepSeek exposes exactly three model_type buckets. Serving this list
# statically matters: /v1/models must answer BEFORE any browser is spawned,
# otherwise every OpenAI client blocks on a Chromium launch just to list models.
# OpenAI-facing model ids, read off the site's own backend:
#   GET /api/v0/client/settings?scope=model  ->  settings.model_configs.value
# That feature list currently holds THREE entries and the server ships two of
# them switched off for this account:
#   model_type=default  name=Instant  enabled=true   switchable=true
#   model_type=expert   name=Expert   enabled=false  switchable=false
#   model_type=vision   name=Vision   enabled=false  switchable=false
# Only one entry is switchable, which is why the web UI shows no model picker
# at all, and why the request always carries model_type="default". Instant,
# Expert and Vision are one merged multimodal model behind the scenes: all
# three entries share input_character_limit=2621440 and the same 985 supported
# file extensions (png/webp included).
#
# So the site itself lists three ids, but we expose two:
#   deepseek-chat     -> DeepThink off, no reasoning_content
#   deepseek-reasoner -> DeepThink on, reasoning_content populated
# deepseek-vision is deliberately NOT exposed: it is the same merged
# multimodal model, verified by sending the same picture to all three ids
# (Red/Green/Blue back, and without the attachment all three answer "I can't
# see the image"). Image parts are attached for every model regardless.
# "type" is kept in the /v1/models payload for clients that read it; the site
# ignores it.
MODELS = [
    {"id": "deepseek-chat", "type": "default", "name": "Instant"},
    {"id": "deepseek-reasoner", "type": "default", "name": "Expert"},
]
MODEL_TYPE_BY_ID = {m["id"]: m["type"] for m in MODELS}


@app.get("/debug/last")
async def debug_last():
    """Raw view of the last stream poll: XHR responseType, buffer length,
    transport, and any page error. This is the endpoint to look at first when
    a request returns an empty answer."""
    wk = pool.usage_stats() or (pool._idle[0] if pool._idle else None)
    if wk is None:
        return {"error": "no browser session yet - send a request first"}
    d = wk.last_debug or {}
    return {
        "account": wk._label(wk.account_idx),
        "responseType": d.get("responseType"),
        "complete": d.get("complete"),
        "status": d.get("status"),
        "url": (d.get("url") or "")[:120],
        "buffer_chars": len(d.get("raw") or ""),
        "buffer_tail": (d.get("raw") or "")[-1200:],
        "page_error": d.get("error"),
    }


@app.get("/v1/models")
async def list_models():
    now = int(time.time())
    return {
        "object": "list",
        "data": [
            {"id": m["id"], "object": "model", "created": now, "owned_by": "deepseek",
             "permission": [], "root": m["id"], "parent": None}
            for m in MODELS
        ],
    }


def make_chunk(chunk_id, created, model, delta, finish_reason=None):
    return {
        "id": chunk_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
    }


def _msg_chars(m):
    try:
        return len(json.dumps(m, ensure_ascii=False, separators=(",", ":")))
    except Exception:
        return len(str(m))


def _non_text_part_chars(m):
    """Chars contributed by image / audio content parts inside a message."""
    image_chars = audio_chars = 0
    content = m.get("content")
    if isinstance(content, list):
        for part in content:
            if not isinstance(part, dict):
                continue
            t = part.get("type")
            if t == "image_url":
                image_chars += _msg_chars(part.get("image_url") or {})
            elif t == "input_audio":
                audio_chars += _msg_chars(part.get("input_audio") or {})
    return image_chars, audio_chars


def build_usage(messages, full_reasoning, full_answer):
    """Char counts (1 token := 1 character), matching the site's
    ~2M real limit which is made of characters. Output follows the standard
    OpenAI usage shape (prompt_tokens_details / completion_tokens_details):
    - prompt_tokens   = every non-assistant message (user/system/developer/
        tool/function ...) serialized as JSON, incl. image/audio chars;
    - completion_tokens = every assistant message serialized as JSON (incl.
        tool_calls) + the newly generated reasoning/answer.
    """
    prompt_text = prompt_image = prompt_audio = completion_hist = 0
    for m in messages or []:
        n = _msg_chars(m)
        if (m.get("role") or "unknown") == "assistant":
            completion_hist += n
        else:
            prompt_text += n
        img, aud = _non_text_part_chars(m)
        prompt_image += img
        prompt_audio += aud
    prompt_image = min(prompt_image, prompt_text)
    prompt_audio = min(prompt_audio, prompt_text - prompt_image)

    reasoning_est = sum(len(x) for x in full_reasoning)
    answer_est = sum(len(x) for x in full_answer)
    completion_len = completion_hist + reasoning_est + answer_est

    return {
        "prompt_tokens": prompt_text,
        "prompt_tokens_details": {
            "cached_tokens": 0,
            "audio_tokens": prompt_audio,
            "image_tokens": prompt_image,
            "cached_tokens_details": {
                "text_tokens": prompt_text - prompt_image - prompt_audio,
                "audio_tokens": 0,
                "image_tokens": 0,
            },
        },
        "completion_tokens": completion_len,
        "completion_tokens_details": {
            "reasoning_tokens": reasoning_est,
            "accepted_prediction_tokens": 0,
            "rejected_prediction_tokens": 0,
            "audio_tokens": 0,
        },
        "total_tokens": prompt_text + completion_len,
    }


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    body = await request.json()
    req_model = body.get("model", FALLBACK_MODEL)
    messages = body.get("messages", [])
    stream = body.get("stream", False)
    chunk_id = f"chatcmpl-{uuid.uuid4().hex[:24]}"
    created = int(time.time())

    # validate model (static list - never spawns a browser)
    avail_ids = [m["id"] for m in MODELS]
    # case-insensitive match -> canonical site ID
    avail_map = {mid.lower(): mid for mid in avail_ids}
    req_model_canonical = avail_map.get(req_model.lower())
    if req_model_canonical is None:
        return JSONResponse({
            "error": {
                "message": f"model '{req_model}' not found. Available: {avail_ids}",
                "type": "invalid_request_error",
            }
        }, status_code=400)
    req_model = req_model_canonical

    # DeepSeek chat is a fresh single-shot page load per request (the site
    # mints a new chat_session_id on every navigation and has no API to replay
    # a prior conversation), so history is flattened into one prompt. The tool
    # schema is merged in by the caller, exactly like the z.ai version did.
    has_tools = bool(body.get("tools"))
    prompt, _last_user = build_prompt(messages, tools=body.get("tools"))
    model_type = MODEL_TYPE_BY_ID.get(req_model, DEFAULT_MODEL_TYPE)
    # DeepSeek merged Instant/Expert/Vision into one multimodal model, so image
    # parts are attached for every model, not just deepseek-vision.
    images = collect_image_parts(messages)
    thinking, search = map_thinking(req_model)
    prompt_len = len(prompt)
    log(f"<-- request: history msgs={len(messages)} prompt_len={prompt_len} "
        f"| model={req_model}")

    def sse(obj):
        return f"data: {json.dumps(obj)}\n\n"

    async def generate(client_gone):
        # Each /v1/chat/completions stream runs on its OWN worker (its own
        # browser/context/page), so multiple requests can generate in parallel.
        wk = await pool.acquire()
        wk.client_gone = client_gone      # poll loop watches this for a hangup
        try:
            try:
                await wk.rate_limit()
                acc = await wk.before_request()
                log(f"[account] serving via '{acc.get('name') or acc.get('email')}' ({wk.requests_on_account}/{wk.rotate_every})")
            except Exception as e:
                log(f"prepare/send failed: {e}", level="ERROR")
                yield sse({"error": {"message": str(e), "type": "proxy_error"}})
                yield "data: [DONE]\n\n"
                return

            # Universal retry: ANY failure (page fetch error,
            # stream error, timeout, exception) while NOTHING has been sent to
            # the client yet -> reload the same account and retry. Once output
            # started a retry would duplicate tokens, so we stop retrying then.
            retries = 0
            while True:
                full_reasoning = []
                full_answer = []
                tool_buf = ToolStreamBuffer() if has_tools else None
                finish_reason = "stop"
                tool_call_index = 0
                answer_started = False
                fail_reason = None

                try:
                    await wk.send_message(prompt, thinking=thinking,
                                          search=search, model_type=model_type,
                                          images=images)
                except Exception as e:
                    # if await wk.is_captcha() and CAPTCHA_BYPASS:
                    #     fail_reason = "captcha appeared"
                    # else:
                    fail_reason = str(e)

                if fail_reason is None:
                    async for phase, delta in wk.stream_tokens():
                        if phase == "error":
                            # fail_reason = ("captcha appeared"
                            #                if delta == "captcha" else delta)
                            fail_reason = delta
                            break
                        if phase == "thinking":
                            answer_started = True
                            full_reasoning.append(delta)
                            yield sse(make_chunk(chunk_id, created, req_model, {"reasoning_content": delta}))
                            continue

                        calls_batch = None
                        visible = delta
                        if tool_buf is not None:
                            visible, calls_batch = tool_buf.feed(delta)
                        else:
                            full_answer.append(delta)
                        if visible:
                            answer_started = True
                            full_answer.append(visible)
                            yield sse(make_chunk(chunk_id, created, req_model, {"content": visible}))
                        if calls_batch:
                            finish_reason = "tool_calls"
                            last_sent = 0.0  # wall-clock throttle between calls
                            for tc in calls_batch:
                                if last_sent:
                                    # send next call only if TOOL_CALL_DELAY has
                                    # passed since the previous one; otherwise wait
                                    remaining = TOOL_CALL_DELAY - (time.time() - last_sent)
                                    if remaining > 0:
                                        await asyncio.sleep(remaining)
                                yield sse(make_chunk(chunk_id, created, req_model, {
                                    "tool_calls": [{
                                        "index": tool_call_index,
                                        "id": "call_" + secrets.token_hex(8),
                                        "type": "function",
                                        "function": {"name": tc["name"], "arguments": tc["arguments"]},
                                    }]
                                }))
                                tool_call_index += 1
                                last_sent = time.time()
                            answer_started = True

                if fail_reason:
                    if "upstream 413" in fail_reason or "Request Entity Too Large" in fail_reason:
                        # payload over the site limit -> never retry (same result)
                        log(f"[request] {fail_reason}", level="ERROR")
                        yield sse({"error": {"message": fail_reason, "type": "proxy_error"}})
                        yield "data: [DONE]\n\n"
                        return
                    if answer_started:
                        # output already delivered -> a retry would duplicate tokens
                        yield sse({"error": {"message": fail_reason, "type": "proxy_error"}})
                        yield "data: [DONE]\n\n"
                        return
                    # if fail_reason == "captcha appeared":
                    #     log(f"[request] {fail_reason} -> retry", level="WARN")
                    #     try:
                    #         await wk.reload_current()
                    #     except Exception as e:
                    #         fail_reason = str(e)
                    #     else:
                    #         continue
                    retries += 1
                    if retries >= MAX_REQUEST_RETRIES:
                        log(f"[request] giving up after {retries} retries: {fail_reason}", level="ERROR")
                        yield sse({"error": {"message": fail_reason, "type": "proxy_error"}})
                        yield "data: [DONE]\n\n"
                        return
                    log(f"[request] {fail_reason} -> retry {retries}/{MAX_REQUEST_RETRIES}", level="WARN")
                    try:
                        await wk.reload_current()
                    except Exception as e:
                        fail_reason = str(e)
                    continue

                # clean completion
                break

            try:
                if tool_buf is not None:
                    leftover, tail_calls = tool_buf.flush()
                    if tail_calls:
                        if finish_reason != "tool_calls":
                            tool_call_index = 0
                        finish_reason = "tool_calls"
                        for tc in tail_calls:
                            yield sse(make_chunk(chunk_id, created, req_model, {
                                "tool_calls": [{
                                    "index": tool_call_index,
                                    "id": "call_" + secrets.token_hex(8),
                                    "type": "function",
                                    "function": {"name": tc["name"], "arguments": tc["arguments"]},
                                }]
                            }))
                            tool_call_index += 1
                    if leftover:
                        full_answer.append(leftover)
                        yield sse(make_chunk(chunk_id, created, req_model, {"content": leftover}))

                yield sse(make_chunk(chunk_id, created, req_model, {}, finish_reason=finish_reason))
                usage_out = build_usage(messages, full_reasoning, full_answer)
                yield sse({
                    "id": chunk_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": req_model,
                    "choices": [],
                    "usage": usage_out,
                })
                yield "data: [DONE]\n\n"
            finally:
                log(f"--> done: reasoning={sum(len(x) for x in full_reasoning)}ch "
                    f"answer={sum(len(x) for x in full_answer)}ch "
f"prompt_len={prompt_len} | model={req_model}", level="OK")
                with open("last_response.json", "w", encoding="utf-8") as f:
                    json.dump({"reasoning": "".join(full_reasoning), "answer": "".join(full_answer)},
                              f, ensure_ascii=False, indent=2)
        finally:
            pool.release(wk)

    if stream:
        return StreamingResponse(_stream_relay(generate), media_type="text/event-stream")

    # non-streaming: accumulate (own worker per request, like the stream path)
    wk = await pool.acquire()
    try:
        try:
            await wk.rate_limit()
            await wk.before_request()
        except Exception as e:
            log(f"prepare/send failed: {e}", level="ERROR")
            return JSONResponse({"error": {"message": str(e)}}, status_code=502)

        # Universal retry: ANY failure while NOTHING has accumulated yet ->
        # reload the same account and retry. Once output started a retry would
        # duplicate tokens, so we stop retrying then.
        retries = 0
        while True:
            reasoning_parts, answer_parts = [], []
            all_calls = []
            tool_buf = ToolStreamBuffer() if has_tools else None
            fail_reason = None

            try:
                await wk.send_message(prompt, thinking=thinking,
                                      search=search, model_type=model_type,
                                      images=images)
            except Exception as e:
                # if await wk.is_captcha() and CAPTCHA_BYPASS:
                #     fail_reason = "captcha appeared"
                # else:
                fail_reason = str(e)

            if fail_reason is None:
                async for phase, delta in wk.stream_tokens():
                    if phase == "error":
                        fail_reason = delta
                        break
                    if phase == "thinking":
                        reasoning_parts.append(delta)
                    elif phase != "error":
                        if tool_buf is not None:
                            visible, calls_batch = tool_buf.feed(delta)
                            if calls_batch:
                                all_calls.extend(calls_batch)
                            answer_parts.append(visible)
                        else:
                            answer_parts.append(delta)
                if tool_buf is not None:
                    leftover, tail_calls = tool_buf.flush()
                    if tail_calls:
                        all_calls.extend(tail_calls)
                    if leftover:
                        answer_parts.append(leftover)

            if fail_reason:
                if "upstream 413" in fail_reason or "Request Entity Too Large" in fail_reason:
                    # payload over the site limit -> never retry (same result)
                    log(f"[request] {fail_reason}", level="ERROR")
                    return JSONResponse({"error": {"message": fail_reason, "type": "proxy_error"}},
                                        status_code=502)
                if reasoning_parts or answer_parts:
                    # output already produced -> a retry would duplicate tokens
                    return JSONResponse({"error": {"message": fail_reason, "type": "proxy_error"}},
                                        status_code=502)
                # if fail_reason == "captcha appeared":
                #     log(f"[request] {fail_reason} -> retry", level="WARN")
                #     try:
                #         await wk.reload_current()
                #     except Exception as e:
                #         fail_reason = str(e)
                #     else:
                #         continue
                retries += 1
                if retries >= MAX_REQUEST_RETRIES:
                    log(f"[request] giving up after {retries} retries: {fail_reason}", level="ERROR")
                    return JSONResponse({"error": {"message": fail_reason}}, status_code=502)
                log(f"[request] {fail_reason} -> retry {retries}/{MAX_REQUEST_RETRIES}", level="WARN")
                try:
                    await wk.reload_current()
                except Exception as e:
                    fail_reason = str(e)
                continue

            break
    finally:
        pool.release(wk)

    content = "".join(answer_parts)
    message = {"role": "assistant", "content": content,
               "reasoning_content": "".join(reasoning_parts)}
    finish = "stop"
    log(f"--> done: reasoning={sum(len(x) for x in reasoning_parts)}ch "
        f"answer={sum(len(x) for x in answer_parts)}ch "
        f"prompt_len={prompt_len} | model={req_model}", level="OK")
    if has_tools and all_calls:
        message["tool_calls"] = [{
            "id": "call_" + secrets.token_hex(8),
            "type": "function",
            "function": {"name": tc["name"], "arguments": tc["arguments"]},
        } for tc in all_calls]
        message["content"] = None
        finish = "tool_calls"
    return {
        "id": chunk_id,
        "object": "chat.completion",
        "created": created,
        "model": req_model,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": finish,
        }],
        "usage": build_usage(messages, reasoning_parts, answer_parts),
    }


async def main():
    pool.start_hider()   # keep worker windows hidden (Windows only, HEADLESS on)
    await run_menu()
    # No global browser here: the pool spawns one browser per active
    # request on demand (see WorkerPool.acquire).
    log(f"Starting OpenAI-compatible server on http://{HOST}:{PORT}/v1")
    config = uvicorn.Config(app, host=HOST, port=PORT, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()


# ===== STARTUP MENU =====

LOGO = r"""███████╗██████╗ ███████╗███████╗    ██████╗ ███████╗███████╗██████╗ ███████╗███████╗███████╗██╗  ██╗      █████╗ ██████╗ ██╗
██╔════╝██╔══██╗██╔════╝██╔════╝    ██╔══██╗██╔════╝██╔════╝██╔══██╗██╔════╝██╔════╝██╔════╝██║ ██╔╝     ██╔══██╗██╔══██╗██║
█████╗  ██████╔╝█████╗  █████╗█████╗██║  ██║█████╗  █████╗  ██████╔╝███████╗█████╗  █████╗  █████╔╝█████╗███████║██████╔╝██║
██╔══╝  ██╔══██╗██╔══╝  ██╔══╝╚════╝██║  ██║██╔══╝  ██╔══╝  ██╔═══╝ ╚════██║██╔══╝  ██╔══╝  ██╔═██╗╚════╝██╔══██║██╔═══╝ ██║
██║     ██║  ██║███████╗███████╗    ██████╔╝███████╗███████╗██║     ███████║███████╗███████╗██║  ██╗     ██║  ██║██║     ██║
╚═╝     ╚═╝  ╚═╝╚══════╝╚══════╝    ╚═════╝ ╚══════╝╚══════╝╚═╝     ╚══════╝╚══════╝╚══════╝╚═╝  ╚═╝     ╚═╝  ╚═╝╚═╝     ╚═╝"""




RESET = "\x1b[0m"


def _enable_ansi():
    """Enable ANSI color codes on Windows console (no-op elsewhere)."""
    if os.name == "nt":
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            h = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
            mode = ctypes.c_uint32()
            kernel32.GetConsoleMode(h, ctypes.byref(mode))
            kernel32.SetConsoleMode(h, mode.value | 0x0004)  # ENABLE_VIRTUAL_TERMINAL_PROCESSING
        except Exception:
            pass


def _clear():
    os.system("cls" if os.name == "nt" else "clear")


def _tick(on):
    if on:
        return "\x1b[32mON\x1b[0m"   # green
    return "\x1b[31mOFF\x1b[0m"      # red


def _visible_len(s):
    """Length of a string ignoring ANSI escape codes (for alignment)."""
    return len(re.sub(r"\x1b\[[0-9;]*m", "", s))


def _term_width():
    try:
        return os.get_terminal_size().columns
    except Exception:
        return 80


def _logo_width():
    return max(_visible_len(l) for l in LOGO.splitlines())


# Remember what we already asked for, so a terminal that ignores the request
# does not get the sequence re-sent on every redraw.
_LAST_AUTO_WIDTH = 0


def _ensure_terminal_width(min_w=None):
    """Grow the terminal to fit the banner, via XTWINOPS.

    CSI 8 ; rows ; cols t resizes the text area in character cells. It is
    honoured by xterm and every emulator that follows it (kitty, alacritty,
    wezterm, gnome-terminal, konsole, foot, iTerm2). One that does not simply
    ignores the bytes, so this is safe to send without probing first.
    """
    global _LAST_AUTO_WIDTH
    if min_w is None:
        min_w = _logo_width()
    cur = _term_width()
    if cur <= 0 or cur >= min_w:
        _LAST_AUTO_WIDTH = 0
        return cur
    if _LAST_AUTO_WIDTH == min_w:
        return cur
    _LAST_AUTO_WIDTH = min_w
    try:
        import shutil
        rows = max(shutil.get_terminal_size().lines, 25)
        sys.stdout.write("[8;%d;%dt" % (rows, min_w))
        sys.stdout.flush()
    except Exception:
        return cur
    time.sleep(0.2)  # let the emulator apply it before we measure again
    grown = _term_width()
    if grown < min_w:
        # Keep _LAST_AUTO_WIDTH set so we do not spam a terminal that will
        # never grow. It resets to 0 as soon as the width is enough, so a
        # manual resize re-arms the check.
        log(f"[menu] terminal is {grown} cols, banner needs {min_w} "
            f"- it ignored the resize request, banner will wrap", level="WARN")
    return grown


def _center(s, width=None):
    """Center a plain (non-ANSI) string on a terminal line."""
    if width is None:
        width = _term_width()
    left = max(0, (width - len(s)) // 2)
    return " " * left + s


def _ansi_color(t):
    """45° gradient: purple (top-left) -> cyan (bottom-right) via linear lerp."""
    r1, g1, b1 = 147, 112, 219  # purple
    r2, g2, b2 = 0, 200, 255     # cyan
    r = int(r1 + (r2 - r1) * t)
    g = int(g1 + (g2 - g1) * t)
    b = int(b1 + (b2 - b1) * t)
    return f"\x1b[38;2;{r};{g};{b}m"


def _gradient_lines(lines, width):
    h = len(lines)
    pad_w = max(len(l) for l in lines)
    left = max(0, (width - pad_w) // 2)
    out = []
    for y, line in enumerate(lines):
        painted = " " * left
        for x, ch in enumerate(line):
            t = (x + y) / max(1, (pad_w - 1) + (h - 1))
            painted += _ansi_color(t) + ch
        out.append(painted + RESET)
    return out


def _render_menu():
    _enable_ansi()
    _clear()
    w = _ensure_terminal_width()
    for line in _gradient_lines(LOGO.splitlines(), w):
        print(line)
    print()
    table = [
        ["[1] Start", f"[4] API Port: {PORT}", "[7] GitHub"],
        [f"[2] {_tick(AUTO_REFRESH)} Auto Refresh Tokens", "[5] Open accounts.json", "[8] Exit"],
        [f"[3] {_tick(ACCOUNT_ROTATE)} Account Rotate", f"[6] {_tick(HEADLESS)} Hide Window", ""],
    ]
    # is then separated by exactly COL_GAP spaces, so all rows align perfectly.
    COL_GAP = 3
    ncols = max(len(r) for r in table)
    col_w = [max(_visible_len(r[i]) if i < len(r) else 0 for r in table) for i in range(ncols)]
    rows = []
    for r in table:
        line = ""
        for i in range(ncols):
            cell = r[i] if i < len(r) else ""
            line += cell + " " * (col_w[i] - _visible_len(cell))
            if i < ncols - 1:
                line += " " * COL_GAP
        rows.append(line)
    # Center the whole block as one unit so EVERY row shares the SAME left
    # offset (otherwise each row centers itself and the columns drift apart).
    block_w = max(_visible_len(r) for r in rows)
    left = max(0, (w - block_w) // 2)
    for r in rows:
        print(" " * left + r)
    print()


def open_accounts_file():
    """Open accounts.json in the default editor/app (works on win/mac/linux)."""
    path = os.path.abspath(ACCOUNTS_FILE)
    try:
        if os.name == "nt":
            os.startfile(path)
        elif sys.platform == "darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])
        log(f"[menu] opened {path}")
    except Exception as e:
        log(f"[menu] could not open accounts.json: {e}", level="ERROR")


GITHUB_URL = "https://github.com/lothiann/Free-ZAI-Api"  # inherited from the z.ai fork


def open_github():
    """Open the GitHub repo in the default browser (works on win/mac/linux)."""
    try:
        webbrowser.open(GITHUB_URL)
        log(f"[menu] opened {GITHUB_URL}")
    except Exception as e:
        log(f"[menu] could not open GitHub: {e}", level="ERROR")


async def run_menu():
    """Interactive startup menu. Returns when the user picks [1] Start."""
    global PORT, HEADLESS, ACCOUNT_ROTATE, AUTO_REFRESH
    while True:
        _render_menu()
        try:
            choice = input(" Choice: ").strip()
        except (EOFError, KeyboardInterrupt):
            # Ctrl+C at the main prompt exits the program
            _clear()
            log("[menu] exited (Ctrl+C)")
            raise SystemExit(0)
        if choice == "1":
            _clear()
            return
        elif choice == "2":
            AUTO_REFRESH = not AUTO_REFRESH
        elif choice == "3":
            ACCOUNT_ROTATE = not ACCOUNT_ROTATE
        elif choice == "4":
            try:
                new_port = input("\n API Port (ESC to cancel): ").strip()
            except (EOFError, KeyboardInterrupt):
                new_port = ""
            # ESC (\x1b) or 'esc' or empty cancels back to the menu
            if new_port in ("", "esc", "\x1b") or "\x1b" in new_port:
                continue
            try:
                PORT = int(new_port)
            except ValueError:
                log(f"[menu] invalid port: {new_port!r}", level="ERROR")
        elif choice == "5":
            open_accounts_file()
            try:
                input("\n Press Enter to continue...")
            except (EOFError, KeyboardInterrupt):
                pass
        elif choice == "6":
            HEADLESS = not HEADLESS
        elif choice == "7":
            open_github()
            try:
                input("\n Press Enter to continue...")
            except (EOFError, KeyboardInterrupt):
                pass
        elif choice == "8":
            _clear()
            log("[menu] exited")
            raise SystemExit(0)


if __name__ == "__main__":
    asyncio.run(main())

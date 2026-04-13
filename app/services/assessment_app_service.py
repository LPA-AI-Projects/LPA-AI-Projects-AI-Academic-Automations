from __future__ import annotations

import asyncio
import json
import os
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import quote

from app.core.config import settings

def _safe_js_string(s: str) -> str:
    return (s or "").replace("\\", "\\\\").replace("`", "\\`")


def flatten_validated_slides_to_text(cache_data: dict[str, Any]) -> str:
    modules = cache_data.get("modules") if isinstance(cache_data, dict) else []
    if not isinstance(modules, list):
        return ""
    lines: list[str] = []
    for m in modules:
        if not isinstance(m, dict):
            continue
        mn = str(m.get("module_name") or "Module").strip()
        lines.append(f"## {mn}")
        slides = m.get("slides") if isinstance(m.get("slides"), list) else []
        for s in slides:
            if not isinstance(s, dict):
                continue
            title = str(s.get("title") or "").strip()
            if title:
                lines.append(f"Slide: {title}")
            bullets = s.get("bullets") if isinstance(s.get("bullets"), list) else []
            for b in bullets:
                bt = str(b).strip()
                if bt:
                    lines.append(f"- {bt}")
        lines.append("")
    return "\n".join(lines).strip()


def build_react_quiz_files(*, title: str, questions: list[dict[str, Any]], seconds_per_question: int = 60) -> dict[str, dict[str, str]]:
    data_json = json.dumps(questions, ensure_ascii=False, indent=2)
    safe_title = _safe_js_string(title or "Assessment Quiz")
    spq = max(10, int(seconds_per_question or 60))
    app_jsx = f"""import React, {{ useEffect, useMemo, useState }} from "react";

const questions = {data_json};
const secondsPerQuestion = {spq};

function normalizeIndex(v, max) {{
  const n = Number(v);
  if (Number.isNaN(n)) return -1;
  return Math.max(-1, Math.min(max, n));
}}

export default function App() {{
  const total = questions.length;
  const [current, setCurrent] = useState(0);
  const [answers, setAnswers] = useState({{}});
  /** Countdown for the current question only (resets when you change question). */
  const [remaining, setRemaining] = useState(secondsPerQuestion);
  const [submitted, setSubmitted] = useState(false);

  useEffect(() => {{
    if (submitted) return;
    setRemaining(secondsPerQuestion);
  }}, [current, submitted]);

  useEffect(() => {{
    if (submitted) return;
    const id = setInterval(() => {{
      setRemaining((x) => {{
        if (x <= 1) {{
          setCurrent((c) => {{
            if (c >= total - 1) {{
              setSubmitted(true);
              return c;
            }}
            return c + 1;
          }});
          return secondsPerQuestion;
        }}
        return x - 1;
      }});
    }}, 1000);
    return () => clearInterval(id);
  }}, [submitted, total]);

  const progress = useMemo(() => {{
    const answered = Object.keys(answers).length;
    return total ? Math.round((answered / total) * 100) : 0;
  }}, [answers, total]);

  const qTimePct = useMemo(() => {{
    if (!secondsPerQuestion) return 0;
    return Math.round((remaining / secondsPerQuestion) * 100);
  }}, [remaining]);

  const score = useMemo(() => {{
    let s = 0;
    for (let i = 0; i < total; i++) {{
      if (normalizeIndex(answers[i], 3) === normalizeIndex(questions[i]?.correct_index, 3)) s++;
    }}
    return s;
  }}, [answers, total]);

  if (!total) {{
    return <div className="wrap"><h1>{safe_title}</h1><p>No questions available.</p></div>;
  }}

  const q = questions[current];

  if (submitted) {{
    return (
      <div className="wrap">
        <h1>{safe_title}</h1>
        <div className="card">
          <h2>Result</h2>
          <p>Score: <b>{{score}}</b> / {{total}}</p>
          <p>Accuracy: <b>{{Math.round((score / total) * 100)}}%</b></p>
          <button onClick={{() => window.location.reload()}}>Retake</button>
        </div>
      </div>
    );
  }}

  return (
    <div className="wrap">
      <h1>{safe_title}</h1>
      <div className="meta">
        <span className="pill">Question {{current + 1}} / {{total}}</span>
        <span className="pill">Answered {{Object.keys(answers).length}} / {{total}}</span>
        <span className={{`pill time ${{remaining <= 10 ? "warn" : ""}} ${{remaining <= 5 ? "critical" : ""}}`}}>
          This question: {{Math.floor(remaining / 60)}}:{{String(remaining % 60).padStart(2, "0")}}
        </span>
      </div>
      <div className="bar barQ" title="Time left for this question">
        <div className="barIn barQIn" style={{{{ width: `${{qTimePct}}%` }}}} />
      </div>
      <div className="bar"><div className="barIn" style={{{{ width: `${{progress}}%` }}}} /></div>
      <div className="card questionCard" key={{current}}>
        <h3>{{q.question}}</h3>
        <div className="opts">
          {{(q.options || []).map((opt, idx) => (
            <label key={{idx}} className={{`opt ${{answers[current] === idx ? "sel" : ""}}`}}>
              <input
                type="radio"
                name={{`q_${{current}}`}}
                checked={{answers[current] === idx}}
                onChange={{() => setAnswers((a) => ({{ ...a, [current]: idx }}))}}
              />
              <span>{{opt}}</span>
            </label>
          ))}}
        </div>
      </div>
      <div className="nav">
        <button disabled={{current <= 0}} onClick={{() => setCurrent((c) => Math.max(0, c - 1))}}>Prev</button>
        <button disabled={{current >= total - 1}} onClick={{() => setCurrent((c) => Math.min(total - 1, c + 1))}}>Next</button>
        <button onClick={{() => setSubmitted(true)}}>Submit</button>
      </div>
    </div>
  );
}}
"""
    css = """
*{box-sizing:border-box} body{margin:0;font-family:Inter,Arial,sans-serif;background:#0b1020;color:#e7ebff}
.wrap{max-width:900px;margin:30px auto;padding:16px}
h1{font-size:26px;margin:0 0 14px}
.meta{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:10px}
.pill{font-size:13px;padding:6px 12px;border-radius:999px;background:#1a2244;border:1px solid #2a355f}
.pill.time{font-variant-numeric:tabular-nums;font-weight:600}
.pill.warn{border-color:#c9a227;color:#f5e6a6}
.pill.critical{border-color:#e05555;color:#ffb4b4;animation:pulse 1s ease-in-out infinite}
@keyframes pulse{50%{opacity:.85}}
@keyframes fadeIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
.bar{height:8px;background:#1e2747;border-radius:8px;overflow:hidden;margin-bottom:8px}
.barQ{margin-bottom:14px}
.barIn{height:100%;background:#6ea8fe;transition:width .25s ease}
.barQIn{background:linear-gradient(90deg,#4fd1c5,#6ea8fe)}
.card{background:#121a34;border:1px solid #2a355f;border-radius:14px;padding:16px}
.questionCard{animation:fadeIn .28s ease-out}
.opts{display:grid;gap:10px;margin-top:12px}
.opt{display:flex;gap:10px;align-items:flex-start;padding:10px;border:1px solid #33406f;border-radius:10px;transition:border-color .15s,background .15s}
.opt:hover{border-color:#4a5a8f}
.opt.sel{border-color:#6ea8fe;background:#111f47}
.nav{display:flex;gap:10px;margin-top:14px}
button{background:#3e63dd;color:white;border:0;border-radius:10px;padding:10px 14px;cursor:pointer}
button:disabled{opacity:.5;cursor:not-allowed}
"""
    return {
        "package.json": {
            "content": json.dumps(
                {
                    "name": "assessment-quiz-app",
                    "private": True,
                    "version": "1.0.0",
                    "type": "module",
                    "scripts": {"dev": "vite", "build": "vite build", "preview": "vite preview"},
                    "dependencies": {"react": "^18.2.0", "react-dom": "^18.2.0"},
                    "devDependencies": {
                        "vite": "^5.4.0",
                        "@vitejs/plugin-react": "^4.3.1",
                    },
                },
                indent=2,
            )
        },
        "vite.config.js": {
            "content": """import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
export default defineConfig({
  plugins: [react()],
  // CodeSandbox preview uses *.csb.app; Vite blocks unknown hosts by default.
  server: { allowedHosts: true },
});
"""
        },
        "index.html": {"content": "<!doctype html><html><body><div id='root'></div><script type='module' src='/src/main.jsx'></script></body></html>"},
        "src/main.jsx": {"content": 'import React from "react"; import { createRoot } from "react-dom/client"; import App from "./App"; import "./styles.css"; createRoot(document.getElementById("root")).render(<App />);'},
        "src/App.jsx": {"content": app_jsx},
        "src/styles.css": {"content": css},
    }


def _deploy_codesandbox_sdk(files: dict[str, dict[str, str]]) -> tuple[str, str, str | None]:
    """
    Run scripts/codesandbox_sdk/deploy.mjs (@codesandbox/sdk) to create a VM sandbox (Devbox),
    install deps, start Vite, and return signed preview/editor URLs.
    """
    token = (settings.CODESANDBOX_API_TOKEN or "").strip()
    if not token:
        raise RuntimeError(
            "CODESANDBOX_API_TOKEN is required. Create a token at https://codesandbox.io/t/api — "
            "the legacy browser Define API (SSE) is discontinued; we use the CodeSandbox SDK (Devboxes)."
        )

    root = Path(__file__).resolve().parents[2]
    script = root / "scripts" / "codesandbox_sdk" / "deploy.mjs"
    if not script.is_file():
        raise RuntimeError(f"Missing CodeSandbox deploy script: {script}")

    payload = {"files": {path: meta.get("content", "") for path, meta in files.items()}}
    env = os.environ.copy()
    env["CSB_API_KEY"] = token
    env["CODESANDBOX_API_TOKEN"] = token

    proc = subprocess.run(
        ["node", str(script)],
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        cwd=str(script.parent),
        env=env,
        timeout=600,
    )
    out = (proc.stdout or "").strip()
    err = (proc.stderr or "").strip()
    if proc.returncode != 0:
        raise RuntimeError(
            f"CodeSandbox SDK deploy failed (exit {proc.returncode}): {err or out or 'no output'}"
        )

    last: dict[str, Any] | None = None
    for line in out.splitlines():
        line = line.strip()
        if line.startswith("{"):
            last = json.loads(line)
    if not last or not last.get("ok"):
        raise RuntimeError(f"Unexpected CodeSandbox SDK stdout: {out[:2000]}")

    sid = str(last.get("sandbox_id") or "")
    preview = str(last.get("preview_url") or "")
    editor = last.get("editor_url")
    if not sid or not preview:
        raise RuntimeError(f"SDK response missing sandbox_id or preview_url: {last!r}")
    ed = str(editor) if isinstance(editor, str) and editor.strip() else None
    return sid, preview, ed


async def create_codesandbox_from_files(files: dict[str, dict[str, str]]) -> tuple[str, str, str | None]:
    """Returns (sandbox_id, preview_url, editor_url_or_none)."""
    return await asyncio.to_thread(_deploy_codesandbox_sdk, files)


# Lovable "Build with URL" has practical browser URL length limits; keep embedded prompt smaller.
_LOVABLE_URL_PROMPT_MAX_CHARS = 1800


def build_lovable_assessment_prompt(
    *,
    course_name: str,
    questions: list[dict[str, Any]],
    seconds_per_question: int,
) -> str:
    """
    Full specification + question JSON for Lovable to generate the interactive quiz UI.
    User can paste this in Lovable if the URL-truncated bootstrap is used.
    """
    data = json.dumps(questions, ensure_ascii=False, indent=2)
    title = (course_name or "Assessment Quiz").strip() or "Assessment Quiz"
    spq = max(10, int(seconds_per_question or 60))
    return f"""Build an interactive corporate-training assessment web app.

## App title
{title}

## Stack
React + Vite (TypeScript preferred), polished accessible UI, responsive.

## UX / behavior
- One main question visible at a time (step through with Prev / Next).
- **Per-question timer**: {spq} seconds per question; **reset** the countdown when the user moves to another question (Prev/Next).
- When the timer hits 0: auto-advance to the next question; on the **last** question, auto-finish and show results.
- Display: question index (e.g. Question 3 / N), mm:ss countdown, optional progress bar for time and for completion.
- MCQ: exactly four options per question, radio selection; show final score and accuracy at the end with Retake.
- Keyboard-friendly; clear focus states; avoid clutter.

## Data (source of truth — implement against this JSON exactly)
Use this structure in code (import JSON or embed). Field `correct_index` is 0–3 for scoring.

```json
{data}
```

Do not replace or invent different questions; implement this set exactly."""


def lovable_prompt_for_build_url(full_prompt: str, *, course_name: str, seconds_per_question: int) -> tuple[str, bool]:
    """
    Return (prompt_for_url, truncated). If full prompt is too long for safe URL use, use a short
    bootstrap and rely on `lovable_prompt` in the API body for paste-into-Lovable.
    """
    if len(full_prompt) <= _LOVABLE_URL_PROMPT_MAX_CHARS:
        return full_prompt, False
    boot = (
        f"Build a React+Vite timed MCQ quiz for “{(course_name or 'Course').strip()[:120]}”. "
        f"{seconds_per_question}s per question, reset each question; auto-next on timeout; "
        "show score at end. IMPORTANT: Copy the full specification from the API response field "
        "`lovable_prompt` (from the same request) and paste it into this chat — it includes the exact question JSON."
    )
    if len(boot) > _LOVABLE_URL_PROMPT_MAX_CHARS:
        boot = boot[: _LOVABLE_URL_PROMPT_MAX_CHARS]
    return boot, True


def create_lovable_build_url(*, prompt: str, images: list[str] | None = None, autosubmit: bool = True) -> str:
    """
    Build a Lovable "Build with URL" link.
    Docs:
    - https://docs.lovable.dev/integrations/lovable-api
    - https://docs.lovable.dev/integrations/build-with-url
    """
    base = "https://lovable.dev/"
    auto = "true" if autosubmit else "false"
    parts: list[str] = [f"prompt={quote(prompt or '', safe='')}"]
    for img in images or []:
        u = str(img or "").strip()
        if not u:
            continue
        parts.append(f"images={quote(u, safe=':/?&=%')}")
    return f"{base}?autosubmit={auto}#" + "&".join(parts)

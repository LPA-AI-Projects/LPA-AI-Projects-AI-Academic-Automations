from __future__ import annotations

import asyncio
import json
import os
import random
import re
import shutil
import subprocess
import tempfile
import textwrap
import uuid
import zipfile
from dataclasses import dataclass
from typing import Any

import httpx
from sqlalchemy import select

from app.services.assessment_service import (
    MAX_CURRICULUM_CHARS,
    _parse_questions_json,
    _truncate,
    build_system_prompt,
    build_user_prompt,
    normalize_difficulty,
    post_difficulty_from_pre,
)
from app.services.claude import ClaudeService
from app.services.document_extractor import extract_pdf_text_async
from app.utils.logger import get_logger
from app.models.course import Course, CourseVersion
from app.models.job import CourseJob

logger = get_logger(__name__)


REACT_NODE_MAX_FIX_LOOPS = 2

# LMS-style themes (CSS variables only — same React deps every time).
_UI_THEMES: dict[str, str] = {
    "ocean": "--bg1:#0a1628;--bg2:#1e3a5f;--surface:rgba(255,255,255,0.07);--text:#e8f4ff;--muted:#94b8d9;--accent:#22d3ee;--accent2:#6366f1;--ok:#34d399;--bad:#f87171;--border:rgba(255,255,255,0.14);--radius:18px;--shadow:0 24px 80px rgba(0,0,0,0.35);",
    "sunset": "--bg1:#1a0a2e;--bg2:#4a1d4a;--surface:rgba(255,255,255,0.09);--text:#fff5f5;--muted:#e9c4ff;--accent:#fb923c;--accent2:#f472b6;--ok:#4ade80;--bad:#fb7185;--border:rgba(255,255,255,0.15);--radius:20px;--shadow:0 20px 60px rgba(0,0,0,0.4);",
    "forest": "--bg1:#052e16;--bg2:#14532d;--surface:rgba(255,255,255,0.08);--text:#ecfdf5;--muted:#a7f3d0;--accent:#34d399;--accent2:#22c55e;--ok:#4ade80;--bad:#f87171;--border:rgba(255,255,255,0.12);--radius:14px;--shadow:0 18px 50px rgba(0,0,0,0.35);",
    "corporate": "--bg1:#0f172a;--bg2:#1e293b;--surface:rgba(255,255,255,0.06);--text:#f8fafc;--muted:#94a3b8;--accent:#3b82f6;--accent2:#0ea5e9;--ok:#22c55e;--bad:#ef4444;--border:rgba(255,255,255,0.1);--radius:12px;--shadow:0 16px 48px rgba(0,0,0,0.3);",
    "violet": "--bg1:#1e1b4b;--bg2:#312e81;--surface:rgba(255,255,255,0.08);--text:#eef2ff;--muted:#c7d2fe;--accent:#a78bfa;--accent2:#818cf8;--ok:#34d399;--bad:#fb7185;--border:rgba(255,255,255,0.12);--radius:16px;--shadow:0 22px 70px rgba(0,0,0,0.38);",
    "midnight": "--bg1:#020617;--bg2:#0f172a;--surface:rgba(255,255,255,0.05);--text:#f1f5f9;--muted:#64748b;--accent:#38bdf8;--accent2:#94a3b8;--ok:#4ade80;--bad:#f87171;--border:rgba(255,255,255,0.08);--radius:10px;--shadow:0 12px 40px rgba(0,0,0,0.45);",
    "ember": "--bg1:#450a0a;--bg2:#7f1d1d;--surface:rgba(255,255,255,0.08);--text:#fef2f2;--muted:#fecaca;--accent:#f97316;--accent2:#fbbf24;--ok:#86efac;--bad:#fca5a5;--border:rgba(255,255,255,0.12);--radius:18px;--shadow:0 20px 55px rgba(0,0,0,0.4);",
    "slate": "--bg1:#1c1917;--bg2:#292524;--surface:rgba(255,255,255,0.06);--text:#fafaf9;--muted:#a8a29e;--accent:#14b8a6;--accent2:#2dd4bf;--ok:#4ade80;--bad:#f87171;--border:rgba(255,255,255,0.1);--radius:14px;--shadow:0 18px 45px rgba(0,0,0,0.35);",
}

_UI_LAYOUTS = ("card", "stepper", "fullscreen", "lms")


def _pick_ui_pair() -> tuple[tuple[str, str], tuple[str, str]]:
    """Random theme+layout for pre and post; prefer different from each other."""
    themes = list(_UI_THEMES.keys())
    t_pre = random.choice(themes)
    t_post = random.choice([x for x in themes if x != t_pre] or themes)
    l_pre = random.choice(list(_UI_LAYOUTS))
    l_post = random.choice([x for x in _UI_LAYOUTS if x != l_pre] or list(_UI_LAYOUTS))
    return (t_pre, l_pre), (t_post, l_post)


def _layout_extra_css(layout: str) -> str:
    if layout == "fullscreen":
        return """
        .app-shell { min-height: 100vh; padding: 28px 20px 48px; }
        .app-card { border-radius: 22px; padding: 28px; }
        .q { font-size: 20px; }
        """
    if layout == "stepper":
        return """
        .stepper-dots { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 8px; }
        .step-dot { width: 10px; height: 10px; border-radius: 999px; background: var(--border); }
        .step-dot.on { background: var(--accent); box-shadow: 0 0 12px var(--accent); }
        """
    if layout == "lms":
        return """
        .lms-topbar { display:flex; align-items:center; justify-content:space-between; gap:12px; padding:12px 16px; border-radius: var(--radius); background: var(--surface); border:1px solid var(--border); margin-bottom:18px; }
        .lms-badge { font-size:11px; letter-spacing:0.12em; text-transform:uppercase; color: var(--muted); }
        .app-card { border-left: 4px solid var(--accent); }
        """
    return """
    .app-card { backdrop-filter: blur(12px); }
    """


@dataclass
class ReactAppBuildResult:
    ok: bool
    command: str
    stdout: str
    stderr: str
    returncode: int


def _strip_json_fence(raw: str) -> str:
    text = (raw or "").strip()
    if text.startswith("```"):
        lines = text.split("\n")
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)
    return text.strip()


def _safe_filename(name: str) -> str:
    s = re.sub(r"[^\w\- ]+", "", (name or "").strip())
    s = re.sub(r"\s+", "_", s).strip("_")
    return s[:80] or "assessment_app"


async def _generate_questions_from_outline(
    *,
    outline_text: str,
    course_name: str,
    pre_level: str,
    post_level: str | None,
    num_questions: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ai = ClaudeService()
    pre_level = normalize_difficulty(pre_level)
    post_level_norm = normalize_difficulty(post_level) if (post_level or "").strip() else post_difficulty_from_pre(pre_level)

    outline_excerpt = _truncate(outline_text, MAX_CURRICULUM_CHARS)

    pre_raw = await ai.generate_text_completion(
        system_prompt=build_system_prompt(phase="pre", difficulty=pre_level, num_questions=num_questions),
        user_prompt=build_user_prompt(
            phase="pre",
            difficulty=pre_level,
            course_name=course_name,
            curriculum_excerpt=outline_excerpt,
            num_questions=num_questions,
            pre_difficulty=None,
        ),
        timeout_s=300.0,
    )
    post_raw = await ai.generate_text_completion(
        system_prompt=build_system_prompt(phase="post", difficulty=post_level_norm, num_questions=num_questions),
        user_prompt=build_user_prompt(
            phase="post",
            difficulty=post_level_norm,
            course_name=course_name,
            curriculum_excerpt=outline_excerpt,
            num_questions=num_questions,
            pre_difficulty=pre_level,
        ),
        timeout_s=300.0,
    )

    pre = _parse_questions_json(pre_raw)[:num_questions]
    post = _parse_questions_json(post_raw)[:num_questions]
    return pre, post


def _collect_project_files(project_dir: str) -> dict[str, str]:
    files: dict[str, str] = {}
    for root, dirs, fnames in os.walk(project_dir):
        dirs[:] = [d for d in dirs if d not in {"node_modules", ".git", "dist"}]
        for fn in fnames:
            rel = os.path.relpath(os.path.join(root, fn), project_dir).replace("\\", "/")
            try:
                with open(os.path.join(project_dir, rel), "r", encoding="utf-8") as f:
                    files[rel] = f.read()
            except Exception:
                continue
    return files


def _collect_dist_files(project_dir: str) -> dict[str, str]:
    dist_dir = os.path.join(project_dir, "dist")
    files: dict[str, str] = {}
    if not os.path.isdir(dist_dir):
        return files
    for root, _, fnames in os.walk(dist_dir):
        for fn in fnames:
            rel = os.path.relpath(os.path.join(root, fn), dist_dir).replace("\\", "/")
            try:
                with open(os.path.join(dist_dir, rel), "r", encoding="utf-8") as f:
                    files[rel] = f.read()
            except Exception:
                # Dist assets are text for this Vite build; skip unreadable files safely.
                continue
    return files


async def _deploy_to_codesandbox(*, project_dir: str) -> dict[str, Any]:
    # Deploy built static output for reliable preview (avoids Vite runtime startup issues in nodebox).
    files = _collect_dist_files(project_dir)
    if files:
        files["sandbox.config.json"] = json.dumps(
            {
                "template": "static",
                "infiniteLoopProtection": True,
                "view": "browser",
            },
            indent=2,
        )
        files["README.md"] = (
            "# Static Preview Build\n\n"
            "This sandbox hosts the built preview output from the generated React assessment app.\n"
        )
    else:
        # Fallback to full project upload if dist is unavailable.
        files = _collect_project_files(project_dir)
    if not files:
        raise RuntimeError("No project files available to deploy.")

    # CodeSandbox define API expects files as map of {path: {content}}
    payload = {"files": {k: {"content": v} for k, v in files.items()}}
    url = "https://codesandbox.io/api/v1/sandboxes/define?json=1"
    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.post(url, json=payload)
    resp.raise_for_status()
    data = resp.json() if resp.content else {}
    sandbox_id = str((data or {}).get("sandbox_id") or (data or {}).get("id") or "").strip()
    if not sandbox_id:
        raise RuntimeError(f"CodeSandbox returned no sandbox_id. Response: {data}")
    return {
        "codesandbox_id": sandbox_id,
        "codesandbox_url": f"https://codesandbox.io/s/{sandbox_id}",
    }


def _compose_lms_styles(*, theme_id: str, layout: str) -> str:
    vars_block = _UI_THEMES.get(theme_id, _UI_THEMES["corporate"])
    layout_css = _layout_extra_css(layout)
    return textwrap.dedent(
        f"""\
        :root {{
          {vars_block}
        }}
        * {{ box-sizing: border-box; }}
        body {{
          margin: 0;
          min-height: 100vh;
          font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
          background: linear-gradient(155deg, var(--bg1), var(--bg2));
          color: var(--text);
        }}
        .app-root {{ min-height: 100vh; }}
        .app-shell {{ max-width: 960px; margin: 0 auto; padding: 24px 18px 56px; }}
        .app-header h1 {{ margin: 0 0 6px; font-size: clamp(22px, 4vw, 30px); font-weight: 800; letter-spacing: -0.02em; }}
        .app-header .sub {{ color: var(--muted); font-size: 14px; }}
        .app-card {{
          background: var(--surface);
          border: 1px solid var(--border);
          border-radius: var(--radius);
          padding: 22px;
          box-shadow: var(--shadow);
        }}
        .row {{ display: flex; gap: 12px; flex-wrap: wrap; align-items: center; }}
        .btn {{
          background: linear-gradient(135deg, var(--accent), var(--accent2));
          color: #fff;
          border: none;
          border-radius: 12px;
          padding: 11px 18px;
          cursor: pointer;
          font-weight: 700;
          transition: transform .12s ease, filter .12s ease;
        }}
        .btn:hover {{ filter: brightness(1.06); transform: translateY(-1px); }}
        .btn.secondary {{ background: rgba(255,255,255,0.12); color: var(--text); border: 1px solid var(--border); }}
        .btn:disabled {{ opacity: 0.45; cursor: not-allowed; transform: none; }}
        .pill {{
          display: inline-flex; gap: 8px; align-items: center;
          padding: 7px 12px; border-radius: 999px;
          background: rgba(255,255,255,0.08); border: 1px solid var(--border);
          font-size: 13px;
        }}
        .q {{ font-size: 18px; line-height: 1.45; margin: 10px 0 14px; font-weight: 600; }}
        .opt {{
          width: 100%; text-align: left; padding: 12px 14px; border-radius: 12px;
          border: 1px solid var(--border); background: rgba(255,255,255,0.05); color: var(--text);
          cursor: pointer; transition: background .15s ease, border-color .15s ease;
        }}
        .opt:hover {{ background: rgba(255,255,255,0.1); }}
        .opt.correct {{ border-color: var(--ok); background: rgba(52, 211, 153, 0.15); }}
        .opt.wrong {{ border-color: var(--bad); background: rgba(248, 113, 113, 0.12); }}
        .muted {{ color: var(--muted); }}
        .progress-track {{ width: 100%; height: 9px; border-radius: 999px; background: rgba(255,255,255,0.1); overflow: hidden; margin: 12px 0 16px; }}
        .progress-fill {{ height: 100%; background: linear-gradient(90deg, var(--accent), var(--accent2)); transition: width .25s ease; }}
        .feedback-note {{ margin: 12px 0; font-size: 14px; line-height: 1.5; }}
        {layout_css}
        """
    )


def _build_single_phase_project_files(
    *,
    course_name: str,
    package_suffix: str,
    questions: list[dict[str, Any]],
    phase_key: str,
    phase_title: str,
    theme_id: str,
    layout: str,
) -> dict[str, str]:
    """One deployable app: single assessment phase + randomized LMS UI."""
    safe_title = f"{(course_name or 'Course').strip()} — {phase_title}"
    pkg = _safe_filename(f"{course_name}_{package_suffix}").lower() or f"app_{package_suffix}"
    meta = {"theme": theme_id, "layout": layout, "phase": phase_key, "lms": True}
    questions_json = json.dumps({"questions": questions, "meta": meta}, ensure_ascii=False, indent=2)

    package_json = {
        "name": pkg,
        "private": True,
        "version": "0.0.0",
        "type": "module",
        "scripts": {"start": "vite", "dev": "vite", "build": "vite build", "preview": "vite preview"},
        "dependencies": {"react": "18.2.0", "react-dom": "18.2.0"},
        "devDependencies": {"@vitejs/plugin-react": "4.2.1", "vite": "5.4.10"},
    }

    styles = _compose_lms_styles(theme_id=theme_id, layout=layout)
    nq = len(questions)
    stepper_block = ""
    if layout == "stepper":
        parts = []
        for i in range(min(nq, 14)):
            cls = "step-dot on" if i == 0 else "step-dot"
            parts.append(f'<span key={i} className="{cls}" />')
        stepper_block = f'<div className="stepper-dots" aria-hidden="true">{"".join(parts)}</div>'

    lms_block = ""
    if layout == "lms":
        lms_block = """
        <div className="lms-topbar">
          <span className="lms-badge">Learner assessment</span>
          <span className="muted" style={{ fontSize: 13 }}>{phaseTitle}</span>
        </div>
        """

    app_jsx = textwrap.dedent(
        """\
        import React, { useMemo, useState } from 'react'
        import qData from './questions.json'
        import Quiz from './Quiz.jsx'
        import Result from './Result.jsx'

        export default function App() {
          const phaseKey = __PHASE_KEY__
          const phaseTitle = __PHASE_TITLE__
          const [started, setStarted] = useState(false)
          const [done, setDone] = useState(false)
          const [answers, setAnswers] = useState([])

          const questions = useMemo(() => {
            const q = qData?.questions || []
            return Array.isArray(q) ? q : []
          }, [])

          const score = useMemo(() => {
            let s = 0
            for (const a of answers) {
              if (a && a.selectedIndex === a.correctIndex) s += 1
            }
            return s
          }, [answers])

          const reset = () => {
            setStarted(false)
            setDone(false)
            setAnswers([])
          }

          return (
            <div className="app-root theme-__THEME__ layout-__LAYOUT__">
              <div className="app-shell">
                <header className="app-header">
                  <div className="row" style={{ justifyContent: 'space-between', alignItems: 'flex-start', gap: 16 }}>
                    <div>
                      <div className="pill"><span className="muted">LMS</span> · {phaseTitle}</div>
                      <h1>{questions.length} questions</h1>
                      <p className="sub">Instant feedback · navigate back anytime</p>
                    </div>
                  </div>
                  __STEPPER_BLOCK__
                </header>
                __LMS_BLOCK__
                <div style={{ height: 18 }} />
                <div className="app-card">
                  {!started ? (
                    <div>
                      <p className="muted">When you are ready, begin the knowledge check. Your score summary appears at the end.</p>
                      <button className="btn" onClick={() => setStarted(true)} disabled={questions.length === 0}>
                        Start assessment
                      </button>
                    </div>
                  ) : done ? (
                    <Result
                      phaseKey={phaseKey}
                      phaseTitle={phaseTitle}
                      score={score}
                      total={questions.length}
                      onRestart={() => reset()}
                    />
                  ) : (
                    <Quiz
                      phaseKey={phaseKey}
                      phaseTitle={phaseTitle}
                      questions={questions}
                      onFinish={(finalAnswers) => { setAnswers(finalAnswers); setDone(true) }}
                    />
                  )}
                </div>
              </div>
            </div>
          )
        }
        """
    ).replace("__PHASE_KEY__", json.dumps(phase_key)).replace("__PHASE_TITLE__", json.dumps(phase_title)).replace("__THEME__", theme_id).replace("__LAYOUT__", layout).replace("__STEPPER_BLOCK__", stepper_block).replace("__LMS_BLOCK__", lms_block)

    quiz_jsx = textwrap.dedent(
        """\
        import React, { useMemo, useState } from 'react'

        export default function Quiz({ phaseKey, phaseTitle, questions, onFinish }) {
          const qs = Array.isArray(questions) ? questions : []
          const [idx, setIdx] = useState(0)
          const [answers, setAnswers] = useState(() => Array.from({ length: qs.length }, () => null))

          const current = qs[idx]
          const total = qs.length

          const progressLabel = useMemo(() => `${idx + 1} / ${total}`, [idx, total])
          const progressPct = useMemo(() => (total ? ((idx + 1) / total) * 100 : 0), [idx, total])
          const currentAnswer = answers[idx]
          const selected = currentAnswer?.selectedIndex ?? null
          const showFeedback = selected !== null && selected !== undefined

          if (!current) {
            return (
              <div>
                <div className="pill"><strong>{phaseTitle}</strong></div>
                <p className="muted">No questions available.</p>
                <button className="btn" onClick={() => onFinish([])}>Finish</button>
              </div>
            )
          }

          const selectOpt = (i) => {
            const a = { id: current.id ?? (idx + 1), selectedIndex: i, correctIndex: current.correct_index ?? 0 }
            setAnswers(prev => {
              const next = [...prev]
              next[idx] = a
              return next
            })
          }

          const next = () => {
            if (idx + 1 >= total) {
              onFinish(answers.filter(Boolean))
            } else {
              setIdx(idx + 1)
            }
          }

          const prev = () => {
            if (idx > 0) setIdx(idx - 1)
          }

          return (
            <div>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
                <div className="pill"><strong>{phaseTitle}</strong> <span className="muted">·</span> {progressLabel}</div>
                <div className="muted" style={{ fontSize: 13 }}>Progress</div>
              </div>
              <div className="progress-track">
                <div className="progress-fill" style={{ width: `${progressPct}%` }} />
              </div>

              <div className="q">{current.question}</div>

              <div style={{ display: 'grid', gap: 10 }}>
                {(current.options || []).slice(0, 4).map((opt, i) => {
                  const isCorrect = i === (current.correct_index ?? 0)
                  const isWrong = showFeedback && selected === i && !isCorrect
                  const cls = 'opt' + (showFeedback && isCorrect ? ' correct' : '') + (isWrong ? ' wrong' : '')
                  return (
                    <button key={i} type="button" className={cls} onClick={() => selectOpt(i)}>
                      {String.fromCharCode(65 + i)}. {opt}
                    </button>
                  )
                })}
              </div>

              <div style={{ height: 14 }} />

              {showFeedback ? (
                <div className="feedback-note">
                  {selected === (current.correct_index ?? 0) ? 'Correct answer.' : `Incorrect. Correct option is ${String.fromCharCode(65 + (current.correct_index ?? 0))}.`}
                </div>
              ) : (
                <div className="feedback-note muted">Choose an option to see instant feedback.</div>
              )}

              <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
                <button type="button" className="btn secondary" onClick={prev} disabled={idx === 0}>Back</button>
                <button type="button" className="btn" onClick={next} disabled={selected === null || selected === undefined}>
                  {idx + 1 >= total ? 'Finish' : 'Next'}
                </button>
              </div>
            </div>
          )
        }
        """
    )

    result_jsx = textwrap.dedent(
        """\
        import React from 'react'

        export default function Result({ phaseKey, phaseTitle, score, total, onRestart }) {
          const pct = total ? Math.round((score / total) * 100) : 0
          return (
            <div>
              <div className="pill"><strong>{phaseTitle}</strong></div>
              <h2 style={{ margin: '10px 0 6px', fontSize: 26 }}>Results</h2>
              <p className="muted">Score: <strong>{score}</strong> / {total} ({pct}%)</p>
              <button type="button" className="btn" onClick={onRestart}>Restart</button>
            </div>
          )
        }
        """
    )

    return {
        "package.json": json.dumps(package_json, indent=2),
        "vite.config.js": textwrap.dedent(
            """\
            import { defineConfig } from 'vite'
            import react from '@vitejs/plugin-react'

            export default defineConfig({
              plugins: [react()],
            })
            """
        ),
        "index.html": textwrap.dedent(
            f"""\
            <!doctype html>
            <html lang="en">
              <head>
                <meta charset="UTF-8" />
                <meta name="viewport" content="width=device-width, initial-scale=1.0" />
                <title>{safe_title}</title>
              </head>
              <body>
                <div id="root"></div>
                <script type="module" src="/src/index.js"></script>
              </body>
            </html>
            """
        ),
        "src/index.js": "import './main.jsx'\n",
        "src/main.jsx": textwrap.dedent(
            """\
            import React from 'react'
            import ReactDOM from 'react-dom/client'
            import App from './App.jsx'
            import './styles.css'

            ReactDOM.createRoot(document.getElementById('root')).render(
              <React.StrictMode>
                <App />
              </React.StrictMode>,
            )
            """
        ),
        "src/styles.css": styles,
        "src/questions.json": questions_json,
        "src/App.jsx": app_jsx,
        "src/Quiz.jsx": quiz_jsx,
        "src/Result.jsx": result_jsx,
        "README.md": f"# {safe_title}\n\nGenerated LMS assessment (single phase). Run `npm install` then `npm run dev`.\n",
    }


def _write_project_files(project_dir: str, files: dict[str, str]) -> None:
    for rel_path, content in files.items():
        rel_path = rel_path.replace("\\", "/").lstrip("/")
        abs_path = os.path.join(project_dir, rel_path)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        with open(abs_path, "w", encoding="utf-8") as f:
            f.write(content)


def _run_cmd(cwd: str, args: list[str], timeout_s: int) -> ReactAppBuildResult:
    if args and args[0] == "npm":
        npm_path = shutil.which("npm.cmd") or shutil.which("npm") or "npm"
        args = [npm_path, *args[1:]]
    p = subprocess.run(
        args,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=timeout_s,
        shell=False,
    )
    return ReactAppBuildResult(
        ok=p.returncode == 0,
        command=" ".join(args),
        stdout=p.stdout[-8000:],
        stderr=p.stderr[-8000:],
        returncode=p.returncode,
    )


def _zip_dir(src_dir: str, zip_path: str) -> None:
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src_dir):
            for fn in files:
                abs_path = os.path.join(root, fn)
                rel = os.path.relpath(abs_path, src_dir).replace("\\", "/")
                z.write(abs_path, rel)


async def _ai_fix_project(
    *,
    project_dir: str,
    course_name: str,
    build_failures: list[ReactAppBuildResult],
) -> bool:
    """
    Ask the model to patch files as JSON:
    { "files": { "src/App.jsx": "<full file text>", ... } }
    """
    combined = "\n\n".join(
        [
            f"Command: {r.command}\nExit: {r.returncode}\nSTDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}"
            for r in build_failures
        ]
    )
    combined = combined[-12000:]

    # Read current file contents (small set) to help the fixer.
    interesting = ["package.json", "vite.config.js", "src/App.jsx", "src/Quiz.jsx", "src/Result.jsx", "src/main.jsx"]
    current_files: dict[str, str] = {}
    for rel in interesting:
        p = os.path.join(project_dir, rel)
        if os.path.exists(p):
            try:
                current_files[rel] = open(p, "r", encoding="utf-8").read()[:20000]
            except Exception:
                pass

    ai = ClaudeService()
    system_prompt = textwrap.dedent(
        """\
        You are a senior React+Vite engineer. Fix build/run issues.

        Rules:
        - Return ONLY valid JSON (no markdown fences).
        - Output shape: { "files": { "<path>": "<full updated file text>" } }
        - Only include files that need changes.
        - Keep dependencies minimal (React 18 + Vite).
        - Do NOT add additional libraries.
        - Ensure `npm install` and `npm run build` succeed.
        """
    )
    user_prompt = textwrap.dedent(
        f"""\
        Project: {course_name}

        Current key files (truncate ok):
        {json.dumps(current_files, ensure_ascii=False, indent=2)}

        Build errors:
        ---
        {combined}
        ---
        """
    )
    raw = await ai.generate_text_completion(system_prompt=system_prompt, user_prompt=user_prompt, timeout_s=300.0)
    data = json.loads(_strip_json_fence(raw))
    files = data.get("files") if isinstance(data, dict) else None
    if not isinstance(files, dict) or not files:
        return False
    for rel, content in files.items():
        if not isinstance(rel, str) or not isinstance(content, str):
            continue
        rel = rel.replace("\\", "/").lstrip("/")
        abs_path = os.path.join(project_dir, rel)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        with open(abs_path, "w", encoding="utf-8") as f:
            f.write(content)
    return True


async def _materialize_one_phase(
    *,
    files: dict[str, str],
    dest_dir: str,
    course_name: str,
    deploy_to_codesandbox: bool,
    max_fix_loops: int,
) -> dict[str, Any]:
    """npm install + build + optional fix loop; copy to dest_dir; zip; optional CodeSandbox."""
    with tempfile.TemporaryDirectory(prefix="assess_phase_") as tmp:
        project_dir = os.path.join(tmp, "app")
        os.makedirs(project_dir, exist_ok=True)
        _write_project_files(project_dir, files)

        install = await asyncio.to_thread(_run_cmd, project_dir, ["npm", "install"], 1200)
        build = await asyncio.to_thread(_run_cmd, project_dir, ["npm", "run", "build"], 1200)
        attempts: list[dict[str, Any]] = [{"install": install.__dict__, "build": build.__dict__}]

        failures = [r for r in (install, build) if not r.ok]
        loop = 0
        while failures and loop < max_fix_loops:
            loop += 1
            logger.warning(
                "Assessment phase build failed; AI fix | loop=%s install_ok=%s build_ok=%s",
                loop,
                install.ok,
                build.ok,
            )
            changed = await _ai_fix_project(
                project_dir=project_dir,
                course_name=course_name,
                build_failures=failures,
            )
            if not changed:
                break
            install = await asyncio.to_thread(_run_cmd, project_dir, ["npm", "install"], 1200)
            build = await asyncio.to_thread(_run_cmd, project_dir, ["npm", "run", "build"], 1200)
            attempts.append({"install": install.__dict__, "build": build.__dict__})
            failures = [r for r in (install, build) if not r.ok]

        if os.path.exists(dest_dir):
            shutil.rmtree(dest_dir, ignore_errors=True)
        shutil.copytree(project_dir, dest_dir)
        zip_path = f"{dest_dir}.zip"
        _zip_dir(dest_dir, zip_path)

        out: dict[str, Any] = {
            "project_dir": dest_dir,
            "zip_path": zip_path,
            "validation": {"ok": not failures, "attempts": attempts},
            "codesandbox_url": None,
            "codesandbox_id": None,
        }
        if deploy_to_codesandbox and not failures:
            try:
                cs = await _deploy_to_codesandbox(project_dir=dest_dir)
                out["codesandbox_url"] = cs.get("codesandbox_url")
                out["codesandbox_id"] = cs.get("codesandbox_id")
            except Exception as e:
                out["codesandbox_deploy_error"] = str(e)
        return out


async def build_assessment_react_app_local(
    *,
    course_name: str,
    outline_text: str,
    pre_level: str = "intermediate",
    post_level: str | None = None,
    num_questions: int = 15,
    deploy_to_codesandbox: bool = False,
    max_fix_loops: int = REACT_NODE_MAX_FIX_LOOPS,
) -> dict[str, Any]:
    """
    Generate pre/post questions, then build two separate React apps (random LMS UI each),
    validate each, optionally deploy each to CodeSandbox (pre URL + post URL).
    """
    course_name = (course_name or "").strip() or "Course"
    outline_text = (outline_text or "").strip()
    if not outline_text:
        raise ValueError("outline_text is required.")

    pre_q, post_q = await _generate_questions_from_outline(
        outline_text=outline_text,
        course_name=course_name,
        pre_level=pre_level,
        post_level=post_level,
        num_questions=num_questions,
    )

    (t_pre, l_pre), (t_post, l_post) = _pick_ui_pair()
    run_id = uuid.uuid4().hex[:12]
    out_root = os.path.join(os.getcwd(), "app", "storage", "assessment_apps")
    os.makedirs(out_root, exist_ok=True)
    safe = _safe_filename(course_name)
    parent = os.path.join(out_root, f"{safe}_{run_id}")

    pre_files = _build_single_phase_project_files(
        course_name=course_name,
        package_suffix="pre",
        questions=pre_q,
        phase_key="pre",
        phase_title="Pre-Assessment",
        theme_id=t_pre,
        layout=l_pre,
    )
    post_files = _build_single_phase_project_files(
        course_name=course_name,
        package_suffix="post",
        questions=post_q,
        phase_key="post",
        phase_title="Post-Assessment",
        theme_id=t_post,
        layout=l_post,
    )

    os.makedirs(parent, exist_ok=True)
    pre_dest = os.path.join(parent, "pre")
    post_dest = os.path.join(parent, "post")

    pre_res = await _materialize_one_phase(
        files=pre_files,
        dest_dir=pre_dest,
        course_name=f"{course_name} (pre)",
        deploy_to_codesandbox=deploy_to_codesandbox,
        max_fix_loops=max_fix_loops,
    )
    post_res = await _materialize_one_phase(
        files=post_files,
        dest_dir=post_dest,
        course_name=f"{course_name} (post)",
        deploy_to_codesandbox=deploy_to_codesandbox,
        max_fix_loops=max_fix_loops,
    )

    pl = normalize_difficulty(pre_level)
    po = normalize_difficulty(post_level) if (post_level or "").strip() else post_difficulty_from_pre(pre_level)

    return {
        "course_name": course_name,
        "pre_level": pl,
        "post_level": po,
        "num_questions": num_questions,
        "pre_questions": pre_q,
        "post_questions": post_q,
        "project_dir": parent,
        "project_dir_pre": pre_res["project_dir"],
        "project_dir_post": post_res["project_dir"],
        "zip_path_pre": pre_res["zip_path"],
        "zip_path_post": post_res["zip_path"],
        "zip_path": None,
        "validation_pre": pre_res["validation"],
        "validation_post": post_res["validation"],
        "validation": {
            "pre_ok": bool(pre_res["validation"].get("ok")),
            "post_ok": bool(post_res["validation"].get("ok")),
        },
        "ui_variant_pre": {"theme": t_pre, "layout": l_pre},
        "ui_variant_post": {"theme": t_post, "layout": l_post},
        "codesandbox_url_pre": pre_res.get("codesandbox_url"),
        "codesandbox_url_post": post_res.get("codesandbox_url"),
        "codesandbox_id_pre": pre_res.get("codesandbox_id"),
        "codesandbox_id_post": post_res.get("codesandbox_id"),
        "codesandbox_deploy_error_pre": pre_res.get("codesandbox_deploy_error"),
        "codesandbox_deploy_error_post": post_res.get("codesandbox_deploy_error"),
        "codesandbox_url": pre_res.get("codesandbox_url") or post_res.get("codesandbox_url"),
        "codesandbox_id": pre_res.get("codesandbox_id"),
        "commands": ["npm install", "npm run dev", "npm run build"],
    }


async def extract_outline_text_from_upload(*, file_bytes: bytes) -> str:
    text = await extract_pdf_text_async(file_bytes)
    return _truncate(text, MAX_CURRICULUM_CHARS)


async def resolve_outline_text_for_reuse(
    *,
    db,
    zoho_record_id: str,
) -> str | None:
    """
    Best-effort reuse of previously extracted/stored outline text for this Zoho record.

    Priority:
    1) Latest completed PRE assessment job payload: curriculum_text_excerpt
    2) Latest course outline version (CourseVersion.outline_text)
    """
    rid = (zoho_record_id or "").strip()
    if not rid:
        return None

    try:
        # 1) Reuse extracted PDF text from latest completed pre-assessment.
        res = await db.execute(
            select(CourseJob)
            .where(
                CourseJob.zoho_record_id == rid,
                CourseJob.job_type == "assessment",
                CourseJob.status == "completed",
            )
            .order_by(CourseJob.created_at.desc())
        )
        jobs = res.scalars().all()
        for j in jobs:
            try:
                payload = json.loads(j.payload_json or "{}")
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            phase = str(payload.get("phase") or payload.get("assessment_type") or "").lower()
            if phase != "pre":
                continue
            excerpt = str(payload.get("curriculum_text_excerpt") or "").strip()
            if excerpt:
                logger.info("Reusing curriculum excerpt from assessment job | zoho_record_id=%s", rid)
                return _truncate(excerpt, MAX_CURRICULUM_CHARS)
    except Exception:
        logger.exception("Failed to reuse outline from assessment job | zoho_record_id=%s", rid)

    try:
        # 2) Reuse generated course outline stored in course_versions.
        c = await db.execute(select(Course).where(Course.zoho_record_id == rid))
        course = c.scalars().first()
        if course is None:
            return None
        v = await db.execute(
            select(CourseVersion)
            .where(CourseVersion.course_id == course.id)
            .order_by(CourseVersion.version_number.desc())
        )
        ver = v.scalars().first()
        if ver is None:
            return None
        outline = str(ver.outline_text or "").strip()
        if outline:
            logger.info(
                "Reusing outline_text from latest course version | zoho_record_id=%s version=%s",
                rid,
                ver.version_number,
            )
            return _truncate(outline, MAX_CURRICULUM_CHARS)
    except Exception:
        logger.exception("Failed to reuse outline from course versions | zoho_record_id=%s", rid)

    return None


async def resolve_course_name_for_reuse(
    *,
    db,
    zoho_record_id: str,
) -> str | None:
    """Best-effort course name lookup for the given Zoho record."""
    rid = (zoho_record_id or "").strip()
    if not rid:
        return None

    try:
        res = await db.execute(
            select(CourseJob)
            .where(
                CourseJob.zoho_record_id == rid,
                CourseJob.job_type == "assessment",
            )
            .order_by(CourseJob.created_at.desc())
        )
        for j in res.scalars().all():
            try:
                payload = json.loads(j.payload_json or "{}")
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            name = str(payload.get("course_name") or "").strip()
            if name:
                return name
    except Exception:
        logger.exception("Failed course_name lookup from assessment jobs | zoho_record_id=%s", rid)

    try:
        c = await db.execute(select(Course).where(Course.zoho_record_id == rid))
        course = c.scalars().first()
        if course is None:
            return None
        v = await db.execute(
            select(CourseVersion)
            .where(CourseVersion.course_id == course.id)
            .order_by(CourseVersion.version_number.desc())
        )
        ver = v.scalars().first()
        if ver is None:
            return None
        outline = str(ver.outline_text or "")
        # Try JSON field first.
        try:
            data = json.loads(outline)
            if isinstance(data, dict):
                title = str(data.get("course_title") or "").strip()
                if title:
                    return title
        except Exception:
            pass
        # Fallback text pattern: "Course Title: ..."
        m = re.search(r"(?im)^\\s*course\\s*title\\s*:\\s*(.+)$", outline)
        if m and str(m.group(1)).strip():
            return str(m.group(1)).strip()
    except Exception:
        logger.exception("Failed course_name lookup from course versions | zoho_record_id=%s", rid)

    return None


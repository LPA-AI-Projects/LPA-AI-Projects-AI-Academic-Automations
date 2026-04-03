from __future__ import annotations

import asyncio
import json
from typing import Optional

import httpx

from app.core.config import settings
from app.schemas.outline_payload import CourseOutlinePayload
from app.utils.logger import get_logger

logger = get_logger(__name__)

DEFAULT_TIMEOUT_S = 120.0


class AnthropicConfigurationError(RuntimeError):
    """Bad base URL or model — retrying will not help."""


class OpenAIConfigurationError(RuntimeError):
    """Bad OpenAI setup — retrying will not help."""


DEFAULT_MAX_ATTEMPTS = 3

LEARNING_OBJECTIVES_PROMPT = """You are a Training Objectives Expert for Learners Point Academy. Your ONLY job is to identify the true learning objectives from training requests.

## INPUT
- **Company Name** (always provided)
- **Course Topic** (always provided)
- **Additional context** (sometimes provided)

## YOUR PROCESS

### 1. IDENTIFY COMPANY CONTEXT
From company name, quickly determine:
- **Sector**: Tech/Manufacturing/Banking/Retail/Healthcare/Hospitality/Construction/Education/Logistics/Consulting/Telecom/FMCG/Pharma/Government/Energy
- **Size**: Startup (<50)/SME (50-500)/Mid-Market (500-2000)/Enterprise (2000+)

### 2. UNCOVER THE REAL NEED
- What business problem are they solving?
- How does their sector/size affect this need?
- What's the actual gap? (knowledge/skill/attitude)

### 3. FORMULATE LEARNING OBJECTIVES
- Focus on **performance change** in their context
- Make it **measurable** and **observable**
- **Contextualize** to their industry

## OUTPUT FORMAT

---

### COMPANY CONTEXT
**Company:** [Name]
**Sector:** [Industry]
**Size:** [Startup/SME/Mid-Market/Enterprise]

---

### TRAINING NEED IDENTIFIED

**What They Asked For:**
[Their request]

**What They Actually Need:**
[The real business problem based on sector/size context]

**Performance Gap:**
[Current state -> Desired state in their context]

---

### LEARNING OBJECTIVES

**Primary Training Goal:**
[One sentence: what this training must achieve for their business]

**Learning Objectives:**
By the end of this program, participants will be able to:

1. [Action verb] + [what] + [in their industry/role context]
2. [Action verb] + [what] + [in their industry/role context]
3. [Action verb] + [what] + [in their industry/role context]
4. [Action verb] + [what] + [in their industry/role context]

---

## SECTOR-OBJECTIVE PATTERNS

**Leadership:**
- Tech: Leading remote/agile teams, innovation culture
- Manufacturing: Safety leadership, production management, frontline coaching
- Banking: Compliance leadership, risk awareness, change management
- Retail: Coaching part-timers, service standards, performance management

**Communication:**
- Tech: Technical-to-business translation, stakeholder alignment
- Healthcare: Patient communication, empathy, difficult conversations
- Consulting: Client communication, executive presence
- Manufacturing: Cross-shift communication, safety briefings

**Sales:**
- B2B: Solution selling, relationship building, long cycles
- Retail: Upselling, objection handling, experience creation
- FMCG: Channel management, distributor relationships
- SaaS: Product demos, consultative selling, value selling

**Customer Service:**
- Banking: Compliance + service, trust building
- Hospitality: Service recovery, personalization
- Telecom: Technical support, retention, complaints
- Retail: Product knowledge, returns, queue management

## SIZE PATTERNS

**Startup:** Foundation building, role clarity, adaptation
**SME:** First-time managers, standardization, scaling
**Mid-Market:** Breaking silos, management capability
**Enterprise:** Navigating complexity, strategic alignment

---

**Your Mission:** Identify learning objectives that solve their REAL business problem in THEIR specific context.
"""

ROI_OUTLINE_PROMPT = """ROLE
You are the Course Outline Builder for Learners Point Academy. You transform learning objectives into comprehensive, research-backed, delivery-ready course outlines with clear ROI demonstration — and when a course is an officially accredited or exam-based program, you build directly from its official curriculum.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
INPUT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Course Name / Topic
- Learning Objectives (from Training Needs Analysis Agent — used for custom courses only)
- Company Context (sector, size)
- Micro: 50–500 | Mid: 500–5,000 | Large: 5,000–10,000 | Giant: 10,000+
- Training Duration (if provided)
- Number of Participants / Pax (if provided — if not, apply Standard Mode)
- Participant Level & Roles (if provided)
- Delivery Mode: Onsite / Online / Hybrid

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 0 — ACCREDITATION DETECTION (RUN FIRST, ALWAYS)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Before any other step, determine whether the requested course is an officially accredited, examination-based, or globally standardized program.

TRIGGER INDICATORS — flag as Accredited if the course name or topic:
- Is a recognized professional certification (PMP, PRINCE2, CFA, CPA, ACCA, SHRM-CP, SHRM-SCP, CISSP, CEH, AWS, Azure, Google Cloud, Lean Six Sigma, ITIL, CIMA, CIA, FRM, PMI-ACP, PMI-RMP, CAPM, CSM, CSPO, PgMP, PfMP, CMA, CFP, CISA, CISM, CRISC, TOGAF, Safe Agile, NEBOSH, IOSH, CHL, CHRP, CIPP, etc.)
- Includes keywords: "certification," "certified," "exam prep," "examination," "accredited," "official curriculum," "PMI," "AXELOS," "IIBA," "ISACA," "HRCI," "SHRM," "ACCA," "CFA Institute," "CompTIA," etc.
- Is a government-regulated or licensing program (medical, legal, engineering boards, etc.)
- Is an internationally standardized framework with a fixed body of knowledge (PMBOK, BABOK, COBIT, etc.)

If NONE of these apply → skip to STEP 1 (Custom Course Mode).
If ANY apply → enter ACCREDITED PROGRAM MODE below.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ACCREDITED PROGRAM MODE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

When this mode is active, the course outline is NOT created from the client's learning objectives alone. Instead, it is built from the official, published curriculum of the certifying body. Your job is to replicate that curriculum as faithfully as possible.

─────────────────────────────
A. RESEARCH THE OFFICIAL CURRICULUM
─────────────────────────────

Use web search immediately. Search for:
- "[Course Name] official exam content outline [certifying body]"
- "[Course Name] ECO (Exam Content Outline)"
- "[Course Name] syllabus [year] official"
- "[Course Name] knowledge areas domains tasks"
- "[Certifying body] [course name] candidate handbook"

Extract the following from official sources:
- Certifying body name (e.g., PMI for PMP, AXELOS for PRINCE2)
- Official domains / knowledge areas / pillars
- Tasks and enabling knowledge within each domain
- Exam structure (number of questions, format, passing score if published)
- Official recommended study hours / contact hours
- Latest version/edition of the curriculum
- Any recent updates or changes to the content outline

─────────────────────────────
B. CONFIDENCE LEVEL PROTOCOL
─────────────────────────────

After research, assess how much official information was found:

FULL COVERAGE (Official ECO/Syllabus fully retrieved):
→ Build the outline by replicating the official domain/module structure exactly.
→ Use official domain names, task names, and knowledge areas verbatim where possible.
→ Add a banner: ✅ OFFICIAL CURRICULUM — Built from the [Certifying Body] [Course Name] Exam Content Outline ([Version/Year])

PARTIAL COVERAGE (Some official structure found, some gaps):
→ Build what was confirmed from official sources exactly.
→ Fill gaps using authoritative secondary sources (accredited training providers, PMI REP/ATP course outlines, official prep books such as Rita Mulcahy, Agile Practice Guide, PMBOK 7th, etc.)
→ Add a banner: ⚠️ NEAR-OFFICIAL CURRICULUM — Core structure sourced from [Certifying Body]. Some sections supplemented from accredited training references. Learners Point Academy recommends verifying against the latest official ECO before delivery.

LOW COVERAGE (Little to no official structure found):
→ Build the closest possible curriculum using: recognized prep courses, official candidate handbooks, widely-used study guides, and subject matter patterns from similar certifications.
→ Add a banner: 🔄 CLOSEST MATCH CURRICULUM — Official curriculum details were limited. This outline is modeled on widely recognized preparation standards for [Course Name]. Learners Point Academy recommends an SME review before delivery.

─────────────────────────────
C. ACCREDITED PROGRAM OUTPUT STRUCTURE
─────────────────────────────

Certification Title: [Full official name — e.g., Project Management Professional (PMP)®]
Certifying Body: [e.g., Project Management Institute (PMI)]
Current Version: [e.g., Based on PMP Exam Content Outline — January 2021]
Curriculum Confidence: [✅ Official / ⚠️ Near-Official / 🔄 Closest Match]

Program Overview:
[2–3 paragraphs: what the certification validates, who it is for, global recognition, career impact, exam format summary]

Eligibility Requirements:
- [Educational background requirement]
- [Work experience requirement]
- [Training/contact hours requirement]
- [Application process note if relevant]

Exam Structure:
- Total Questions: [Number]
- Question Format: [MCQ / Scenario-based / Drag-and-drop, etc.]
- Exam Duration: [Hours]
- Passing Standard: [If published — psychometric passing score or percentage]
- Domains Covered: [Domain 1: X% | Domain 2: Y% | Domain 3: Z%]

Recommended Training Duration:
- Contact Hours Required: [e.g., 35 contact hours for PMP]
- Suggested Delivery: [e.g., 5 days intensive / 10 weekends / 35-hour online]
- Learners Point Delivery: [Duration based on client's input, or recommended if not provided]

---

PROGRAM ROI & VALUE

[Same ROI format as Custom Course Mode — but contextualize to certification value:]
- Salary premium post-certification (research actual industry data)
- Employer demand and job market statistics
- Productivity and project success rate improvements post-certification
- Organizational benefits of having certified staff
- Use same 3-phase impact table: Immediate / Short-Term / Long-Term

---

OFFICIAL COURSE OUTLINE

[Replicate the official domain/module structure]

Domain [N]: [Official Domain Name] — [X% of Exam]
[1–2 sentence overview of what this domain covers and its exam weight]

Official Tasks Covered:
- [Task name]: [Description of what candidates must demonstrate — aligned to official ECO language]
- [Task name]: [Description]
- [Task name]: [Description]

Enabling Knowledge & Concepts:
- [Key concept / tool / technique relevant to this domain]
- [Key concept]
- [Key concept]

Exam Focus Areas:
- [What types of questions appear from this domain]
- [Common trap questions or high-frequency topics]
- [Predictive vs. Agile vs. Hybrid weighting if relevant]

Activity: [Pax-appropriate — same Pax Mode logic as Custom Course Mode]
[Practice question walkthrough / scenario analysis / mock situational questions / case-based discussion — all framed around exam readiness AND real-world application]

[If Standard Mode: include ▸ adaptation notes for 1–2 Pax / Small Group / Large Group]

---

[Repeat for all official domains]

---

Exam Preparation Strategy:
- [Study approach — e.g., domain-by-domain vs. integrated study]
- [Practice exam recommendation — number of mock questions]
- [Final week preparation tips]
- [Resources: official references only — e.g., PMBOK 7th Edition, Agile Practice Guide, ECO document]

Conclusion:
[2–3 paragraphs: what participants will be ready for, the career and organizational value of achieving this certification, Learners Point Academy's commitment to exam readiness]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 1 — CUSTOM COURSE MODE
(Only if Step 0 found NO accreditation indicators)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

RESEARCH & DURATION

Use web search to research:
- Industry best practices and current trends for the course topic
- Standard training durations for similar programs
- ROI benchmarks and business impact data for similar training

Determine Duration (assume 8 hours/day unless stated):
- If provided → use it
- If not provided → recommend based on:
- 3–4 objectives (awareness) → 1–2 days
- 4–6 objectives (skill building) → 2–3 days
- 6–8 objectives (comprehensive) → 3–5 days
- 8+ objectives (advanced) → 5+ days

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 2 — PAX-BASED ACTIVITY DESIGN (CRITICAL — APPLIES TO BOTH MODES)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Every activity in every module must be designed based on participant count. Apply the correct mode strictly.

─────────────────────────────
PAX MODE: INDIVIDUAL (1 Pax)
─────────────────────────────
Treatment: 1-on-1 coaching and guided learning. All activities are personal, reflective, and directly facilitated between trainer and single participant.

Activity types:
- Self-Assessment & Reflection (trainer-guided introspection on current gaps)
- Socratic Questioning (trainer poses probing questions; participant thinks aloud)
- Live Role Play (trainer plays counterpart: client, manager, customer, etc.)
- Personal Case Study (participant analyzes a real scenario from their own work)
- Action Planning (participant builds a personal implementation roadmap)
- Skill Demonstration (participant performs; trainer observes and gives live feedback)
- For Accredited Mode: 1-on-1 mock exam question walkthrough; trainer explains rationale per answer choice

Rules:
- No group activities, no peer feedback
- Every activity is a direct dialogue or demonstration
- Debrief is immediate and personal
- Outputs: personal action plans, self-audit sheets, skill checklists, practice answer logs

─────────────────────────────
PAX MODE: PAIR (2 Pax)
─────────────────────────────
Treatment: Two participants collaborate as a pair throughout. Activities are peer-to-peer with trainer observation.

Activity types:
- Paired Role Play (participants alternate roles)
- Peer Feedback (structured critique using a framework)
- Joint Case Study (shared analysis leading to a joint recommendation)
- Debate / Devil's Advocate (one argues for, one against a given approach)
- Collaborative Action Planning (shared team-level implementation plan)
- For Accredited Mode: Paired mock exam — each answers independently, then discusses rationale together

Rules:
- Both participants must contribute — no passive observer
- Trainer provides structured debriefs to the pair
- Outputs: shared plan or separate action plans post-discussion

─────────────────────────────
PAX MODE: SMALL GROUP (3–9 Pax)
─────────────────────────────
Treatment: Single cohort, no breakout groups needed. Collaborative and discussion-heavy.

Activity types:
- Facilitated Group Discussion
- Round-Robin Role Play (each participant rotates through roles)
- Group Case Study (unified recommendation from the cohort)
- Fishbowl Exercise (2–3 do the activity, others observe, then rotate)
- Collaborative Problem Solving
- Peer Teaching (each explains one concept back to the group)
- For Accredited Mode: Group exam question analysis; discuss why each option is right/wrong

Rules:
- Each participant has an active role in every activity
- Trainer manages airtime; no single participant dominates
- Full-group debrief; trainer synthesizes key takeaways

─────────────────────────────
PAX MODE: MEDIUM GROUP (10–20 Pax)
─────────────────────────────
Treatment: Workshop format with structured breakout teams and plenary sharing.

Activity types:
- Breakout Group Discussion (teams of 3–5, report back to plenary)
- Team Case Study (different teams, different scenarios, plenary presentation)
- Group Role Play (sub-group performs; others use observer checklists)
- Carousel / Gallery Walk (teams rotate stations)
- Simulation Exercise (structured scenario with defined roles)
- Brainstorming + Dot Voting
- For Accredited Mode: Domain-based team challenge; each team masters one domain and teaches others

Rules:
- Assign group roles: Facilitator, Timekeeper, Note-taker, Presenter
- Breakout groups: 3–5 participants
- Always debrief in plenary
- Rotate group compositions across modules

─────────────────────────────
PAX MODE: LARGE GROUP (21+ Pax)
─────────────────────────────
Treatment: High-energy facilitation. Plenary segments shorter; activity ratio increases. Structured sub-group work always.

Activity types:
- Sub-group Breakouts with structured templates (groups of 4–6)
- Panel Discussion (volunteers present to full group)
- World Café (rotating tables, different topic per table)
- Fishbowl with hot seat rotation
- Parallel Group Simulations
- Live Polling / Audience Response
- Jigsaw Learning (groups become domain experts, then teach others)
- For Accredited Mode: Parallel domain mastery groups; each group responsible for one exam domain

Rules:
- Never run an unstructured open discussion with 21+ Pax
- Use visual management: flipcharts, whiteboards, shared screens
- Build in energy breaks between modules
- Online: breakout rooms with timed instructions

─────────────────────────────
PAX MODE: STANDARD (Pax Not Provided)
─────────────────────────────
Treatment: Default to Medium Group (10–20 Pax) baseline. Add scalability notes to every activity.

Scalability note format (on every activity):
▸ For 1 Pax: [How to adapt — e.g., convert to 1-on-1 coaching/mock walkthrough]
▸ For Small Group (3–9): [How to adapt — e.g., run as full-group discussion, no breakouts]
▸ For Large Group (21+): [How to adapt — e.g., parallel sub-groups with same brief]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 3 — DELIVERY MODE OVERLAY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

- Onsite: Physical props, printed materials, room layout changes, standing exercises, physical role cards, printed practice papers (Accredited Mode)
- Online: Breakout rooms, shared docs, polls, digital whiteboards, timed online mock exams (Accredited Mode)
- Hybrid: Parallel in-room and remote tracks producing the same output; debriefed together

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 4 — ROI ANALYSIS (BOTH MODES)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Research and identify:
- Measurable business outcomes from similar training programs
- Industry benchmarks for performance improvement
- Tangible and intangible benefits
- Time-to-value expectations and sector-specific ROI metrics

ROI Categories:
- Productivity gains (time saved, efficiency increase)
- Quality improvements (error reduction, accuracy)
- Cost savings (reduced waste, lower compliance penalties)
- Revenue impact (sales increase, customer retention)
- Risk mitigation (compliance, safety incidents)
- Employee impact (retention, engagement, capability building)
- For Accredited Mode: Add salary premium data, employer demand stats, certified vs. non-certified performance benchmarks

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 5 — MODULE COUNT (CUSTOM MODE ONLY)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

- 1-day → 2–3 modules
- 2-day → 4–5 modules
- 3-day → 5–7 modules
- 4-day → 6–9 modules
- 5+ day → 8–12 modules

Flow: Foundation → Application → Mastery
Theory: 40% | Practice: 60%

For Accredited Mode: Module count = number of official domains/knowledge areas. Do not consolidate or expand domains — follow the official structure.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
QUALITY STANDARDS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Always:
- Run Step 0 first on every request — no exceptions
- Use web search immediately when Accredited Mode is triggered
- Display the Confidence Level banner clearly at the top of every accredited outline
- Use official domain/task language verbatim wherever it was found
- Supplement gaps with authoritative prep sources, not invented content
- Identify Pax and apply the correct Pax Mode before designing any activity
- Default to Standard Mode + scalability notes when Pax is unknown
- Research ROI benchmarks using web search for both modes
- Provide specific, quantifiable metrics — never vague statements
- Customize ROI to sector and company size
- Connect every ROI metric to a learning objective or official domain
- Cover all official domains (Accredited) or all learning objectives (Custom) across modules
- Use bullet points, not numbered sub-points within bullets

Never:
- Use Custom Course logic when an accredited program is detected
- Invent domain names, task names, or exam weightings for accredited programs
- State a passing score unless officially published by the certifying body
- Design group activities for a 1-Pax session
- Design individual reflection activities as the only option for 20+ Pax
- Provide generic or inflated ROI claims
- Use vague impact language without quantification
- Add timelines/schedules inside module sections
- Fabricate ROI data or improvement percentages
- Share source or reference links in the output
- Add certification pathways or materials lists in Custom Mode outputs

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
YOUR MISSION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

When a client brings a certification program, they are buying credibility — and that credibility rests entirely on alignment with the official body of knowledge. A PMP outline that doesn't follow PMI's ECO is not a PMP course. A SHRM outline that skips competency clusters is not SHRM prep. Your job is to deliver the real thing — faithfully sourced, clearly flagged for confidence level, and designed for the actual people sitting in the room.

When a client brings a custom training need, your job is the same discipline applied differently: a course built for their industry, their size, their people — with activities that actually work for the group in the room, not a generic template dressed up with their logo.

Both modes demand the same standard: researched, accurate, delivery-ready.
"""

STRICT_JSON_OUTPUT_RULES = """Return ONLY valid JSON (no markdown fences, no extra commentary).
The JSON must match this exact shape:
{
  "course_title": "string",
  "duration": "string",
  "total_hours": "string",
  "program_insight": {
    "paragraphs": ["string"],
    "bullets": ["string"]
  },
  "course_details": {
    "regions_served": "string",
    "course_duration": "string",
    "total_learning_hours": "string",
    "key_benefits": "string",
    "value_addition": "string",
    "location": "string",
    "date_time": "string"
  },
  "learning_objectives": [
    {"title": "string", "description": "string"}
  ],
  "capability_impact": [
    {"title": "string", "description": "string"}
  ],
  "modules": [
    {"module_title": "string", "topics": ["string"], "activities": ["string"]}
  ]
}
Use empty strings/lists where needed, but keep all keys present.
"""

REFINE_OUTLINE_PROMPT = """
You are refining an existing course outline based on stakeholder feedback.
Preserve quality, structure, and readability.
Apply feedback precisely without degrading formatting quality.
Return the same strict JSON schema as requested.
"""


class ClaudeService:
    def __init__(self) -> None:
        self.provider = str(getattr(settings, "AI_PROVIDER", "anthropic") or "anthropic").strip().lower()
        self.api_key = settings.ANTHROPIC_API_KEY
        self.base_url = settings.ANTHROPIC_BASE_URL.rstrip("/")
        self.model = settings.ANTHROPIC_MODEL
        self.openai_api_key = str(getattr(settings, "OPENAI_API_KEY", "") or "").strip()
        self.openai_base_url = str(getattr(settings, "OPENAI_BASE_URL", "https://api.openai.com") or "").rstrip("/")
        self.openai_model = str(getattr(settings, "OPENAI_MODEL", "gpt-4o-mini") or "").strip()

        if self.provider == "openai":
            if not self.openai_api_key:
                raise ValueError("OPENAI_API_KEY is missing in environment.")
        else:
            if not self.api_key:
                raise ValueError("ANTHROPIC_API_KEY is missing in environment.")

    async def generate_text_completion(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        timeout_s: float = 180.0,
        model_override: str | None = None,
    ) -> str:
        """Generic completion for auxiliary flows (e.g. assessments). Uses configured AI_PROVIDER."""
        return await self._call_messages_api(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            timeout_s=timeout_s,
            max_attempts=DEFAULT_MAX_ATTEMPTS,
            model_override=model_override,
        )

    async def _call_messages_api(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        timeout_s: float,
        max_attempts: int,
        model_override: str | None = None,
    ) -> str:
        if self.provider == "openai":
            return await self._call_openai_chat_api(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                timeout_s=timeout_s,
                max_attempts=max_attempts,
                model_override=model_override,
            )

        url = f"{self.base_url}/v1/messages"
        headers = {
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        payload = {
            "model": self.model,
            "max_tokens": 8192,
            "temperature": 0.2,
            "system": system_prompt,
            "tools":[{"type": "web_search_20250305","name": "web_search"}],
            "messages": [{"role": "user", "content": user_prompt}],
        }

        timeout = httpx.Timeout(timeout_s, connect=10.0)
        last_error: Optional[BaseException] = None

        candidate_models: list[str] = []
        preferred_model = (model_override or "").strip() or self.model
        for m in (preferred_model, self.model, "claude-3-5-sonnet-latest", "claude-sonnet-4-20250514"):
            mm = (m or "").strip()
            if mm and mm not in candidate_models:
                candidate_models.append(mm)

        for model_idx, model_name in enumerate(candidate_models, start=1):
            payload["model"] = model_name
            for attempt in range(1, max_attempts + 1):
                try:
                    async with httpx.AsyncClient(timeout=timeout) as client:
                        response = await client.post(url, headers=headers, json=payload)

                    if response.status_code == 404:
                        body_preview = (response.text or "")[:500]
                        logger.error(
                            "Anthropic returned 404 | url=%s model=%s body=%s",
                            url,
                            model_name,
                            body_preview,
                        )
                        raise AnthropicConfigurationError(
                            "Anthropic API returned 404. Check ANTHROPIC_BASE_URL (must be "
                            "https://api.anthropic.com without /v1) and ANTHROPIC_MODEL "
                            "(use a valid model ID from Anthropic docs, e.g. claude-sonnet-4-20250514)."
                        )

                    if response.status_code == 400:
                        body_preview = (response.text or "")[:1200]
                        logger.warning(
                            "Anthropic returned 400 | model=%s attempt=%s/%s body=%s",
                            model_name,
                            attempt,
                            max_attempts,
                            body_preview,
                        )
                        if "credit balance is too low" in body_preview.lower():
                            raise RuntimeError(
                                "Anthropic credits are exhausted. Please add credits in Anthropic Plans & Billing."
                            )
                        # Common local issue: invalid ANTHROPIC_MODEL value in .env.
                        if ("model" in body_preview.lower() or "not_found_error" in body_preview.lower()) and model_idx < len(candidate_models):
                            logger.warning(
                                "Anthropic model rejected; trying fallback model next | current_model=%s",
                                model_name,
                            )
                            break

                    if response.status_code >= 500:
                        raise httpx.HTTPStatusError(
                            "Claude server error", request=response.request, response=response
                        )
                    response.raise_for_status()

                    data = response.json()
                    content_blocks = data.get("content", [])
                    if not isinstance(content_blocks, list) or not content_blocks:
                        raise RuntimeError("Claude returned empty content.")

                    text_parts: list[str] = []
                    for block in content_blocks:
                        if isinstance(block, dict) and block.get("type") == "text":
                            text_value = block.get("text")
                            if isinstance(text_value, str):
                                text_parts.append(text_value)

                    final_text = "\n".join(text_parts).strip()
                    if not final_text:
                        raise RuntimeError("Claude returned blank text.")

                    if model_name != self.model:
                        logger.warning(
                            "Anthropic model fallback in use | configured=%s active=%s",
                            self.model,
                            model_name,
                        )
                    return final_text

                except AnthropicConfigurationError:
                    raise
                except (httpx.TimeoutException, httpx.RequestError, httpx.HTTPStatusError, RuntimeError) as exc:
                    last_error = exc
                    logger.warning(
                        "Claude call failed attempt=%s/%s model=%s error=%r",
                        attempt,
                        max_attempts,
                        model_name,
                        exc,
                    )
                    if attempt < max_attempts:
                        await asyncio.sleep(2 ** (attempt - 1))

        logger.error("Claude failed after retries | last_error=%r", last_error)
        raise RuntimeError("AI service failed after retries. Please try again later.")

    async def _call_openai_chat_api(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        timeout_s: float,
        max_attempts: int,
        model_override: str | None = None,
    ) -> str:
        url = f"{self.openai_base_url}/v1/chat/completions"
        active_model = (model_override or "").strip() or self.openai_model
        headers = {
            "Authorization": f"Bearer {self.openai_api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": active_model,
            "temperature": 0.2,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        timeout = httpx.Timeout(timeout_s, connect=10.0)
        last_error: Optional[BaseException] = None

        for attempt in range(1, max_attempts + 1):
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.post(url, headers=headers, json=payload)

                if response.status_code == 404:
                    body_preview = (response.text or "")[:500]
                    logger.error(
                        "OpenAI returned 404 | url=%s model=%s body=%s",
                        url,
                        self.openai_model,
                        body_preview,
                    )
                    raise OpenAIConfigurationError(
                        "OpenAI API returned 404. Check OPENAI_BASE_URL (must be root URL without /v1) and OPENAI_MODEL."
                    )

                if response.status_code == 401:
                    raise OpenAIConfigurationError("OpenAI authentication failed. Check OPENAI_API_KEY.")

                if response.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        "OpenAI server error", request=response.request, response=response
                    )
                if response.status_code >= 400:
                    body_preview = (response.text or "")[:1200]
                    logger.warning(
                        "OpenAI returned %s | model=%s attempt=%s/%s body=%s",
                        response.status_code,
                        active_model,
                        attempt,
                        max_attempts,
                        body_preview,
                    )
                response.raise_for_status()

                data = response.json()
                choices = data.get("choices") if isinstance(data, dict) else None
                if not isinstance(choices, list) or not choices:
                    raise RuntimeError("OpenAI returned empty choices.")
                first = choices[0] if isinstance(choices[0], dict) else {}
                message = first.get("message") if isinstance(first, dict) else {}
                content = message.get("content") if isinstance(message, dict) else None
                if not isinstance(content, str) or not content.strip():
                    raise RuntimeError("OpenAI returned blank content.")
                return content.strip()

            except OpenAIConfigurationError:
                raise
            except (httpx.TimeoutException, httpx.RequestError, httpx.HTTPStatusError, RuntimeError) as exc:
                last_error = exc
                logger.warning("OpenAI call failed attempt=%s/%s error=%r", attempt, max_attempts, exc)
                if attempt < max_attempts:
                    await asyncio.sleep(2 ** (attempt - 1))

        logger.error("OpenAI failed after retries | last_error=%r", last_error)
        raise RuntimeError("AI service failed after retries. Please try again later.")

    def _extract_json_candidate(self, text: str) -> str:
        raw = (text or "").strip()
        if not raw:
            return raw
        if raw.startswith("{") and raw.endswith("}"):
            return raw
        fenced = raw
        if fenced.startswith("```"):
            fenced = fenced.strip("`")
            if fenced.lower().startswith("json"):
                fenced = fenced[4:].strip()
        first = fenced.find("{")
        last = fenced.rfind("}")
        if first >= 0 and last > first:
            return fenced[first : last + 1]
        return raw

    async def build_learning_objectives(
        self,
        context_text: str,
        *,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> str:
        return await self._call_messages_api(
            system_prompt=LEARNING_OBJECTIVES_PROMPT,
            user_prompt=context_text,
            timeout_s=timeout_s,
            max_attempts=max_attempts,
        )

    async def build_roi_course_outline(
        self,
        context_text: str,
        learning_objectives_text: str,
        *,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> str:
        user_prompt = (
            "Input Context:\n"
            f"{context_text}\n\n"
            "Learning Objectives Output:\n"
            f"{learning_objectives_text}\n"
        )
        return await self._call_messages_api(
            system_prompt=ROI_OUTLINE_PROMPT,
            user_prompt=user_prompt,
            timeout_s=timeout_s,
            max_attempts=max_attempts,
        )

    async def build_roi_course_outline_json(
        self,
        context_text: str,
        learning_objectives_text: str,
        *,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> CourseOutlinePayload:
        user_prompt = (
            "Input Context:\n"
            f"{context_text}\n\n"
            "Learning Objectives Output:\n"
            f"{learning_objectives_text}\n"
        )
        system_prompt = ROI_OUTLINE_PROMPT + "\n\n" + STRICT_JSON_OUTPUT_RULES

        for attempt in range(1, max_attempts + 1):
            raw = await self._call_messages_api(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                timeout_s=timeout_s,
                max_attempts=1,
            )
            try:
                json_candidate = self._extract_json_candidate(raw)
                data = json.loads(json_candidate)
                return CourseOutlinePayload(**data)
            except Exception as exc:
                logger.warning("Structured outline validation failed attempt=%s/%s error=%r", attempt, max_attempts, exc)
                if attempt < max_attempts:
                    user_prompt = (
                        "Your previous output was invalid JSON for the required schema. "
                        "Return corrected JSON only.\n\n"
                        f"Original input context:\n{context_text}\n\n"
                        f"Learning objectives:\n{learning_objectives_text}\n"
                    )
                    await asyncio.sleep(2 ** (attempt - 1))
                    continue
                raise RuntimeError("AI service returned invalid structured output.")

    async def refine_course_outline_json(
        self,
        previous_outline_json_or_text: str,
        feedback: str,
        *,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> CourseOutlinePayload:
        user_prompt = (
            "Previous course outline:\n"
            f"{previous_outline_json_or_text}\n\n"
            "User feedback to apply:\n"
            f"{feedback}\n"
        )
        system_prompt = REFINE_OUTLINE_PROMPT + "\n\n" + STRICT_JSON_OUTPUT_RULES

        for attempt in range(1, max_attempts + 1):
            raw = await self._call_messages_api(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                timeout_s=timeout_s,
                max_attempts=1,
            )
            try:
                json_candidate = self._extract_json_candidate(raw)
                data = json.loads(json_candidate)
                return CourseOutlinePayload(**data)
            except Exception as exc:
                logger.warning("Structured refine validation failed attempt=%s/%s error=%r", attempt, max_attempts, exc)
                if attempt < max_attempts:
                    user_prompt = (
                        "Your previous refine output was invalid JSON for the required schema. "
                        "Return corrected JSON only.\n\n"
                        f"Previous outline:\n{previous_outline_json_or_text}\n\n"
                        f"Feedback:\n{feedback}\n"
                    )
                    await asyncio.sleep(2 ** (attempt - 1))
                    continue
                raise RuntimeError("AI service returned invalid structured output for refine.")

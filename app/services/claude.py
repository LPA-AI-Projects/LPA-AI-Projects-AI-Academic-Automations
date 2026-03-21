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
You are the Course Outline Builder for Learners Point Academy. You transform learning objectives into comprehensive, research-backed, delivery-ready course outlines with clear ROI demonstration.

INPUT
Learning Objectives (from Training Needs Analysis Agent)
Company Context (sector,
size of company: eg,
              Micro: 50-500
              Mid: 500-5000
              Large: 5000-10,000
              Giant: 10,000+
Training Duration (if provided)
Participant Details (number, level, roles)
Delivery Mode (onsite/online/hybrid)

PROCESS

1. RESEARCH & DURATION
Use web search to research:
Industry best practices and current trends
Standard training durations for similar programs
Content depth required for objectives
ROI benchmarks and business impact data for similar training programs

Determine Duration (8 hours/day):
If provided -> Use it
If not provided -> Recommend based on:
3-4 objectives (awareness) -> 1-2 days
4-6 objectives (skill building) -> 2-3 days
6-8 objectives (comprehensive) -> 3-5 days
8+ objectives (advanced) -> 5+ days

2. ROI ANALYSIS & RESEARCH
Research and identify:
Measurable business outcomes from similar training
Industry benchmarks for performance improvement
Tangible and intangible benefits
Time-to-value expectations
Sector-specific ROI metrics

ROI Categories to Consider:
- Productivity gains (time saved, efficiency increase)
- Quality improvements (error reduction, accuracy increase)
- Cost savings (reduced waste, lower compliance penalties)
- Revenue impact (sales increase, customer retention)
- Risk mitigation (compliance, safety incidents reduction)
- Employee impact (retention, engagement, capability building)

3. MODULE COUNT DETERMINATION
Module count is FLEXIBLE based on course length, content depth, and learning objectives.

Guidelines:
1-day -> 2-3 modules
2-day -> 4-5 modules
3-day -> 5-7 modules
4-day -> 6-9 modules
5+ day -> 8-12 modules

Create as many modules as needed to properly cover all objectives with logical flow.

4. MODULE DESIGN
Each module must:
Address specific learning objectives
Flow logically (foundation -> application -> mastery)
Balance theory (40%) and practice (60%)
Be sector-contextualized

5. ACTIVITY DESIGN
Design based on:
Participants: Pairs (<10), groups (10-20), mixed (20+)
Delivery: Physical props (onsite), breakout rooms (online)
Level: Simplified (junior), complex (senior)
Objective: Discussion (awareness), role-play (skills), simulation (application)

Activity Types: Group Discussion, Case Study, Role Play, Simulation, Exercise, Group Work, Brainstorming

Activities = 40-60% of module time

OUTPUT FORMAT

Course Title: [Compelling, professional title]

Duration: [X days (Y hours)]

Course Overview:
[2-3 paragraphs: what it covers, target audience, sector relevance, participant gains]

Training Objectives:
- [Objective 1]
- [Objective 2]
- [Objective 3]
- [All objectives from input]

Key Outcomes:
By the end of this program, participants will be able to:
- [Outcome 1 - specific, measurable, sector-relevant]
- [Outcome 2 - specific, measurable, sector-relevant]
- [Outcome 3 - specific, measurable, sector-relevant]
- [All outcomes]

---

TRAINING ROI & BUSINESS IMPACT


RETURN ON INVESTMENT (ROI)

Expected Business Impact & Returns:
Business Impact: Explain where and how is the optimization happing
(*Note- ROI should be of 2 pages where the information should be summarized in a tabular format displaying the 3 phases of impact and the rest of the information should be concised)
(*Note-In business impact the result should be in bulleted format and improvement should be in %)

IMMEDIATE IMPACT (1-3 Months Post-Training):
Participants will immediately apply learned skills, resulting in measurable improvements:

- [Specific Metric/Area]: Expected improvement of [X]% in [specific outcome]
  Rationale: [Brief explanation of how training drives this improvement]

- [Specific Metric/Area]: Reduction of [X]% in [errors/time/costs/incidents]
  Rationale: [Brief explanation of how training drives this improvement]

- [Specific Metric/Area]: Increase of [X]% in [productivity/efficiency/quality]
  Rationale: [Brief explanation of how training drives this improvement]

SHORT-TERM IMPACT (3-6 Months Post-Training):
As skills become embedded in daily work practices:

- [Specific Metric/Area]: Sustained improvement of [X]% in [specific outcome]
  Rationale: [Brief explanation of compounding effects]

- [Specific Metric/Area]: Cost savings of approximately [currency/percentage] through [specific change]
  Rationale: [Brief explanation of how this translates to business value]

- [Specific Metric/Area]: Enhanced [quality/speed/accuracy] leading to [X]% improvement in [business metric]
  Rationale: [Brief explanation of the connection]

LONG-TERM IMPACT (6-12 Months Post-Training):
Strategic and cultural transformation delivers sustained value:

- [Specific Metric/Area]: [X]% improvement in [strategic outcome]
  Rationale: [Brief explanation of long-term cultural/capability shift]

- [Specific Metric/Area]: Competitive advantage through [specific capability]
  Rationale: [Brief explanation of strategic positioning]

- [Specific Metric/Area]: Reduced [risk/turnover/incidents] by [X]%
  Rationale: [Brief explanation of preventive value]

(*Note-Donot add elaborated impacts in phases. Add just the tabular columns)

Break-Even Timeline: Estimated [X] months
ROI Multiplier: Projected [X]x return on investment within 12 months
(* Mention it below the tabular columns that this ROI framework reflects observed performance patterns from similar training engagements delivered across financial services, corporate sales, relationship management, and leadership teams.
The extent and speed of impact are directly influenced by how effectively employees apply the knowledge, tools, and behaviors introduced during the program, along with leadership reinforcement and on-the-job execution discipline. )

COURSE OUTLINE

Module 1: [Title]
[1-2 sentence introduction]

Topics Covered:
- [Topic]: [Detailed description of content, key concepts, practical applications - 1-2 sentences]
- [Topic]: [Detailed description - substantive, not just title]
- [Topic]: [Detailed description]
- [Topic]: [Detailed description]

Activity:
[Activity Type]: [Comprehensive description including scenario, what participants do, structure based on count/delivery mode, expected outcomes, sector connection]

---

Module 2: [Title]
[1-2 sentence introduction]

Topics Covered:
- [Topic]: [Detailed description]
- [Topic]: [Detailed description]
- [Topic]: [Detailed description]

Activity:
[Activity Type]: [Comprehensive, contextualized description]

---

[Continue for all modules - create optimal number based on duration and objectives]

---

Conclusion:
[2-3 paragraphs: what participants will have gained, how it applies to their work context, the transformation expected, reinforcement of ROI and business impact]

---

QUALITY STANDARDS

Always:
? Research ROI benchmarks using web search
? Provide specific, quantifiable metrics (not vague statements)
? Customize ROI to their sector and company size
? Use realistic, research-backed improvement percentages
? Create visual ROI representation using text graphics
? Connect ROI to specific learning objectives
? Make topic descriptions detailed and substantive
? Contextualize activities to sector, level, delivery mode
? Cover all learning objectives across modules
? Create RIGHT number of modules (not fixed)
? Use bullet points (not numbered sub-points)

Never:
? Provide generic or inflated ROI claims
? Use vague terms like "better performance" without quantification
? Ignore sector-specific ROI factors
? Fix module count (e.g., always 4 modules)
? Add timelines/schedules to module sections
? Guess or fabricate ROI data
? Add certification pathways or materials lists
? Share source links in output
? Create ROI metrics that can't be measured

ROI RESEARCH GUIDELINES:
- Search for "[course topic] training ROI statistics"
- Search for "[industry] training impact metrics"
- Look for case studies with measurable outcomes
- Find industry benchmarks for performance improvements
- Identify typical time-to-value for similar training
- Consider both tangible (cost, revenue) and intangible (engagement, culture) returns

Output Format
Course Title:
Duration:
Course Overview
Training Objectives
Key Outcomes
ROI
Module wise Course outline
Conclusion

Let content and objectives drive module count, not predetermined numbers.

Dont add researched links in the output

---

Your Mission: Create a comprehensive course outline with compelling, research-backed ROI that demonstrates clear business value and return on training investment.
*Note - donot display the reference or source link in the output.
"""

STRICT_JSON_OUTPUT_RULES = """
You MUST return ONLY valid JSON and no other text.
Do not wrap in markdown fences.
Do not rename keys.
Do not leave required fields empty.
Keep values concise: avoid very long paragraphs; use short, practical descriptions.

MODULE CONTENT RULES (CRITICAL):
- Each topic: max 8-12 words.
- Each activity: max 12-15 words.
- No paragraphs in topics/activities.
- 4-6 topics per module only.
- 2-4 activities per module only.

MODULE COUNT RULE:
- 1-2 day course: max 4 modules.
- 3+ day course: max 6 modules.
- Never exceed 6 modules.

Required JSON shape:
{
  "course_title": "",
  "duration": "",
  "total_hours": "",
  "program_insight": {
    "paragraphs": [],
    "bullets": []
  },
  "course_details": {
    "regions_served": "",
    "course_duration": "",
    "total_learning_hours": "",
    "key_benefits": "",
    "value_addition": "",
    "location": "",
    "date_time": ""
  },
  "learning_objectives": [{"title": "", "description": ""}],
  "capability_impact": [{"title": "", "description": ""}],
  "modules": [{"module_title": "", "topics": [], "activities": []}]
}
"""

REFINE_OUTLINE_PROMPT = """
You are refining an existing course outline based on stakeholder feedback.
Preserve quality, structure, and readability.
Apply feedback precisely without degrading formatting quality.
Return the same strict JSON schema as requested.
"""


class ClaudeService:
    def __init__(self) -> None:
        self.api_key = settings.ANTHROPIC_API_KEY
        self.base_url = settings.ANTHROPIC_BASE_URL.rstrip("/")
        self.model = settings.ANTHROPIC_MODEL
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY is missing in environment.")

    async def _call_messages_api(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        timeout_s: float,
        max_attempts: int,
    ) -> str:
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
            "messages": [{"role": "user", "content": user_prompt}],
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
                        "Anthropic returned 404 | url=%s model=%s body=%s",
                        url,
                        self.model,
                        body_preview,
                    )
                    raise AnthropicConfigurationError(
                        "Anthropic API returned 404. Check ANTHROPIC_BASE_URL (must be "
                        "https://api.anthropic.com without /v1) and ANTHROPIC_MODEL "
                        "(use a valid model ID from Anthropic docs, e.g. claude-sonnet-4-20250514)."
                    )

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

                return final_text

            except AnthropicConfigurationError:
                raise
            except (httpx.TimeoutException, httpx.RequestError, httpx.HTTPStatusError, RuntimeError) as exc:
                last_error = exc
                logger.warning("Claude call failed attempt=%s/%s error=%r", attempt, max_attempts, exc)
                if attempt < max_attempts:
                    await asyncio.sleep(2 ** (attempt - 1))

        logger.error("Claude failed after retries | last_error=%r", last_error)
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

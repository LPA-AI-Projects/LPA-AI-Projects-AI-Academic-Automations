from __future__ import annotations

import asyncio
from typing import Any

import httpx

from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class GammaNotConfigured(RuntimeError):
    pass


def _gamma_configured() -> bool:
    return bool(getattr(settings, "GAMMA_API_KEY", "") and getattr(settings, "GAMMA_BASE_URL", ""))


async def generate_ppt(
    slides_batch: list[dict[str, Any]],
    *,
    additional_instructions: str = "",
    include_export_bytes: bool = True,
) -> dict[str, Any]:
    """
    Uses Gamma Public API async workflow:
    - POST /v1.0/generations
    - poll GET /v1.0/generations/{id}
    - optionally download exportUrl (pptx)
    """
    if not _gamma_configured():
        raise GammaNotConfigured("Gamma API is not configured. Set GAMMA_API_KEY and GAMMA_BASE_URL.")

    base = str(getattr(settings, "GAMMA_BASE_URL")).rstrip("/")
    api_key = str(getattr(settings, "GAMMA_API_KEY")).strip()

    # Build a compact, deterministic inputText for Gamma.
    lines: list[str] = []
    for i, s in enumerate(slides_batch, start=1):
        title = str(s.get("title") or "").strip()
        bullets = s.get("bullets") if isinstance(s.get("bullets"), list) else []
        notes = str(s.get("notes") or "").strip()
        visual = str(s.get("visual") or "").strip()
        lines.append(f"Slide {i}: {title}")
        for b in bullets[:8]:
            lines.append(f"- {str(b).strip()}")
        if notes:
            lines.append(f"Speaker notes: {notes}")
        if visual:
            lines.append(f"Visual suggestion: {visual}")
        lines.append("")  # blank line separator

    input_text = "\n".join(lines).strip()

    create_url = f"{base}/v1.0/generations"
    headers = {"X-API-KEY": api_key, "Accept": "application/json"}
    desired_cards = max(1, len(slides_batch))
    strict_instructions = (
        f"Create exactly {desired_cards} slides/cards. "
        "Treat each 'Slide X:' section as one distinct slide. "
        "Do not merge sections. Do not skip sections. "
        "Do not summarize into fewer slides. Keep slide count very close to requested."
    )
    merged_instructions = strict_instructions
    if (additional_instructions or "").strip():
        merged_instructions = f"{strict_instructions} {(additional_instructions or '').strip()}"

    payload = {
        "inputText": input_text,
        # Gamma defaults can condense aggressively; generate + numCards gives tighter count control.
        "textMode": "generate",
        "format": "presentation",
        "numCards": desired_cards,
        "exportAs": "pptx",
        "additionalInstructions": merged_instructions[:5000],
    }

    logger.info(
        "Gamma createGeneration | slides=%s num_cards=%s input_chars=%s",
        len(slides_batch),
        desired_cards,
        len(input_text),
    )

    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(create_url, headers=headers, json=payload)
        if resp.status_code >= 400:
            logger.error(
                "Gamma createGeneration rejected | status_code=%s body=%s",
                resp.status_code,
                (resp.text or "")[:2000],
            )
        resp.raise_for_status()
        data = resp.json()
        gen_id = data.get("generationId")
        if not isinstance(gen_id, str) or not gen_id:
            raise RuntimeError("Gamma createGeneration did not return generationId.")
        logger.info("Gamma generation created | generationId=%s", gen_id)

        status_url = f"{base}/v1.0/generations/{gen_id}"
        export_url: str | None = None
        gamma_url: str | None = None
        for attempt in range(1, 121):
            st = await client.get(status_url, headers=headers)
            st.raise_for_status()
            st_data = st.json()
            status = st_data.get("status")
            if status == "completed":
                export_url = st_data.get("exportUrl")
                gamma_url = st_data.get("gammaUrl")
                logger.info("Gamma generation completed | generationId=%s", gen_id)
                break
            if status == "failed":
                err = st_data.get("error") or {}
                logger.warning("Gamma generation failed | generationId=%s error=%s", gen_id, err)
                raise RuntimeError(f"Gamma generation failed: {err}")
            if attempt in (1, 5, 10, 20, 40, 80, 120):
                logger.info("Gamma generation polling | generationId=%s attempt=%s status=%s", gen_id, attempt, status)
            await asyncio.sleep(2.0)

        if include_export_bytes:
            if not export_url or not isinstance(export_url, str):
                raise RuntimeError("Gamma generation did not complete with exportUrl.")
            logger.info("Gamma downloading export | generationId=%s", gen_id)
            dl = await client.get(export_url, headers=headers, timeout=120.0)
            dl.raise_for_status()
            logger.info("Gamma export downloaded | generationId=%s bytes=%s", gen_id, len(dl.content))
            return {
                "ppt_bytes": dl.content,
                "generation_id": gen_id,
                "gamma_url": gamma_url,
            }

        logger.info("Gamma link-only mode | generationId=%s", gen_id)
        return {
            "ppt_bytes": b"",
            "generation_id": gen_id,
            "gamma_url": gamma_url,
        }


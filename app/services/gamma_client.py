from __future__ import annotations

import asyncio
from typing import Any

import httpx

from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

# Keep Gamma prompts within typical 16:9 card height (template + export warnings).
GAMMA_PROMPT_MAX_BULLETS_PER_SLIDE = 5
GAMMA_PROMPT_MAX_SPEAKER_NOTES_CHARS = 320


class GammaNotConfigured(RuntimeError):
    pass


def _gamma_configured() -> bool:
    return bool(getattr(settings, "GAMMA_API_KEY", "") and getattr(settings, "GAMMA_BASE_URL", ""))


def _build_sharing_options() -> dict[str, Any]:
    workspace_access = str(getattr(settings, "GAMMA_WORKSPACE_ACCESS", "") or "").strip() or "edit"
    external_access = str(getattr(settings, "GAMMA_EXTERNAL_ACCESS", "") or "").strip() or "edit"
    sharing: dict[str, Any] = {
        "workspaceAccess": workspace_access,
        "externalAccess": external_access,
    }
    recipients = settings.get_gamma_email_edit_list()
    if recipients:
        email_access = str(getattr(settings, "GAMMA_EMAIL_OPTIONS_ACCESS", "") or "").strip() or "edit"
        sharing["emailOptions"] = {
            "recipients": recipients,
            "access": email_access,
        }
    return sharing


def _build_image_options_for_template() -> dict[str, Any] | None:
    """
    Optional imageOptions for POST /v1.0/generations/from-template only.

    Gamma rejects ``source`` in template mode (e.g. ``source should not exist``).
    Allowed keys are effectively ``model`` and ``style`` only; ``GAMMA_IMAGE_SOURCE``
    is ignored here and applies only to non-template generation if we add it later.
    """
    out: dict[str, Any] = {}
    model = str(getattr(settings, "GAMMA_IMAGE_MODEL", "") or "").strip()
    if model:
        out["model"] = model
    style = str(getattr(settings, "GAMMA_IMAGE_STYLE", "") or "").strip()
    if style:
        out["style"] = style
    return out or None


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

    # Build a compact, deterministic inputText for Gamma (dense decks overflow 16:9 cards).
    lines: list[str] = []
    cap = GAMMA_PROMPT_MAX_BULLETS_PER_SLIDE
    for i, s in enumerate(slides_batch, start=1):
        title = str(s.get("title") or "").strip()
        bullets = s.get("bullets") if isinstance(s.get("bullets"), list) else []
        notes = str(s.get("notes") or "").strip()
        if len(notes) > GAMMA_PROMPT_MAX_SPEAKER_NOTES_CHARS:
            notes = notes[:GAMMA_PROMPT_MAX_SPEAKER_NOTES_CHARS].rsplit(" ", 1)[0].strip() + "…"
        visual = str(s.get("visual") or "").strip()
        lines.append(f"Slide {i}: {title}")
        for b in bullets[:cap]:
            lines.append(f"- {str(b).strip()}")
        if notes:
            lines.append(f"Speaker notes (presenter only; do not paste verbatim as body text): {notes}")
        if visual:
            lines.append(
                "Required on-slide graphic (photo, AI illustration, or diagram — not an empty box): "
                f"{visual}"
            )
        lines.append("")  # blank line separator

    input_text = "\n".join(lines).strip()

    use_template = bool(getattr(settings, "GAMMA_USE_TEMPLATE", False))
    template_id = str(getattr(settings, "GAMMA_TEMPLATE_ID", "") or "").strip()
    use_template = use_template and bool(template_id)
    create_url = f"{base}/v1.0/generations/from-template" if use_template else f"{base}/v1.0/generations"
    headers = {"X-API-KEY": api_key, "Accept": "application/json"}
    desired_cards = max(1, len(slides_batch))
    strict_instructions = (
        f"Create exactly {desired_cards} slides/cards. "
        "Treat each 'Slide X:' section as one distinct slide. "
        "Do not merge sections. Do not skip sections. "
        "Do not summarize into fewer slides. Keep slide count very close to requested."
    )
    # Gamma warns when card content exceeds 16:9 — bias toward shorter on-slide copy + real graphics.
    visual_layout_instructions = (
        "Layout (16:9): Each card must fit without vertical overflow or tiny illegible text. "
        "Title + at most 4–5 short bullets per slide (roughly ≤12 words per bullet). "
        "Do not duplicate speaker notes on the slide. Prefer splitting ideas across slides over shrinking fonts. "
        "Graphics: Every 'Required on-slide graphic' line must result in a visible asset — AI illustration, "
        "photo, or a clear diagram/infographic (flowchart, matrix, cycle, icon set, simple chart). "
        "Do not leave image or diagram regions as empty grey placeholders; if the template has a side or "
        "lower panel for visuals, use it. Match diagram style to the description (e.g. 2x2 grid = four-quadrant "
        "graphic with labels). Use consistent iconography across the deck. "
        "Keep margins, title band, and body rhythm consistent; one coherent palette."
    )
    merged_instructions = f"{strict_instructions} {visual_layout_instructions}"
    if (additional_instructions or "").strip():
        merged_instructions = f"{merged_instructions} {(additional_instructions or '').strip()}"

    payload: dict[str, Any]
    sharing_options = _build_sharing_options()
    if use_template:
        prompt = f"{input_text}\n\nInstructions:\n{merged_instructions[:5000]}"
        payload = {
            "gammaId": template_id,
            "prompt": prompt,
            "exportAs": "pptx",
            "sharingOptions": sharing_options,
        }
        image_opts = _build_image_options_for_template()
        if image_opts:
            payload["imageOptions"] = image_opts
        theme_id = str(getattr(settings, "GAMMA_THEME_ID", "") or "").strip()
        if theme_id:
            payload["themeId"] = theme_id
    else:
        payload = {
            "inputText": input_text,
            # Gamma defaults can condense aggressively; generate + numCards gives tighter count control.
            "textMode": "generate",
            "format": "presentation",
            "numCards": desired_cards,
            "exportAs": "pptx",
            "additionalInstructions": merged_instructions[:5000],
            "sharingOptions": sharing_options,
        }

    gamma_endpoint = "from-template" if use_template else "generate"
    logger.info(
        "Gamma createGeneration | slides=%s num_cards=%s input_chars=%s mode=%s url_suffix=%s template_set=%s",
        len(slides_batch),
        desired_cards,
        len(input_text),
        gamma_endpoint,
        "/v1.0/generations/from-template" if use_template else "/v1.0/generations",
        bool(template_id),
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
        editable_gamma_url: str | None = None
        for attempt in range(1, 121):
            st = await client.get(status_url, headers=headers)
            st.raise_for_status()
            st_data = st.json()
            status = st_data.get("status")
            if status == "completed":
                export_url = st_data.get("exportUrl")
                gamma_url = st_data.get("gammaUrl")
                # Gamma API can vary naming across versions/tenants.
                editable_gamma_url = (
                    st_data.get("editableGammaUrl")
                    or st_data.get("editUrl")
                    or st_data.get("editorUrl")
                    or st_data.get("workspaceUrl")
                    or st_data.get("presentationUrl")
                )
                if not editable_gamma_url:
                    editable_gamma_url = gamma_url
                logger.info("Gamma generation completed | generationId=%s", gen_id)
                break
            if status == "failed":
                err = st_data.get("error") or {}
                logger.warning("Gamma generation failed | generationId=%s error=%s", gen_id, err)
                raise RuntimeError(f"Gamma generation failed: {err}")
            if attempt in (1, 5, 10, 20, 40, 80, 120):
                logger.info("Gamma generation polling | generationId=%s attempt=%s status=%s", gen_id, attempt, status)
            await asyncio.sleep(2.0)

        out_base: dict[str, Any] = {
            "generation_id": gen_id,
            "gamma_url": gamma_url,
            "editable_gamma_url": editable_gamma_url,
            # Echoes the JSON body sent to Gamma (for DB/debugging; no API key in body).
            "gamma_endpoint": gamma_endpoint,
            "gamma_create_url": create_url,
            "request_payload": dict(payload),
        }
        if include_export_bytes:
            if not export_url or not isinstance(export_url, str):
                raise RuntimeError("Gamma generation did not complete with exportUrl.")
            logger.info("Gamma downloading export | generationId=%s", gen_id)
            dl = await client.get(export_url, headers=headers, timeout=120.0)
            dl.raise_for_status()
            logger.info("Gamma export downloaded | generationId=%s bytes=%s", gen_id, len(dl.content))
            return {
                **out_base,
                "ppt_bytes": dl.content,
            }

        logger.info("Gamma link-only mode | generationId=%s", gen_id)
        return {
            **out_base,
            "ppt_bytes": b"",
        }


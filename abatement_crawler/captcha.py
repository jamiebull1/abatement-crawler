"""Captcha and bot-challenge detection for the abatement crawler."""

from __future__ import annotations


class CaptchaDetected(Exception):
    """Raised when a captcha or bot-challenge page is detected in an HTTP response."""

    def __init__(self, url: str, captcha_type: str) -> None:
        self.url = url
        self.captcha_type = captcha_type
        super().__init__(f"Captcha detected at {url}: {captcha_type}")


def detect_captcha(response: object) -> str | None:
    """Inspect an HTTP response for captcha or bot-challenge indicators.

    Args:
        response: A ``requests.Response``-like object with ``.status_code``
                  and ``.text`` attributes.

    Returns:
        A short string identifying the captcha type (e.g. ``"cloudflare"``,
        ``"recaptcha"``, ``"hcaptcha"``, ``"rate_limited"``, ``"bot_protection"``,
        ``"generic"``), or ``None`` if no captcha is detected.
    """
    # HTTP 429 is an unambiguous rate-limit / bot signal
    if getattr(response, "status_code", 200) == 429:
        return "rate_limited"

    try:
        html = response.text.lower()
    except Exception:
        return None

    # Cloudflare IUAM / Turnstile / challenge pages
    if any(
        s in html
        for s in (
            "cf-challenge",
            "cf_chl_opt",
            "just a moment",
            "checking your browser",
            "enable javascript and cookies",
            "cloudflare ray id",
        )
    ):
        return "cloudflare"

    # Google reCAPTCHA (v2 / v3 / Enterprise)
    if "www.google.com/recaptcha" in html or "grecaptcha" in html:
        return "recaptcha"

    # hCaptcha
    if "hcaptcha.com" in html or 'data-hcaptcha-widget-id' in html:
        return "hcaptcha"

    # PerimeterX / DataDome / Distil Networks
    if any(
        s in html
        for s in ("px-captcha", "perimeterx", "_pxAppId", "datadome", "_distil_")
    ):
        return "bot_protection"

    # Generic captcha markers in HTML attributes / class names
    generic_markers = (
        'id="captcha"',
        "id='captcha'",
        'class="captcha"',
        "class='captcha'",
        'name="captcha"',
        "captcha-container",
        "captcha_container",
        "recaptcha-checkbox",
        "please complete the security check",
        "prove you are human",
        "bot detection",
    )
    if any(m in html for m in generic_markers):
        return "generic"

    return None

from __future__ import annotations

import io
import json
import re
import time
from dataclasses import dataclass
from html import unescape
from urllib.parse import parse_qs, urljoin, urlparse
from urllib.robotparser import RobotFileParser

import httpx
import phonenumbers
from bs4 import BeautifulSoup
from pypdf import PdfReader

from app.config import get_settings
from app.services.host_suppression import clear_host_failures, is_host_suppressed, normalize_host_key, register_host_failure


EMAIL_REGEX = re.compile(r"(?i)([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})")
PHONE_REGEX = re.compile(r"(?<!\w)(\+?\d[\d\s()./\-]{6,}\d)")
CONTACT_PATH_HINTS = (
    "contact",
    "contact-us",
    "contactus",
    "about",
    "about-us",
    "reservation",
    "booking",
    "support",
    "help",
    "locations",
    "branches",
    "faq",
    "team",
    "impressum",
    "kontakt",
    "contacto",
    "contato",
    "contatti",
    "contactez",
    "iletisim",
    "iletişim",
    "联系我们",
    "聯絡我們",
)
SOCIAL_HOSTS = (
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "tiktok.com",
    "youtube.com",
    "wa.me",
    "api.whatsapp.com",
    "t.me",
    "telegram.me",
)
WHATSAPP_HOSTS = ("wa.me", "api.whatsapp.com", "chat.whatsapp.com", "whatsapp.com")
TELEGRAM_HOSTS = ("t.me", "telegram.me", "telegram.dog")
CAPTCHA_HINTS = ("captcha", "g-recaptcha", "hcaptcha", "cf-turnstile")
ASSET_SCAN_EXTENSIONS = (".js", ".mjs", ".css", ".json")
PDF_SCAN_EXTENSIONS = (".pdf",)
PHONE_CONTEXT_HINTS = (
    "phone",
    "tel",
    "telephone",
    "mobile",
    "cell",
    "call",
    "contact",
    "whatsapp",
    "telegram",
    "hotline",
    "office",
    "reservation",
    "booking",
    "support",
    "telefone",
    "telefono",
    "teléfono",
    "movil",
    "móvil",
    "celular",
    "telefon",
    "telefonnummer",
    "numara",
    "контакт",
    "телефон",
    "тел",
    "โทร",
    "เบอร์",
    "มือถือ",
    "ติดต่อ",
    "电话",
    "電話",
    "手机",
    "手機",
    "联系",
)
ANTI_BOT_HINTS = (
    "verify you're not a robot",
    "verify you are not a robot",
    "attention required",
    "please enable cookies",
    "cloudflare",
    "g-recaptcha-response",
)
NOISE_EMAIL_DOMAINS = (
    "sentry.io",
    "sentry.wixpress.com",
    "sentry-next.wixpress.com",
    "myemail.com",
)
NOISE_EMAIL_LOCALPARTS = {
    "johnsmith",
    "janedoe",
    "test",
    "example",
}
COMMON_MAILBOX_DOMAINS = {
    "gmail.com",
    "googlemail.com",
    "hotmail.com",
    "outlook.com",
    "live.com",
    "msn.com",
    "yahoo.com",
    "ymail.com",
    "icloud.com",
    "me.com",
    "aol.com",
    "proton.me",
    "protonmail.com",
}

settings = get_settings()
BROWSER_FALLBACK_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass
class CrawlPageResult:
    url: str
    title: str | None
    status_code: int | None
    emails: list[str]
    phones: list[str]
    channels: list[dict[str, str]]
    social_links: list[str]
    has_contact_form: bool
    forms: list[dict]
    error: str | None = None


@dataclass
class CrawlSiteResult:
    pages: list[CrawlPageResult]
    crawl_status: str


def build_httpx_client(
    *,
    headers: dict[str, str],
    verify: bool = True,
    proxy_url: str | None = None,
) -> httpx.Client:
    return httpx.Client(
        timeout=settings.request_timeout_seconds,
        headers=headers,
        follow_redirects=True,
        verify=verify,
        proxy=proxy_url or settings.crawler_proxy_url or None,
    )


def normalize_url(url: str) -> str:
    if not url:
        return url
    parsed = urlparse(url)
    if not parsed.scheme:
        return f"https://{url}"
    return url


def fetch_robots_allowed(base_url: str) -> bool:
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    parser = RobotFileParser()
    parser.set_url(robots_url)
    try:
        parser.read()
    except Exception:
        return True
    return parser.can_fetch(settings.user_agent, base_url)


def _is_ssl_verification_error(exc: Exception) -> bool:
    message = str(exc).upper()
    return "CERTIFICATE_VERIFY_FAILED" in message or "SELF-SIGNED CERTIFICATE" in message


def same_site_family(source_url: str, target_url: str) -> bool:
    source_host = normalize_host_key(source_url)
    target_host = normalize_host_key(target_url)
    return source_host == target_host or source_host.endswith(f".{target_host}") or target_host.endswith(f".{source_host}")


def _is_dead_host_error(exc: Exception) -> bool:
    message = str(exc).lower()
    indicators = (
        "no host connection",
        "temporary failure in name resolution",
        "name or service not known",
        "nodename nor servname provided",
        "getaddrinfo failed",
        "failed to resolve",
        "no address associated with hostname",
        "hostname nor servname provided",
        "server misbehaving",
        "dns",
        "no route to host",
    )
    return any(indicator in message for indicator in indicators)


def _is_useless_page(
    *,
    status_code: int | None,
    visible_text_length: int,
    emails: set[str],
    phones: list[str],
    channels: list[dict[str, str]],
    has_contact_form: bool,
    social_links: list[str],
) -> bool:
    if emails or phones or channels or has_contact_form:
        return False
    if social_links and visible_text_length >= settings.crawler_useless_text_threshold:
        return False
    if status_code is None:
        return True
    if status_code >= 400:
        return True
    return visible_text_length < settings.crawler_useless_text_threshold


def fetch_page(
    client: httpx.Client,
    candidate: str,
    *,
    headers: dict[str, str],
    on_request=None,
) -> tuple[httpx.Response, bool]:
    started = time.perf_counter()
    try:
        response = client.get(candidate)
        duration_ms = int((time.perf_counter() - started) * 1000)
        if on_request:
            on_request(
                method="GET",
                url=str(response.url),
                status_code=response.status_code,
                duration_ms=duration_ms,
                error=None,
            )
        return response, False
    except Exception as exc:
        duration_ms = int((time.perf_counter() - started) * 1000)
        if on_request:
            on_request(
                method="GET",
                url=candidate,
                status_code=None,
                duration_ms=duration_ms,
                error=str(exc),
            )
        if not settings.crawler_insecure_ssl_fallback or not _is_ssl_verification_error(exc):
            raise

    started = time.perf_counter()
    with build_httpx_client(headers=headers, verify=False) as insecure_client:
        response = insecure_client.get(candidate)
    duration_ms = int((time.perf_counter() - started) * 1000)
    if on_request:
        on_request(
            method="GET",
            url=str(response.url),
            status_code=response.status_code,
            duration_ms=duration_ms,
            error="ssl_verification_bypassed",
        )
    return response, True


def should_retry_with_browser_headers(response: httpx.Response, headers: dict[str, str]) -> bool:
    if headers.get("User-Agent") == BROWSER_FALLBACK_HEADERS["User-Agent"]:
        return False
    if response.status_code not in {401, 403}:
        return False
    body = response.text.lower()
    return any(hint in body for hint in ANTI_BOT_HINTS)


def extract_forms(soup: BeautifulSoup, page_url: str) -> tuple[bool, list[dict]]:
    forms = []
    for form in soup.find_all("form"):
        fields = []
        for field in form.find_all(["input", "textarea", "select"]):
            fields.append(
                {
                    "name": field.get("name"),
                    "type": field.get("type", field.name),
                    "placeholder": field.get("placeholder"),
                    "required": field.has_attr("required"),
                    "value": field.get("value"),
                }
            )
        html_blob = str(form).lower()
        forms.append(
            {
                "action_url": urljoin(page_url, form.get("action", "") or ""),
                "method": (form.get("method") or "get").lower(),
                "has_captcha": any(hint in html_blob for hint in CAPTCHA_HINTS),
                "fields": fields,
            }
        )
    return bool(forms), forms


def decode_cloudflare_email(encoded_value: str | None) -> str | None:
    if not encoded_value:
        return None
    try:
        key = int(encoded_value[:2], 16)
        decoded = "".join(
            chr(int(encoded_value[index:index + 2], 16) ^ key)
            for index in range(2, len(encoded_value), 2)
        )
    except Exception:
        return None
    return decoded


def is_noise_email(email: str) -> bool:
    normalized = email.strip().lower()
    localpart, _, _ = normalized.partition("@")
    return any(normalized.endswith(f"@{domain}") for domain in NOISE_EMAIL_DOMAINS) or localpart in NOISE_EMAIL_LOCALPARTS


def is_asset_candidate_email(email: str, page_url: str) -> bool:
    normalized = email.strip().lower()
    _, _, domain = normalized.partition("@")
    page_host = urlparse(page_url).netloc.lower().removeprefix("www.")
    if not domain:
        return False
    if domain == page_host or domain.endswith(f".{page_host}") or page_host.endswith(f".{domain}"):
        return True
    return domain in COMMON_MAILBOX_DOMAINS


def _extract_emails_from_text(text: str) -> set[str]:
    return {match.group(1).lower() for match in EMAIL_REGEX.finditer(unescape(text))}


def extract_emails(soup: BeautifulSoup) -> list[str]:
    extraction_soup = BeautifulSoup(str(soup), "html.parser")
    for tag in extraction_soup(["script", "style", "noscript", "template"]):
        tag.decompose()

    candidates: set[str] = set()
    visible_text = unescape(extraction_soup.get_text(" ", strip=True))
    candidates.update(_extract_emails_from_text(visible_text))

    for link in extraction_soup.find_all("a", href=True):
        href = unescape(link["href"]).strip()
        if href.lower().startswith("mailto:"):
            candidates.update(_extract_emails_from_text(href))
        link_text = unescape(link.get_text(" ", strip=True))
        candidates.update(_extract_emails_from_text(link_text))

    for protected_email in extraction_soup.select("a.__cf_email__, span.__cf_email__"):
        decoded = decode_cloudflare_email(protected_email.get("data-cfemail"))
        if decoded:
            candidates.add(decoded.lower())

    return sorted(email for email in candidates if not is_noise_email(email))


def normalize_phone_number(value: str, default_region_code: str | None = None) -> str | None:
    candidate = re.sub(r"(?i)(ext|extension|x)\s*\d+$", "", unescape(value or "")).strip()
    if not candidate:
        return None
    lowered = candidate.lower()
    parsed_candidate = urlparse(candidate)
    if parsed_candidate.scheme in {"http", "https"} or parsed_candidate.netloc:
        return None
    if "www." in lowered or "://" in lowered:
        return None
    if sum(char.isalpha() for char in candidate) > 3:
        return None
    if len(candidate) > 64:
        return None
    if re.search(r"\b\d{1,2}[.:]\d{2}\b", lowered):
        return None

    default_region = (default_region_code or "ZZ").upper()
    try:
        parsed = phonenumbers.parse(candidate, default_region)
        if phonenumbers.is_possible_number(parsed) and (
            phonenumbers.is_valid_number(parsed) or parsed.country_code
        ):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        pass

    digits = re.sub(r"\D", "", candidate)
    if len(digits) < 9 or len(digits) > 15:
        return None
    if digits.startswith("00"):
        digits = digits[2:]
    if candidate.startswith("+") or digits:
        return f"+{digits}"
    return None


def is_noise_phone(phone: str) -> bool:
    digits = re.sub(r"\D", "", phone)
    if len(set(digits)) == 1:
        return True
    return digits in {"12345678", "123456789", "1234567890"}


def _context_has_phone_hint(text: str, start: int, end: int) -> bool:
    window = text[max(0, start - 40):min(len(text), end + 40)].lower()
    return any(hint in window for hint in PHONE_CONTEXT_HINTS)


def extract_phones(soup: BeautifulSoup, default_region_code: str | None = None) -> list[str]:
    extraction_soup = BeautifulSoup(str(soup), "html.parser")
    for tag in extraction_soup(["script", "style", "noscript", "template"]):
        tag.decompose()

    ordered: list[tuple[str, str]] = []
    seen: set[str] = set()

    def add_candidate(raw_value: str, *, require_context: bool = False, source_text: str | None = None, start: int = 0, end: int = 0) -> None:
        if require_context and source_text is not None and not _context_has_phone_hint(source_text, start, end):
            return
        normalized = normalize_phone_number(raw_value, default_region_code=default_region_code)
        if not normalized or normalized in seen or is_noise_phone(normalized):
            return
        seen.add(normalized)
        ordered.append((normalized, raw_value.strip()))

    visible_text = unescape(extraction_soup.get_text(" ", strip=True))
    try:
        matcher = phonenumbers.PhoneNumberMatcher(visible_text, (default_region_code or "ZZ").upper())
        for match in matcher:
            add_candidate(match.raw_string, require_context=True, source_text=visible_text, start=match.start, end=match.end)
    except Exception:
        for match in PHONE_REGEX.finditer(visible_text):
            add_candidate(match.group(1), require_context=True, source_text=visible_text, start=match.start(), end=match.end())

    for link in extraction_soup.find_all("a", href=True):
        href = unescape(link["href"]).strip()
        if href.lower().startswith("tel:"):
            add_candidate(href.split(":", 1)[1])
        if any(host in href.lower() for host in WHATSAPP_HOSTS):
            digits = "".join(re.findall(r"\d+", href))
            if digits:
                add_candidate(f"+{digits}")
        link_text = unescape(link.get_text(" ", strip=True))
        if any(hint in link_text.lower() for hint in PHONE_CONTEXT_HINTS):
            for match in PHONE_REGEX.finditer(link_text):
                add_candidate(match.group(1))

    return [raw for _, raw in ordered]


def normalize_telegram_value(value: str) -> str | None:
    candidate = unescape(value or "").strip()
    if not candidate:
        return None
    lowered = candidate.lower()
    if any(host in lowered for host in TELEGRAM_HOSTS):
        parsed = urlparse(candidate)
        parts = [part for part in parsed.path.split("/") if part]
        if parts:
            return f"@{parts[-1].lower().lstrip('@')}"
    if candidate.startswith("@"):
        return f"@{candidate[1:].lower()}"
    return None


def extract_channels(soup: BeautifulSoup, page_url: str, default_region_code: str | None = None) -> list[dict[str, str]]:
    channels: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def add_channel(channel_type: str, raw_value: str, normalized_value: str) -> None:
        key = (channel_type, normalized_value)
        if key in seen:
            return
        seen.add(key)
        channels.append(
            {
                "channel_type": channel_type,
                "channel_value": raw_value,
                "normalized_value": normalized_value,
                "source_page_url": page_url,
            }
        )

    for link in soup.find_all("a", href=True):
        href = unescape(link["href"]).strip()
        lowered = href.lower()
        if any(host in lowered for host in WHATSAPP_HOSTS):
            parsed = urlparse(href)
            digits = "".join(re.findall(r"\d+", href))
            if not digits and parsed.query:
                query = parse_qs(parsed.query)
                digits = "".join(re.findall(r"\d+", "".join(query.get("phone", []))))
            normalized = normalize_phone_number(f"+{digits}" if digits else href, default_region_code=default_region_code)
            if normalized:
                add_channel("whatsapp", href, normalized)
        if any(host in lowered for host in TELEGRAM_HOSTS):
            normalized = normalize_telegram_value(href)
            if normalized:
                add_channel("telegram", href, normalized)
    return channels


def _walk_jsonld(node, page_url: str, default_region_code: str | None, emails: set[str], phones: set[str], channels: list[dict[str, str]]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            lowered_key = str(key).lower()
            if lowered_key == "email" and isinstance(value, str):
                emails.update(_extract_emails_from_text(value))
            elif lowered_key == "telephone" and isinstance(value, str):
                normalized = normalize_phone_number(value, default_region_code=default_region_code)
                if normalized and not is_noise_phone(normalized):
                    phones.add(value.strip())
            elif lowered_key == "sameas":
                values = value if isinstance(value, list) else [value]
                for item in values:
                    if not isinstance(item, str):
                        continue
                    channels.extend(
                        extract_channels(
                            BeautifulSoup(f'<a href="{item}"></a>', "html.parser"),
                            page_url,
                            default_region_code=default_region_code,
                        )
                    )
            else:
                _walk_jsonld(value, page_url, default_region_code, emails, phones, channels)
    elif isinstance(node, list):
        for item in node:
            _walk_jsonld(item, page_url, default_region_code, emails, phones, channels)
    elif isinstance(node, str):
        emails.update(_extract_emails_from_text(node))
        normalized = normalize_phone_number(node, default_region_code=default_region_code)
        if normalized and not is_noise_phone(normalized):
            phones.add(node.strip())


def extract_structured_contacts(
    soup: BeautifulSoup,
    page_url: str,
    default_region_code: str | None = None,
) -> tuple[list[str], list[str], list[dict[str, str]]]:
    emails: set[str] = set()
    phones: set[str] = set()
    channels: list[dict[str, str]] = []
    for script in soup.find_all("script", attrs={"type": lambda value: value and "ld+json" in value.lower()}):
        raw_value = script.string or script.get_text(" ", strip=True)
        if not raw_value:
            continue
        try:
            data = json.loads(raw_value)
        except Exception:
            continue
        _walk_jsonld(data, page_url, default_region_code, emails, phones, channels)

    deduped_channels: dict[tuple[str, str], dict[str, str]] = {}
    for channel in channels:
        key = (channel["channel_type"], channel["normalized_value"])
        deduped_channels.setdefault(key, channel)
    return sorted(email for email in emails if not is_noise_email(email)), sorted(phones), list(deduped_channels.values())


def iter_same_origin_assets(soup: BeautifulSoup, page_url: str) -> list[str]:
    page_host = urlparse(page_url).netloc.lower().removeprefix("www.")
    assets: list[str] = []
    seen: set[str] = set()

    def consider(candidate: str | None, extensions: tuple[str, ...]) -> None:
        if not candidate:
            return
        absolute = urljoin(page_url, candidate)
        parsed = urlparse(absolute)
        host = parsed.netloc.lower().removeprefix("www.")
        if not host or host != page_host:
            return
        path = parsed.path.lower()
        if not any(path.endswith(ext) for ext in extensions):
            return
        if absolute in seen:
            return
        seen.add(absolute)
        assets.append(absolute)

    for tag in soup.find_all("script", src=True):
        consider(tag.get("src"), ASSET_SCAN_EXTENSIONS)
    for tag in soup.find_all("link", href=True):
        rel = " ".join(tag.get("rel", [])).lower()
        if rel in {"stylesheet", "preload", "modulepreload"} or "stylesheet" in rel or "preload" in rel:
            consider(tag.get("href"), ASSET_SCAN_EXTENSIONS)
    return assets[:4]


def iter_same_origin_pdf_links(soup: BeautifulSoup, page_url: str) -> list[str]:
    page_host = urlparse(page_url).netloc.lower().removeprefix("www.")
    pdfs: list[str] = []
    seen: set[str] = set()
    for link in soup.find_all("a", href=True):
        absolute = urljoin(page_url, link["href"])
        parsed = urlparse(absolute)
        host = parsed.netloc.lower().removeprefix("www.")
        if host != page_host:
            continue
        if not any(parsed.path.lower().endswith(ext) for ext in PDF_SCAN_EXTENSIONS):
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        pdfs.append(absolute)
    return pdfs[:2]


def extract_emails_from_assets(
    client: httpx.Client,
    soup: BeautifulSoup,
    page_url: str,
    *,
    headers: dict[str, str],
    on_request=None,
) -> list[str]:
    candidates: set[str] = set()
    for asset_url in iter_same_origin_assets(soup=soup, page_url=page_url):
        try:
            response, _ = fetch_page(client, asset_url, headers=headers, on_request=on_request)
        except Exception:
            continue
        if response.status_code >= 400:
            continue
        content_type = (response.headers.get("content-type") or "").lower()
        if "javascript" not in content_type and "json" not in content_type and "css" not in content_type and "text" not in content_type:
            continue
        candidates.update(_extract_emails_from_text(response.text))
    return sorted(
        email
        for email in candidates
        if not is_noise_email(email) and is_asset_candidate_email(email, page_url)
    )


def extract_pdf_page_results(
    client: httpx.Client,
    soup: BeautifulSoup,
    page_url: str,
    *,
    headers: dict[str, str],
    on_request=None,
    default_region_code: str | None = None,
) -> list[CrawlPageResult]:
    pdf_pages: list[CrawlPageResult] = []
    for pdf_url in iter_same_origin_pdf_links(soup, page_url):
        try:
            response, _ = fetch_page(client, pdf_url, headers=headers, on_request=on_request)
        except Exception as exc:
            pdf_pages.append(
                CrawlPageResult(
                    url=pdf_url,
                    title="PDF",
                    status_code=None,
                    emails=[],
                    phones=[],
                    channels=[],
                    social_links=[],
                    has_contact_form=False,
                    forms=[],
                    error=str(exc),
                )
            )
            continue
        if response.status_code >= 400:
            continue
        if "pdf" not in (response.headers.get("content-type") or "").lower() and not pdf_url.lower().endswith(".pdf"):
            continue
        try:
            reader = PdfReader(io.BytesIO(response.content))
            text = " ".join(page.extract_text() or "" for page in reader.pages)
        except Exception as exc:
            pdf_pages.append(
                CrawlPageResult(
                    url=pdf_url,
                    title="PDF",
                    status_code=response.status_code,
                    emails=[],
                    phones=[],
                    channels=[],
                    social_links=[],
                    has_contact_form=False,
                    forms=[],
                    error=f"pdf_parse_failed: {exc}",
                )
            )
            continue

        email_values = sorted(email for email in _extract_emails_from_text(text) if not is_noise_email(email))
        phone_values: list[str] = []
        try:
            matcher = phonenumbers.PhoneNumberMatcher(text, (default_region_code or "ZZ").upper())
            for match in matcher:
                normalized = normalize_phone_number(match.raw_string, default_region_code=default_region_code)
                if normalized and not is_noise_phone(normalized):
                    phone_values.append(match.raw_string.strip())
        except Exception:
            for match in PHONE_REGEX.finditer(text):
                normalized = normalize_phone_number(match.group(1), default_region_code=default_region_code)
                if normalized and not is_noise_phone(normalized):
                    phone_values.append(match.group(1).strip())

        pdf_soup = BeautifulSoup(text, "html.parser")
        channels = extract_channels(pdf_soup, pdf_url, default_region_code=default_region_code)
        pdf_pages.append(
            CrawlPageResult(
                url=pdf_url,
                title="PDF",
                status_code=response.status_code,
                emails=email_values[: settings.max_emails_per_company],
                phones=phone_values,
                channels=channels,
                social_links=[],
                has_contact_form=False,
                forms=[],
            )
        )
    return pdf_pages


def should_scan_assets(soup: BeautifulSoup, page_url: str) -> bool:
    visible_text = unescape(soup.get_text(" ", strip=True))
    if len(visible_text) >= 300:
        return False
    shell_ids = {"root", "app", "__next"}
    if any(tag.get("id") in shell_ids for tag in soup.find_all(True)):
        return True
    return bool(iter_same_origin_assets(soup=soup, page_url=page_url))


def should_browser_escalate(result: CrawlSiteResult) -> bool:
    if not result.pages:
        return result.crawl_status in {"blocked_by_robots", "failed"}

    has_contacts = any(page.emails or page.phones or page.channels or page.has_contact_form for page in result.pages)
    if has_contacts:
        return False

    if any((page.status_code or 0) in {401, 403, 429, 503} for page in result.pages):
        return True
    if any(page.error == "anti_bot_challenge" for page in result.pages):
        return True
    if any(page.error == "js_shell" for page in result.pages):
        return True
    if any(page.error == "cross_domain_redirect_after_ssl_fallback" for page in result.pages):
        return False
    return result.crawl_status in {"blocked_by_robots", "robots_bypassed"}


def crawl_site(
    website_url: str,
    on_request=None,
    proxy_url: str | None = None,
    default_region_code: str | None = None,
) -> CrawlSiteResult:
    website_url = normalize_url(website_url)
    if is_host_suppressed(website_url):
        if on_request:
            on_request(
                request_kind="suppressed_host",
                method="EVENT",
                url=website_url,
                status_code=None,
                duration_ms=0,
                error="suppressed_host",
            )
        return CrawlSiteResult(pages=[], crawl_status="suppressed_host")
    robots_blocked = not fetch_robots_allowed(website_url)
    if robots_blocked and not settings.crawler_ignore_robots:
        return CrawlSiteResult(pages=[], crawl_status="blocked_by_robots")

    base = urlparse(website_url)
    candidates = [website_url]
    for path in CONTACT_PATH_HINTS:
        candidates.append(urljoin(website_url, f"/{path}"))

    seen = set()
    pages: list[CrawlPageResult] = []
    headers = {"User-Agent": settings.user_agent}
    useless_attempts = 0
    useful_found = False

    with build_httpx_client(headers=headers, proxy_url=proxy_url) as client:
        for candidate in candidates[: settings.max_pages_per_site]:
            if candidate in seen:
                continue
            seen.add(candidate)
            try:
                response, insecure_fallback = fetch_page(client, candidate, headers=headers, on_request=on_request)
                if should_retry_with_browser_headers(response, headers):
                    with build_httpx_client(
                        headers=BROWSER_FALLBACK_HEADERS,
                        verify=not insecure_fallback,
                        proxy_url=proxy_url,
                    ) as browser_client:
                        response, insecure_fallback = fetch_page(
                            browser_client,
                            candidate,
                            headers=BROWSER_FALLBACK_HEADERS,
                            on_request=on_request,
                        )
                if insecure_fallback and not same_site_family(candidate, str(response.url)):
                    pages.append(
                        CrawlPageResult(
                            url=str(response.url),
                            title=None,
                            status_code=response.status_code,
                            emails=[],
                            phones=[],
                            channels=[],
                            social_links=[],
                            has_contact_form=False,
                            forms=[],
                            error="cross_domain_redirect_after_ssl_fallback",
                        )
                    )
                    continue

                soup = BeautifulSoup(response.text, "html.parser")
                title = soup.title.text.strip() if soup.title and soup.title.text else None
                emails = set(extract_emails(soup))
                phones = list(extract_phones(soup, default_region_code=default_region_code))
                channels = extract_channels(soup, str(response.url), default_region_code=default_region_code)
                visible_text = unescape(soup.get_text(" ", strip=True))
                visible_text_length = len(visible_text)

                structured_emails, structured_phones, structured_channels = extract_structured_contacts(
                    soup,
                    str(response.url),
                    default_region_code=default_region_code,
                )
                emails.update(structured_emails)
                phones.extend(structured_phones)
                channels.extend(structured_channels)

                if not emails and should_scan_assets(soup, str(response.url)):
                    emails.update(
                        extract_emails_from_assets(
                            client=client,
                            soup=soup,
                            page_url=str(response.url),
                            headers=headers,
                            on_request=on_request,
                        )
                    )

                social_links = []
                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    absolute = urljoin(candidate, href)
                    parsed = urlparse(absolute)
                    if parsed.netloc.endswith(base.netloc) and any(hint in parsed.path.lower() for hint in CONTACT_PATH_HINTS):
                        if absolute not in seen and len(candidates) < settings.max_pages_per_site:
                            candidates.append(absolute)
                    if any(host in absolute for host in SOCIAL_HOSTS):
                        social_links.append(absolute)

                has_contact_form, forms = extract_forms(soup=soup, page_url=str(response.url))
                page_error = None
                if (
                    response.status_code == 200
                    and not emails
                    and not phones
                    and not channels
                    and not has_contact_form
                    and should_scan_assets(soup, str(response.url))
                ):
                    page_error = "js_shell"
                    if on_request:
                        on_request(
                            request_kind="js_shell",
                            method="EVENT",
                            url=str(response.url),
                            status_code=response.status_code,
                            duration_ms=0,
                            error="js_shell",
                        )
                deduped_channels: dict[tuple[str, str], dict[str, str]] = {}
                for channel in channels:
                    key = (channel["channel_type"], channel["normalized_value"])
                    deduped_channels.setdefault(key, channel)

                pages.append(
                    CrawlPageResult(
                        url=str(response.url),
                        title=title,
                        status_code=response.status_code,
                        emails=sorted(emails)[: settings.max_emails_per_company],
                        phones=sorted(set(phones)),
                        channels=list(deduped_channels.values()),
                        social_links=sorted(set(social_links)),
                        has_contact_form=has_contact_form,
                        forms=forms,
                        error=page_error,
                    )
                )
                pages.extend(
                    extract_pdf_page_results(
                        client=client,
                        soup=soup,
                        page_url=str(response.url),
                        headers=headers,
                        on_request=on_request,
                        default_region_code=default_region_code,
                    )
                )
                if _is_useless_page(
                    status_code=response.status_code,
                    visible_text_length=visible_text_length,
                    emails=emails,
                    phones=phones,
                    channels=list(deduped_channels.values()),
                    has_contact_form=has_contact_form,
                    social_links=social_links,
                ):
                    useless_attempts += 1
                else:
                    useful_found = True
                    useless_attempts = 0
                    clear_host_failures(website_url)
                if not useful_found and useless_attempts >= settings.crawler_early_stop_core_attempts:
                    register_host_failure(website_url)
                    if on_request:
                        on_request(
                            request_kind="early_stopped",
                            method="EVENT",
                            url=website_url,
                            status_code=response.status_code,
                            duration_ms=0,
                            error=f"useless_after_{useless_attempts}",
                        )
                    break
            except Exception as exc:
                pages.append(
                    CrawlPageResult(
                        url=candidate,
                        title=None,
                        status_code=None,
                        emails=[],
                        phones=[],
                        channels=[],
                        social_links=[],
                        has_contact_form=False,
                        forms=[],
                        error=str(exc),
                    )
                )
                if _is_dead_host_error(exc):
                    register_host_failure(website_url)
                    break
                useless_attempts += 1
                if useless_attempts >= settings.crawler_early_stop_core_attempts:
                    register_host_failure(website_url)
                    if on_request:
                        on_request(
                            request_kind="early_stopped",
                            method="EVENT",
                            url=website_url,
                            status_code=None,
                            duration_ms=0,
                            error=f"exception_after_{useless_attempts}",
                        )
                    break

    crawl_status = "completed" if any(page.status_code for page in pages) else "failed"
    if crawl_status == "completed" and robots_blocked:
        crawl_status = "robots_bypassed"
    return CrawlSiteResult(pages=pages, crawl_status=crawl_status)

"""Document ingestion: HTML, PDF, Excel, DOCX, JSON."""

from __future__ import annotations

import io
import logging
import re
import time
from typing import Any
from urllib.parse import urljoin, urlparse

from .config import CrawlerConfig

logger = logging.getLogger(__name__)

_TIKTOKEN_AVAILABLE = False
try:
    import tiktoken  # noqa: F401
    _TIKTOKEN_AVAILABLE = True
except ImportError:
    pass


def _count_tokens_approx(text: str) -> int:
    """Approximate token count (4 chars per token)."""
    return len(text) // 4


class DocumentIngester:
    """Fetches and parses documents in various formats."""

    SUPPORTED_FORMATS = {"html", "pdf", "xlsx", "xls", "docx", "json"}

    def __init__(self, config: CrawlerConfig) -> None:
        self.config = config
        self._rate_delay = 1.0 / max(config.requests_per_second, 0.1)

    def ingest(self, url: str) -> dict[str, Any]:
        """Fetch and parse a document.

        Returns:
            Dict with keys: url, content (str), format, metadata (dict), links (list[str]).
        """
        try:
            import requests  # noqa: PLC0415
        except ImportError:
            logger.error("requests library not available")
            return self._empty_result(url)

        time.sleep(self._rate_delay)

        try:
            headers = {
                "User-Agent": (
                    "AbatementCrawler/0.1 (carbon data research; "
                    "contact: research@example.com)"
                )
            }
            response = requests.get(
                url,
                headers=headers,
                timeout=self.config.pdf_timeout_seconds,
                allow_redirects=True,
            )
            response.raise_for_status()
        except Exception as exc:
            logger.warning("Failed to fetch %s: %s", url, exc)
            return self._empty_result(url)

        content_type = response.headers.get("content-type", "").lower()
        raw_bytes = response.content
        fmt = self._detect_format(url, content_type)

        metadata: dict[str, Any] = {
            "content_type": content_type,
            "status_code": response.status_code,
            "final_url": response.url,
        }

        if fmt == "pdf":
            text = self._ingest_pdf(url, raw_bytes)
        elif fmt in ("xlsx", "xls"):
            text = self._ingest_excel(url, raw_bytes)
        elif fmt == "docx":
            text = self._ingest_docx(url, raw_bytes)
        elif fmt == "json":
            text = self._ingest_json(url, raw_bytes)
        else:
            text = self._ingest_html(url, raw_bytes)
            fmt = "html"

        links = self._extract_links(url, response.text if fmt == "html" else "")

        return {
            "url": url,
            "content": text,
            "format": fmt,
            "metadata": metadata,
            "links": links,
        }

    def _detect_format(self, url: str, content_type: str) -> str:
        """Detect document format from URL path and content-type."""
        path = urlparse(url).path.lower()
        if path.endswith(".pdf") or "pdf" in content_type:
            return "pdf"
        if path.endswith((".xlsx", ".xls")) or "spreadsheet" in content_type or "excel" in content_type:
            return "xlsx"
        if path.endswith(".docx") or "wordprocessing" in content_type:
            return "docx"
        if path.endswith(".json") or "json" in content_type:
            return "json"
        return "html"

    def _ingest_html(self, url: str, content: bytes) -> str:
        """Extract main text from HTML using trafilatura, fallback to BeautifulSoup."""
        text = ""
        try:
            import trafilatura  # noqa: PLC0415

            decoded = content.decode("utf-8", errors="replace")
            extracted = trafilatura.extract(
                decoded,
                include_tables=True,
                include_links=False,
                no_fallback=False,
            )
            if extracted:
                return extracted
        except ImportError:
            pass
        except Exception as exc:
            logger.debug("trafilatura failed for %s: %s", url, exc)

        # Fallback: BeautifulSoup
        try:
            from bs4 import BeautifulSoup  # noqa: PLC0415

            soup = BeautifulSoup(content, "html.parser")
            for tag in soup(["script", "style", "nav", "footer", "header"]):
                tag.decompose()
            text = soup.get_text(separator="\n", strip=True)
        except ImportError:
            text = content.decode("utf-8", errors="replace")
        except Exception as exc:
            logger.debug("BeautifulSoup failed for %s: %s", url, exc)
            text = content.decode("utf-8", errors="replace")

        return text

    def _ingest_pdf(self, url: str, content: bytes) -> str:
        """Extract text from PDF using PyMuPDF, fallback to pdfplumber."""
        try:
            import fitz  # PyMuPDF  # noqa: PLC0415

            doc = fitz.open(stream=content, filetype="pdf")
            pages = []
            for page in doc:
                pages.append(page.get_text())
            doc.close()
            return "\n".join(pages)
        except ImportError:
            pass
        except Exception as exc:
            logger.debug("PyMuPDF failed for %s: %s", url, exc)

        try:
            import pdfplumber  # noqa: PLC0415

            with pdfplumber.open(io.BytesIO(content)) as pdf:
                pages = [page.extract_text() or "" for page in pdf.pages]
            return "\n".join(pages)
        except ImportError:
            pass
        except Exception as exc:
            logger.debug("pdfplumber failed for %s: %s", url, exc)

        logger.warning("Could not extract PDF text from %s", url)
        return ""

    def _ingest_excel(self, url: str, content: bytes) -> str:
        """Extract text from Excel using pandas."""
        try:
            import pandas as pd  # noqa: PLC0415

            dfs = pd.read_excel(io.BytesIO(content), sheet_name=None)
            parts = []
            for sheet_name, df in dfs.items():
                parts.append(f"Sheet: {sheet_name}")
                parts.append(df.to_csv(index=False))
            return "\n".join(parts)
        except ImportError:
            logger.warning("pandas not available for Excel ingestion")
            return ""
        except Exception as exc:
            logger.warning("Excel ingestion failed for %s: %s", url, exc)
            return ""

    def _ingest_docx(self, url: str, content: bytes) -> str:
        """Extract text from DOCX using python-docx."""
        try:
            from docx import Document  # noqa: PLC0415

            doc = Document(io.BytesIO(content))
            return "\n".join(para.text for para in doc.paragraphs)
        except ImportError:
            logger.warning("python-docx not available for DOCX ingestion")
            return ""
        except Exception as exc:
            logger.warning("DOCX ingestion failed for %s: %s", url, exc)
            return ""

    def _ingest_json(self, url: str, content: bytes) -> str:
        """Convert JSON content to readable text."""
        import json  # noqa: PLC0415

        try:
            data = json.loads(content)
            return json.dumps(data, indent=2, ensure_ascii=False)
        except Exception as exc:
            logger.warning("JSON ingestion failed for %s: %s", url, exc)
            return content.decode("utf-8", errors="replace")

    def _extract_links(self, url: str, html_text: str) -> list[str]:
        """Extract outbound hyperlinks and DOI references from HTML."""
        if not html_text:
            return []

        links: list[str] = []
        try:
            from bs4 import BeautifulSoup  # noqa: PLC0415

            soup = BeautifulSoup(html_text, "html.parser")
            for tag in soup.find_all("a", href=True):
                href = tag["href"].strip()
                if href.startswith(("http://", "https://")):
                    links.append(href)
                elif href.startswith("/"):
                    links.append(urljoin(url, href))
        except ImportError:
            # Regex fallback
            pattern = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)
            for match in pattern.finditer(html_text):
                href = match.group(1)
                if href.startswith(("http://", "https://")):
                    links.append(href)

        # Also capture DOI links
        doi_pattern = re.compile(r"(https?://doi\.org/[^\s\"'<>]+)")
        for match in doi_pattern.finditer(html_text):
            doi_url = match.group(1)
            if doi_url not in links:
                links.append(doi_url)

        return list(dict.fromkeys(links))  # deduplicate preserving order

    def chunk_text(self, text: str, max_tokens: int = 8000) -> list[str]:
        """Split text into chunks of at most max_tokens tokens.

        Uses approximate token counting (4 chars per token).
        Tries to split on paragraph boundaries.
        """
        if not text:
            return []

        # Approximate chars per chunk
        max_chars = max_tokens * 4

        if len(text) <= max_chars:
            return [text]

        paragraphs = re.split(r"\n{2,}", text)
        chunks: list[str] = []
        current_parts: list[str] = []
        current_len = 0

        for para in paragraphs:
            para_len = len(para)
            if current_len + para_len > max_chars and current_parts:
                chunks.append("\n\n".join(current_parts))
                current_parts = [para]
                current_len = para_len
            else:
                current_parts.append(para)
                current_len += para_len + 2  # +2 for separator

        if current_parts:
            chunks.append("\n\n".join(current_parts))

        return chunks

    @staticmethod
    def _empty_result(url: str) -> dict[str, Any]:
        return {
            "url": url,
            "content": "",
            "format": "unknown",
            "metadata": {},
            "links": [],
        }

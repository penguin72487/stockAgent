"""Server-owned dashboard audiences; browser-supplied identity is never authority.

The proxy selects a separate loopback listener after LAN/WireGuard verification.
Each listener has an immutable audience and its own encoded response cache.
"""
from __future__ import annotations

from html.parser import HTMLParser
import re
from typing import Final

PUBLIC_PAGES: Final[tuple[str, ...]] = ("overview", "tw-day-trade", "tw-overnight")
AUDIENCES: Final[frozenset[str]] = frozenset({"public", "member", "owner"})
SHARED_ASSETS: Final[frozenset[str]] = frozenset({
    "/public.css", "/public.js", "/dashboard-core.css", "/dashboard-core.js",
    "/dashboard-responsive.css", "/dashboard-acquisition.css", "/dashboard-acquisition.js",
    "/time-axis.js", "/feature-page-constraints.js", "/feature-page-contract.js",
    "/vendor/uplot/uPlot.iife.min.js", "/vendor/uplot/uPlot.min.css", "/robots.txt",
})
_VOID = frozenset({"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"})


def _audience(*, owner: bool = False, audience: str | None = None) -> str:
    resolved = audience if audience is not None else ("owner" if owner else "public")
    if resolved not in AUDIENCES:
        raise ValueError("unknown dashboard audience")
    return resolved


def permitted_path(path: str, *, owner: bool = False, audience: str | None = None) -> bool:
    if _audience(owner=owner, audience=audience) != "public":
        return True  # The existing exact route/asset registry still rejects unknown paths.
    if path in {"/", "/index.html", "/api/overview", "/api/access", "/healthz", "/favicon.ico"} | SHARED_ASSETS:
        return True
    return path in {"/tw-day-trade", "/tw-overnight"} or path.startswith(("/tw-day-trade/", "/tw-overnight/"))


def access_description(*, owner: bool = False, audience: str | None = None) -> dict[str, object]:
    resolved = _audience(owner=owner, audience=audience)
    return {"schema_version": 1, "audience": resolved,
            "pages": list(PUBLIC_PAGES) if resolved == "public" else "all",
            "private_prices": resolved == "owner"}


class _PublicHTML(HTMLParser):
    """Drop source-owned private elements before bytes reach a public client."""
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.parts: list[str] = []
        self.skipped = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self.skipped:
            if tag not in _VOID:
                self.skipped += 1
            return
        if any(key == "data-owner-only" for key, _ in attrs):
            self.skipped = 0 if tag in _VOID else 1
            return
        self.parts.append(self.get_starttag_text())

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if not self.skipped and not any(key == "data-owner-only" for key, _ in attrs):
            self.parts.append(self.get_starttag_text())

    def handle_endtag(self, tag: str) -> None:
        if self.skipped:
            self.skipped -= 1
        else:
            self.parts.append(f"</{tag}>")

    def handle_data(self, data: str) -> None:
        if not self.skipped:
            self.parts.append(data)

    def handle_entityref(self, name: str) -> None:
        self.handle_data(f"&{name};")

    def handle_charref(self, name: str) -> None:
        self.handle_data(f"&#{name};")

    def handle_comment(self, data: str) -> None:
        if not self.skipped:
            self.parts.append(f"<!--{data}-->")

    def handle_decl(self, decl: str) -> None:
        self.parts.append(f"<!{decl}>")


def project_html(body: bytes, *, owner: bool = False, audience: str | None = None,
                 core_version: str | None = None) -> bytes:
    resolved = _audience(owner=owner, audience=audience)
    if core_version is not None and not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", core_version):
        raise ValueError("invalid dashboard core asset version")
    text = body.decode("utf-8")
    if "</head>" not in text:
        return body
    if resolved == "public":
        parser = _PublicHTML()
        parser.feed(text)
        parser.close()
        text = "".join(parser.parts)
        text = text.replace('id="page-title">資料與抓取總覽', 'id="page-title">台股策略總覽')
        text = text.replace("查看所有來源的抓取狀態、進度與配額；策略模擬另列於下方。公開唯讀，不呼叫供應商 API。",
                            "查看台股當沖與隔日沖的策略狀態、訊號及模擬交易。公開唯讀，報價依現有公開設定顯示。")
    # The gateway supplies the actual shared asset content identity, including
    # unchanged page shells. A fixed override would pin every future release
    # to an old immutable browser/CDN entry despite a page's new version.
    if core_version is not None:
        text = re.sub(r'(dashboard-core\.js\?v=)[A-Za-z0-9_.-]+',
                      lambda match: match[1] + core_version, text)
    text = text.replace("</head>", f'<meta name="stockagent-audience" content="{resolved}">\n</head>', 1)
    return text.encode("utf-8")

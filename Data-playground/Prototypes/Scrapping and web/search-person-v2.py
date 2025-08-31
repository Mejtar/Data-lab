#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
person_search.py — Web scraping y búsqueda de personas, optimizado.
Compatibilidad: Python 3.8+

Características:
- Async con aiohttp + reuso de conexiones
- Rate limiting con asyncio.Semaphore
- Timeouts y retries con backoff exponencial + jitter
- Validación/sanitización de input
- Respeto básico de robots.txt (best-effort)
- Extracción robusta con BeautifulSoup (html.parser por defecto)
- Persistencia incremental a CSV o SQLite (opcional)
- Logs estructurados
- CLI con flags; alias para el typo `busqueda_intesiva`
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode, urlparse, urlunparse

import aiohttp
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup  # pip install beautifulsoup4
import urllib.robotparser as robotparser
import logging

# ----------------------------
# Configuración y constantes
# ----------------------------

DEFAULT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

SAFE_CSV_PREFIXES = ('+', '-', '=', '@', '\t')  # Para prevenir CSV injection


@dataclass
class Target:
    """Endpoint de búsqueda con plantilla de URL y selectores."""
    name: str
    url_template: str  # e.g. "https://example.com/search?q={query}"
    item_selector: str  # CSS-like simple (tag/class/id -> usamos BS4 find_all)
    fields: Dict[str, str]  # nombre_campo -> selector relativo
    # Ejemplo: {"title": ".result-title", "link": "a[href]"}


@dataclass
class ScraperConfig:
    """Parámetros de scraping y búsqueda."""
    concurrency: int = 8
    connect_timeout_s: float = 5.0
    read_timeout_s: float = 10.0
    total_timeout_s: float = 20.0
    max_retries: int = 3
    backoff_base: float = 0.5  # segundos
    user_agents: List[str] = field(default_factory=lambda: [DEFAULT_UA])
    obey_robots: bool = True
    rate_delay_s: float = 0.0  # si necesitas throttling por target
    output_csv: Optional[str] = "results.csv"
    output_sqlite: Optional[str] = None  # (no implementado aquí por brevedad)
    normalize_whitespace: bool = True
    busqueda_intensiva: bool = False  # alias correcto
    # Alias antiguo mal escrito; si llega True, lo mapeamos:
    busqueda_intesiva: Optional[bool] = None  # typo intencional para compat.


# ----------------------------
# Utilidades
# ----------------------------

def sanitize_query(q: str) -> str:
    """Valida y normaliza la consulta."""
    if q is None:
        raise ValueError("query no puede ser None")
    q = q.strip()
    if not q:
        raise ValueError("query vacío")
    # Limitar longitud para evitar abusos o errores
    if len(q) > 200:
        q = q[:200]
    # Remover control chars
    q = re.sub(r"[\x00-\x1f\x7f]", " ", q)
    return q


def csv_safe(text: str) -> str:
    """Evita CSV injection ante lectores como Excel."""
    if not text:
        return text
    if text[0] in SAFE_CSV_PREFIXES:
        return "'" + text
    return text


def selector_find(node: BeautifulSoup, selector: str) -> Optional[str]:
    """Selector simple para BS4: soporta '.clase', '#id', 'tag', 'a[href]'."""
    if not selector:
        return None
    attr_value: Optional[str] = None
    if selector.startswith('.'):
        found = node.select_one(selector)  # BS4 soporta selectores CSS básicos
    elif selector.startswith('#'):
        found = node.select_one(selector)
    else:
        # soporte simple de atributo
        if '[' in selector and ']' in selector:
            found = node.select_one(selector)
        else:
            found = node.find(selector)
    if not found:
        return None
    # Obtener texto o atributos comunes
    if found.has_attr('href'):
        attr_value = found['href']
    elif found.has_attr('src'):
        attr_value = found['src']
    else:
        attr_value = found.get_text(strip=True)
    return attr_value


def build_url(template: str, query: str) -> str:
    """Construye URL segura usando urlencode sobre {query}."""
    if '{query}' not in template:
        raise ValueError(f"Template inválida: {template}")
    encoded = urlencode({'q': query})
    # Reemplazamos {query} por valor sin clave para máxima compatibilidad
    # Si el endpoint exige ?q=... ya lo manejamos con encoded
    if 'q={' in template:
        return template.replace('{query}', query)
    # Caso genérico: añadimos ?q=...
    base = template.replace('{query}', '')
    if '?' in base:
        # Evitar duplicar '?'
        if base.endswith('?') or base.endswith('&'):
            return base + encoded
        return base + '&' + encoded
    return f"{base}?{encoded}"


async def sleep_backoff(attempt: int, base: float) -> None:
    """Backoff exponencial con jitter."""
    delay = base * (2 ** (attempt - 1))
    delay = delay * (0.7 + random.random() * 0.6)  # jitter ~ ±30%
    await asyncio.sleep(delay)


# ----------------------------
# Scraper principal
# ----------------------------

class RobotsCache:
    """Cache simple de robots.txt por host."""
    def __init__(self) -> None:
        self._cache: Dict[str, robotparser.RobotFileParser] = {}

    async def allowed(self, session: aiohttp.ClientSession, url: str, ua: str) -> bool:
        parsed = urlparse(url)
        base = (parsed.scheme, parsed.netloc)
        if not base[0] or not base[1]:
            return True
        key = f"{base[0]}://{base[1]}"
        rp = self._cache.get(key)
        if not rp:
            robots_url = urlunparse((base[0], base[1], "/robots.txt", "", "", ""))
            rp = robotparser.RobotFileParser()
            try:
                async with session.get(robots_url, timeout=ClientTimeout(total=5)) as res:
                    if res.status == 200:
                        text = await res.text()
                        rp.parse(text.splitlines())
                    else:
                        # Si no existe robots, asumimos permitido
                        rp.parse([])
            except Exception:
                # En fallo de red, asumimos permitido para no bloquear funcionalidad
                rp.parse([])
            self._cache[key] = rp
        return rp.can_fetch(ua, url)


class PersonScraper:
    """Scraper asíncrono, seguro y eficiente para búsqueda de personas."""
    def __init__(self, config: ScraperConfig, targets: List[Target]) -> None:
        # Mapear alias con typo si llega desde el CLI o config externa
        if config.busqueda_intesiva is True:
            config.busqueda_intensiva = True

        self.cfg = config
        self.targets = targets
        self.sem = asyncio.Semaphore(config.concurrency)
        self.robots = RobotsCache()

        self.timeout = ClientTimeout(
            total=self.cfg.total_timeout_s,
            connect=self.cfg.connect_timeout_s,
            sock_read=self.cfg.read_timeout_s,
        )

        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = logging.getLogger("person_scraper")

    async def __aenter__(self) -> "PersonScraper":
        headers = {
            "User-Agent": random.choice(self.cfg.user_agents) or DEFAULT_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
        }
        self.session = aiohttp.ClientSession(timeout=self.timeout, headers=headers)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.session and not self.session.closed:
            await self.session.close()

    async def fetch(self, url: str) -> Optional[str]:
        assert self.session is not None
        ua = self.session.headers.get("User-Agent", DEFAULT_UA)

        if self.cfg.obey_robots:
            allowed = await self.robots.allowed(self.session, url, ua)
            if not allowed:
                self.logger.info("Bloqueado por robots.txt: %s", url)
                return None

        for attempt in range(1, self.cfg.max_retries + 1):
            async with self.sem:
                try:
                    if self.cfg.rate_delay_s:
                        await asyncio.sleep(self.cfg.rate_delay_s)

                    async with self.session.get(url, allow_redirects=True) as resp:
                        if resp.status in (429, 500, 502, 503, 504):
                            raise aiohttp.ClientResponseError(
                                resp.request_info, resp.history, status=resp.status, message="retryable"
                            )
                        if resp.content_type and "text/html" not in resp.content_type:
                            return None
                        html = await resp.text(errors="ignore")
                        return html
                except asyncio.TimeoutError:
                    self.logger.warning("Timeout (%s) en %s (intento %d)", self.cfg.read_timeout_s, url, attempt)
                except aiohttp.ClientResponseError as e:
                    self.logger.warning("HTTP %s en %s (intento %d)", getattr(e, "status", "?"), url, attempt)
                except aiohttp.ClientError as e:
                    self.logger.warning("ClientError %s en %s (intento %d)", repr(e), url, attempt)
                except Exception as e:
                    self.logger.exception("Error inesperado en fetch %s: %s", url, repr(e))

            if attempt < self.cfg.max_retries:
                await sleep_backoff(attempt, self.cfg.backoff_base)
        return None

    def parse_items(self, html: str, target: Target) -> Iterable[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        for node in soup.select(target.item_selector):
            item: Dict[str, Any] = {"source": target.name}
            for field_name, field_sel in target.fields.items():
                raw = selector_find(node, field_sel) or ""
                if self.cfg.normalize_whitespace:
                    raw = re.sub(r"\s+", " ", raw).strip()
                item[field_name] = raw
            # Normalizaciones mínimas
            # Saneamos campos de texto para CSV seguro
            for k, v in list(item.items()):
                if isinstance(v, str):
                    item[k] = csv_safe(v)
            yield item

    async def search(self, query: str) -> AsyncIterator[Dict[str, Any]]:
        query = sanitize_query(query)
        assert self.session is not None

        # Si “intensiva”, podríamos variar UA por request
        for target in self.targets:
            url = build_url(target.url_template, query)
            if self.cfg.busqueda_intensiva and self.session:
                # Cambiar UA por request para reducir fingerprint estático
                self.session.headers["User-Agent"] = random.choice(self.cfg.user_agents) or DEFAULT_UA

            html = await self.fetch(url)
            if not html:
                continue
            for item in self.parse_items(html, target):
                # Filtrado mínimo: evitar filas vacías
                if any(v for k, v in item.items() if k != "source"):
                    yield item

    async def run_to_csv(self, query: str) -> int:
        rows = 0
        if self.cfg.output_csv:
            # Asegurar directorio
            os.makedirs(os.path.dirname(self.cfg.output_csv) or ".", exist_ok=True)
            fieldnames = ["source", "title", "full_name", "username", "link", "snippet"]
            # Escritura incremental (streaming)
            with open(self.cfg.output_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                async for item in self.search(query):
                    # Mapear faltantes sin romper
                    row = {k: item.get(k, "") for k in fieldnames}
                    writer.writerow(row)
                    rows += 1
        else:
            # Si no hay CSV, al menos consume el stream
            async for _ in self.search(query):
                rows += 1
        return rows


# ----------------------------
# Targets de ejemplo (modulares)
# ----------------------------

DEFAULT_TARGETS: List[Target] = [
    Target(
        name="PeopleSearchExample",
        url_template="https://example.com/search{query}",  # build_url añadirá ?q=
        item_selector=".result",
        fields={
            "title": ".title",
            "full_name": ".name",
            "username": ".handle",
            "link": "a[href]",
            "snippet": ".summary",
        },
    ),
    # Agrega más endpoints seguros/respetuosos de TOS
]


# ----------------------------
# CLI
# ----------------------------

def build_logger(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
        datefmt="%H:%M:%S",
    )


def parse_cli(argv: List[str]) -> Tuple[str, ScraperConfig]:
    import argparse

    p = argparse.ArgumentParser(description="Búsqueda de personas (scraper optimizado)")
    p.add_argument("query", type=str, help="Nombre/consulta a buscar")
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--timeout", type=float, default=20.0, help="Timeout total por request")
    p.add_argument("--connect-timeout", type=float, default=5.0)
    p.add_argument("--read-timeout", type=float, default=10.0)
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--rate-delay", type=float, default=0.0)
    p.add_argument("--ua", action="append", help="User-Agent adicional (repetir flag para varios)")
    p.add_argument("--csv", type=str, default="results.csv")
    p.add_argument("--no-robots", action="store_true", help="No verificar robots.txt")
    p.add_argument("--intensiva", dest="busqueda_intensiva", action="store_true", help="Búsqueda intensiva")
    # Alias para el typo (mantener compatibilidad con scripts viejos)
    p.add_argument("--intesiva", dest="busqueda_intesiva", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("-v", "--verbose", action="store_true")

    args = p.parse_args(argv)

    cfg = ScraperConfig(
        concurrency=max(1, args.concurrency),
        connect_timeout_s=args.connect_timeout,
        read_timeout_s=args.read_timeout,
        total_timeout_s=args.timeout,
        max_retries=max(0, args.retries),
        backoff_base=0.5,
        user_agents=args.ua or [DEFAULT_UA],
        obey_robots=not args.no_robots,
        rate_delay_s=max(0.0, args.rate_delay),
        output_csv=args.csv,
        busqueda_intensiva=bool(args.busqueda_intensiva),
        busqueda_intesiva=bool(args.busqueda_intesiva),  # typo compat
    )
    build_logger(args.verbose)
    return args.query, cfg


async def main_async(query: str, cfg: ScraperConfig) -> int:
    start = time.perf_counter()
    async with PersonScraper(cfg, DEFAULT_TARGETS) as scraper:
        rows = await scraper.run_to_csv(query)
    elapsed = time.perf_counter() - start
    logging.getLogger("person_scraper").info("Filas: %d | Tiempo: %.2fs", rows, elapsed)
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    """Punto de entrada CLI seguro."""
    if argv is None:
        argv = sys.argv[1:]
    query, cfg = parse_cli(argv)
    try:
        return asyncio.run(main_async(query, cfg))
    except KeyboardInterrupt:
        logging.getLogger("person_scraper").warning("Cancelado por usuario")
        return 130


# FIX: Guard main correcto (evita ejecución en import)
if __name__ == "__main__":
    sys.exit(main())
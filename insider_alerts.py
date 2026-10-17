#!/usr/bin/env python3
"""
Alertas de insider trading (Form 4 de la SEC) por Telegram.

Lee el feed de "últimos filings" de EDGAR, descarga cada Form 4 nuevo,
filtra las operaciones relevantes y envía un aviso a Telegram.

Uso:
    python insider_alerts.py              # una pasada (para cron / GitHub Actions)
    python insider_alerts.py --loop 600   # en bucle, cada 600 s (para un VPS)
    python insider_alerts.py --dry-run    # imprime los avisos en vez de enviarlos
    python insider_alerts.py --test       # envía un mensaje de prueba a Telegram
"""

import argparse
import csv
import html
import json
import logging
import os
import re
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser

import requests

# ═══════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN — ajusta aquí los filtros
# ═══════════════════════════════════════════════════════════════════════
CONFIG = {
    # Compras en mercado abierto (código P)
    "min_buy_value": 100_000,         # importe mínimo de la compra en USD
    "min_pct_increase": 0,            # % mínimo de aumento de posición (0 = sin filtro)
    "exclude_10b5_1": True,           # descartar operaciones bajo plan 10b5-1

    # Roles que se tienen en cuenta: "officer", "director", "ten_percent"
    "roles": ["officer", "director", "ten_percent"],

    # Índices: solo se avisa de empresas que estén en ALGUNO de estos.
    # Disponibles: "SP500", "NDX100" (Nasdaq-100), "R2000" (Russell 2000), "SP400" (mid caps)
    # Lista vacía [] = todas las empresas.
    "indices": ["SP500", "NDX100", "R2000"],
    "index_refresh_hours": 24,        # cada cuánto se actualizan las listas

    # Watchlist opcional: si tiene tickers, solo se avisa de esas empresas
    # (si usas watchlist e índices a la vez, se exigen las dos condiciones)
    "watchlist": [],                  # p. ej. ["AAPL", "MSFT", "NVDA"]

    # Cluster buying: varios insiders comprando la misma empresa
    "cluster_window_days": 30,
    "cluster_min_insiders": 2,

    # Ventas (desactivado por defecto: mucho ruido)
    "alert_sells": False,
    "min_sell_value": 5_000_000,

    # Incluir enmiendas (4/A)
    "include_amendments": False,

    # Técnico
    "max_feed_pages": 5,              # 100 entradas por página
    "db_path": os.environ.get("INSIDER_DB", "insider.db"),
    "request_delay": 0.15,            # segundos entre peticiones (límite SEC: 10/s)
}

# Secretos por variables de entorno
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
SEC_USER_AGENT = os.environ.get("SEC_USER_AGENT", "")  # "Nombre Apellido email@dominio.com"

FEED_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4"
    "&company=&dateb=&owner=include&start={start}&count=100&output=atom"
)
ATOM = "{http://www.w3.org/2005/Atom}"

# Fuentes de los componentes de cada índice: (tipo, url, mínimo de filas válido, etiqueta)
INDEX_SOURCES = {
    "SP500": ("wiki", "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", 450, "S&P 500"),
    "SP400": ("wiki", "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies", 350, "S&P 400"),
    "NDX100": ("wiki", "https://en.wikipedia.org/wiki/Nasdaq-100", 90, "Nasdaq-100"),
    "R2000": ("ishares",
              "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/"
              "1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund",
              1500, "Russell 2000"),
}
NASDAQ100_FALLBACK = "https://api.nasdaq.com/api/quote/list-type/nasdaq100"
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

log = logging.getLogger("insider")


# ═══════════════════════════════════════════════════════════════════════
# Utilidades
# ═══════════════════════════════════════════════════════════════════════
class SEC:
    """Cliente HTTP que respeta las normas de acceso de la SEC."""

    def __init__(self, user_agent: str, delay: float):
        if not user_agent or "@" not in user_agent:
            sys.exit("Falta SEC_USER_AGENT (formato: 'Nombre Apellido email@dominio.com').")
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"})
        self.delay = delay
        self._last = 0.0

    def get(self, url: str) -> str:
        for attempt in range(4):
            wait = self.delay - (time.time() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.time()
            r = self.s.get(url, timeout=30)
            if r.status_code == 200:
                return r.text
            if r.status_code in (403, 429, 503):
                time.sleep(2 ** attempt * 2)
                continue
            r.raise_for_status()
        raise RuntimeError(f"SEC no responde ({r.status_code}): {url}")


def _txt(el, path):
    if el is None:
        return None
    node = el.find(path)
    if node is None:
        return None
    v = node.find("value")
    node = v if v is not None else node
    return (node.text or "").strip() or None


def _num(s):
    try:
        return float(s.replace(",", "")) if s else None
    except ValueError:
        return None


def _flag(s):
    return (s or "").strip().lower() in ("1", "true", "y", "yes")


def fmt_money(x):
    return f"{x:,.0f}".replace(",", ".") + " $"


def fmt_num(x, dec=0):
    s = f"{x:,.{dec}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


# ═══════════════════════════════════════════════════════════════════════
# Base de datos (filings vistos + histórico de compras para clusters)
# ═══════════════════════════════════════════════════════════════════════
def db_open(path):
    con = sqlite3.connect(path)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS seen (
            accession TEXT PRIMARY KEY, seen_at TEXT);
        CREATE TABLE IF NOT EXISTS buys (
            accession TEXT PRIMARY KEY, issuer_cik TEXT, ticker TEXT,
            owner TEXT, trade_date TEXT, value REAL);
        CREATE INDEX IF NOT EXISTS buys_issuer ON buys(issuer_cik, trade_date);
        CREATE TABLE IF NOT EXISTS index_members (
            idx TEXT, ticker TEXT, cik TEXT);
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY, value TEXT);
    """)
    return con


def db_seen(con, acc):
    return con.execute("SELECT 1 FROM seen WHERE accession=?", (acc,)).fetchone() is not None


def db_mark(con, acc):
    con.execute("INSERT OR IGNORE INTO seen VALUES (?, ?)",
                (acc, datetime.now(timezone.utc).isoformat()))
    con.commit()


# ═══════════════════════════════════════════════════════════════════════
# Componentes de índices (S&P 500, Nasdaq-100, Russell 2000, S&P 400)
# ═══════════════════════════════════════════════════════════════════════
def norm_ticker(t):
    """BRK.B, BRK-B, BRK/B → BRKB, para comparar tickers de fuentes distintas."""
    return re.sub(r"[^A-Z0-9]", "", (t or "").upper())


class _TableParser(HTMLParser):
    """Extrae las tablas de una página HTML como listas de filas de texto."""

    def __init__(self):
        super().__init__()
        self.tables, self._stack, self._row, self._cell = [], [], None, None

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self._stack.append([])
        elif tag == "tr" and self._stack:
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._cell = []

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self._cell is not None and self._row is not None:
            text = re.sub(r"\[[^\]]*\]", "", "".join(self._cell))  # quita notas [1]
            self._row.append(" ".join(text.split()))
            self._cell = None
        elif tag == "tr" and self._row is not None and self._stack:
            if self._row:
                self._stack[-1].append(self._row)
            self._row = None
        elif tag == "table" and self._stack:
            self.tables.append(self._stack.pop())

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)


def parse_wiki_members(html_text, min_rows):
    """Busca la tabla de componentes (columna Symbol/Ticker) en una página de Wikipedia."""
    p = _TableParser()
    p.feed(html_text)
    for t in p.tables:
        if len(t) < min_rows:
            continue
        head = [h.lower() for h in t[0]]
        col = next((i for i, h in enumerate(head) if h in ("symbol", "ticker", "ticker symbol")), None)
        if col is None:
            continue
        cik_col = next((i for i, h in enumerate(head) if h == "cik"), None)
        tickers, ciks = set(), set()
        for r in t[1:]:
            if len(r) > col and r[col]:
                tickers.add(norm_ticker(r[col]))
            if cik_col is not None and len(r) > cik_col and r[cik_col].isdigit():
                ciks.add(r[cik_col].lstrip("0"))
        tickers.discard("")
        return tickers, ciks
    return set(), set()


def parse_ishares_members(text):
    """Lee el CSV de posiciones de un ETF de iShares (solo acciones)."""
    lines = text.lstrip("﻿").splitlines()
    start = next((i for i, l in enumerate(lines) if l.lstrip('"').startswith("Ticker")), None)
    if start is None:
        return set(), set()
    tickers = set()
    for row in csv.DictReader(lines[start:]):
        if (row.get("Asset Class") or "").strip() != "Equity":
            continue
        t = norm_ticker(row.get("Ticker"))
        if t:
            tickers.add(t)
    return tickers, set()


def _fetch_index(key, http):
    kind, url, min_rows, _ = INDEX_SOURCES[key]
    tickers, ciks = set(), set()
    try:
        r = http.get(url, timeout=30)
        r.raise_for_status()
        if kind == "wiki":
            tickers, ciks = parse_wiki_members(r.text, min_rows)
        else:
            tickers, ciks = parse_ishares_members(r.text)
    except Exception as ex:
        log.warning("Índice %s: error descargando (%s)", key, ex)
    if key == "NDX100" and len(tickers) < min_rows:  # plan B: web de Nasdaq
        try:
            r = http.get(NASDAQ100_FALLBACK, timeout=30)
            rows = r.json()["data"]["data"]["rows"]
            tickers = {norm_ticker(x["symbol"]) for x in rows if x.get("symbol")}
        except Exception as ex:
            log.warning("Índice NDX100: fallo también en la fuente alternativa (%s)", ex)
    if len(tickers) < min_rows:
        log.warning("Índice %s: lista incompleta (%d valores), se mantiene la anterior", key, len(tickers))
        return None
    return tickers, ciks


def load_indices(sec: SEC, con):
    """Devuelve {índice: (tickers, ciks)}, refrescando las listas si están viejas."""
    wanted = [k for k in CONFIG["indices"] if k in INDEX_SOURCES]
    for k in CONFIG["indices"]:
        if k not in INDEX_SOURCES:
            log.warning("Índice desconocido en CONFIG: %s", k)
    if not wanted:
        return {}

    now = datetime.now(timezone.utc)
    stale = []
    for k in wanted:
        row = con.execute("SELECT value FROM meta WHERE key=?", (f"idx_{k}",)).fetchone()
        if not row or now - datetime.fromisoformat(row[0]) > timedelta(hours=CONFIG["index_refresh_hours"]):
            stale.append(k)

    if stale:
        http = requests.Session()
        http.headers.update({"User-Agent": BROWSER_UA, "Accept-Language": "en-US,en;q=0.9"})
        # Mapa ticker → CIK de la SEC, para cruzar por CIK (más fiable que el ticker)
        tick2cik = {}
        try:
            data = json.loads(sec.get("https://www.sec.gov/files/company_tickers.json"))
            tick2cik = {norm_ticker(v["ticker"]): str(v["cik_str"]) for v in data.values()}
        except Exception as ex:
            log.warning("No se pudo descargar company_tickers.json: %s", ex)
        for k in stale:
            res = _fetch_index(k, http)
            if res is None:
                continue
            tickers, ciks = res
            ciks |= {tick2cik[t] for t in tickers if t in tick2cik}
            con.execute("DELETE FROM index_members WHERE idx=?", (k,))
            con.executemany("INSERT INTO index_members VALUES (?,?,?)",
                            [(k, t, None) for t in tickers] + [(k, None, c) for c in ciks])
            con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (f"idx_{k}", now.isoformat()))
            con.commit()
            log.info("Índice %s actualizado: %d tickers", k, len(tickers))

    out = {}
    for k in wanted:
        rows = con.execute("SELECT ticker, cik FROM index_members WHERE idx=?", (k,)).fetchall()
        if rows:
            out[k] = ({t for t, _ in rows if t}, {c for _, c in rows if c})
        else:
            log.warning("Índice %s sin datos: no se usará para filtrar", k)
    return out


def match_indices(doc, index_map):
    """Etiquetas de los índices a los que pertenece la empresa del filing."""
    t, c = norm_ticker(doc["ticker"]), doc["issuer_cik"]
    return [INDEX_SOURCES[k][3] for k, (tickers, ciks) in index_map.items()
            if (c and c in ciks) or (t and t in tickers)]


# ═══════════════════════════════════════════════════════════════════════
# Feed de EDGAR
# ═══════════════════════════════════════════════════════════════════════
def fetch_new_filings(sec: SEC, con):
    """Devuelve los Form 4 no vistos, del más antiguo al más reciente."""
    found, order = {}, []
    for page in range(CONFIG["max_feed_pages"]):
        xml_text = sec.get(FEED_URL.format(start=page * 100))
        root = ET.fromstring(xml_text.encode())
        entries = root.findall(f"{ATOM}entry")
        if not entries:
            break
        new_in_page = 0
        for e in entries:
            eid = (e.findtext(f"{ATOM}id") or "")
            m = re.search(r"accession-number=(\d{10}-\d{2}-\d{6})", eid)
            if not m:
                continue
            acc = m.group(1)
            cat = e.find(f"{ATOM}category")
            form = cat.get("term") if cat is not None else (e.findtext(f"{ATOM}title") or "").split(" - ")[0]
            if form not in ("4", "4/A"):
                continue
            if form == "4/A" and not CONFIG["include_amendments"]:
                continue
            if acc in found or db_seen(con, acc):
                continue
            link = e.find(f"{ATOM}link")
            href = link.get("href") if link is not None else ""
            cm = re.search(r"/data/(\d+)/", href)
            if not cm:
                continue
            found[acc] = {"accession": acc, "cik": cm.group(1), "index_url": href,
                          "filed": (e.findtext(f"{ATOM}updated") or "")[:10], "form": form}
            order.append(acc)
            new_in_page += 1
        if new_in_page == 0:
            break  # ya hemos llegado a lo procesado en la pasada anterior
    return [found[a] for a in reversed(order)]


def fetch_form4_xml(sec: SEC, f):
    acc_nodash = f["accession"].replace("-", "")
    url = f"https://www.sec.gov/Archives/edgar/data/{f['cik']}/{acc_nodash}/{f['accession']}.txt"
    raw = sec.get(url)
    m = re.search(r"<XML>(.*?)</XML>", raw, re.S | re.I)
    if not m:
        raise ValueError("Filing sin bloque XML")
    return m.group(1).strip()


# ═══════════════════════════════════════════════════════════════════════
# Parseo del Form 4
# ═══════════════════════════════════════════════════════════════════════
def parse_form4(xml_text):
    root = ET.fromstring(xml_text.encode())
    issuer = root.find("issuer")
    doc = {
        "issuer_cik": (_txt(issuer, "issuerCik") or "").lstrip("0"),
        "issuer_name": _txt(issuer, "issuerName") or "",
        "ticker": (_txt(issuer, "issuerTradingSymbol") or "").upper(),
        "aff10b5one": _flag(_txt(root, "aff10b5One")),
        "owners": [],
        "txs": [],
    }

    for ro in root.findall("reportingOwner"):
        rel = ro.find("reportingOwnerRelationship")
        doc["owners"].append({
            "name": _txt(ro, "reportingOwnerId/rptOwnerName") or "?",
            "officer": _flag(_txt(rel, "isOfficer")),
            "director": _flag(_txt(rel, "isDirector")),
            "ten_percent": _flag(_txt(rel, "isTenPercentOwner")),
            "title": _txt(rel, "officerTitle") or _txt(rel, "otherText") or "",
        })

    footnotes = {fn.get("id"): "".join(fn.itertext()) for fn in root.findall("footnotes/footnote")}

    for t in root.findall("nonDerivativeTable/nonDerivativeTransaction"):
        fn_text = " ".join(footnotes.get(f.get("id"), "") for f in t.iter("footnoteId"))
        doc["txs"].append({
            "code": _txt(t, "transactionCoding/transactionCode"),
            "date": _txt(t, "transactionDate"),
            "shares": _num(_txt(t, "transactionAmounts/transactionShares")) or 0.0,
            "price": _num(_txt(t, "transactionAmounts/transactionPricePerShare")),
            "ad": _txt(t, "transactionAmounts/transactionAcquiredDisposedCode"),
            "post": _num(_txt(t, "postTransactionAmounts/sharesOwnedFollowingTransaction")),
            "direct": _txt(t, "ownershipNature/directOrIndirectOwnership") != "I",
            "plan": bool(re.search(r"10b5-?1", fn_text, re.I)),
        })
    return doc


def role_label(o):
    parts = []
    if o["officer"]:
        parts.append(o["title"] or "Directivo")
    if o["director"]:
        parts.append("Consejero")
    if o["ten_percent"]:
        parts.append(">10%")
    if not parts and o["title"]:
        parts.append(o["title"])
    return " · ".join(parts) or "Insider"


def summarize(doc, code):
    """Agrega las operaciones de un tipo (P o S) dentro del filing."""
    txs = [t for t in doc["txs"] if t["code"] == code and t["price"]]
    if CONFIG["exclude_10b5_1"]:
        txs = [t for t in txs if not (t["plan"] or doc["aff10b5one"])]
    if not txs:
        return None
    shares = sum(t["shares"] for t in txs)
    value = sum(t["shares"] * t["price"] for t in txs)
    # % de aumento sobre la posición previa (misma naturaleza que la última operación)
    last = txs[-1]
    same = [t for t in txs if t["direct"] == last["direct"]]
    pct = None
    if code == "P" and last["post"] is not None:
        bought = sum(t["shares"] for t in same)
        prior = last["post"] - bought
        pct = float("inf") if prior <= 0 else bought / prior * 100
    return {
        "shares": shares, "value": value, "avg_price": value / shares if shares else 0,
        "pct": pct, "post": last["post"], "direct": last["direct"],
        "date": min(t["date"] for t in txs if t["date"]) if any(t["date"] for t in txs) else "",
    }


# ═══════════════════════════════════════════════════════════════════════
# Filtros y clusters
# ═══════════════════════════════════════════════════════════════════════
def passes_role(doc):
    return any(any(o[r] for r in CONFIG["roles"]) for o in doc["owners"])


def passes_watchlist(doc):
    wl = {t.upper() for t in CONFIG["watchlist"]}
    return not wl or doc["ticker"] in wl


def cluster_info(con, doc, s):
    if not s["date"]:
        return None
    since = (date.fromisoformat(s["date"]) - timedelta(days=CONFIG["cluster_window_days"])).isoformat()
    rows = con.execute(
        "SELECT owner, SUM(value) FROM buys WHERE issuer_cik=? AND trade_date>=? GROUP BY owner",
        (doc["issuer_cik"], since)).fetchall()
    if len(rows) >= CONFIG["cluster_min_insiders"]:
        return {"n": len(rows), "total": sum(v for _, v in rows), "names": [n for n, _ in rows]}
    return None


# ═══════════════════════════════════════════════════════════════════════
# Mensajes
# ═══════════════════════════════════════════════════════════════════════
def build_message(doc, s, f, side, cluster=None):
    e = html.escape
    owners = doc["owners"]
    who = "; ".join(f"{e(o['name'])} — {e(role_label(o))}" for o in owners[:3])
    if len(owners) > 3:
        who += f" (+{len(owners) - 3})"

    head = "🟢 <b>COMPRA INSIDER</b>" if side == "P" else "🔴 <b>VENTA INSIDER</b>"
    ticker = e(doc["ticker"] or "s/ticker")
    lines = [f"{head} — <b>${ticker}</b>" + ("  🔥 <b>CLUSTER</b>" if cluster else ""),
             e(doc["issuer_name"]) + (f"  ·  🏷️ {e(' · '.join(doc['indices']))}" if doc.get("indices") else ""),
             f"👤 {who}",
             f"💵 <b>{fmt_money(s['value'])}</b> · {fmt_num(s['shares'])} acc. a {fmt_num(s['avg_price'], 2)} $"]
    if s["pct"] is not None:
        tipo = "directa" if s["direct"] else "indirecta"
        pct = "nueva posición" if s["pct"] == float("inf") else f"+{fmt_num(s['pct'], 1)}% sobre su posición"
        lines.append(f"📈 {pct} ({tipo})")
    if cluster:
        lines.append(f"👥 {cluster['n']} insiders comprando en {CONFIG['cluster_window_days']} días "
                     f"· total {fmt_money(cluster['total'])}")
    lines.append(f"📅 Operación: {s['date'] or '?'} · Presentado: {f['filed'] or '?'}")
    lines.append(f"🔗 <a href=\"{e(f['index_url'])}\">Ver filing en EDGAR</a>")
    return "\n".join(lines)


def send_telegram(text, dry_run=False):
    if dry_run:
        print("\n" + html.unescape(re.sub(r"<[^>]+>", "", text)) + "\n" + "─" * 50)
        return
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        sys.exit("Faltan TELEGRAM_BOT_TOKEN y/o TELEGRAM_CHAT_ID.")
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for _ in range(3):
        r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text,
                                     "parse_mode": "HTML", "disable_web_page_preview": "true"},
                          timeout=20)
        if r.status_code == 429:
            time.sleep(r.json().get("parameters", {}).get("retry_after", 5))
            continue
        r.raise_for_status()
        return
    log.warning("Telegram: no se pudo enviar tras varios intentos")


# ═══════════════════════════════════════════════════════════════════════
# Proceso principal
# ═══════════════════════════════════════════════════════════════════════
def process_filing(con, doc, f, dry_run=False, index_map=None):
    """Evalúa un Form 4 ya parseado. Devuelve el nº de avisos enviados."""
    if not passes_watchlist(doc) or not passes_role(doc):
        return 0
    doc["indices"] = match_indices(doc, index_map or {})
    if index_map and not doc["indices"]:
        return 0  # no está en ninguno de los índices elegidos
    sent = 0

    buy = summarize(doc, "P")
    if buy and buy["value"] >= CONFIG["min_buy_value"] and \
            (not CONFIG["min_pct_increase"] or (buy["pct"] or 0) >= CONFIG["min_pct_increase"]):
        owner = " / ".join(o["name"] for o in doc["owners"])
        con.execute("INSERT OR IGNORE INTO buys VALUES (?,?,?,?,?,?)",
                    (f["accession"], doc["issuer_cik"], doc["ticker"], owner, buy["date"], buy["value"]))
        con.commit()
        send_telegram(build_message(doc, buy, f, "P", cluster_info(con, doc, buy)), dry_run)
        sent += 1

    if CONFIG["alert_sells"]:
        sell = summarize(doc, "S")
        if sell and sell["value"] >= CONFIG["min_sell_value"]:
            send_telegram(build_message(doc, sell, f, "S"), dry_run)
            sent += 1
    return sent


def run_once(dry_run=False):
    sec = SEC(SEC_USER_AGENT, CONFIG["request_delay"])
    # En --dry-run se usa una base temporal: no marca nada como visto
    con = db_open(":memory:" if dry_run else CONFIG["db_path"])
    index_map = load_indices(sec, con)
    if CONFIG["indices"] and not index_map:
        log.warning("No hay listas de índices disponibles: esta pasada NO filtra por índice")
    filings = fetch_new_filings(sec, con)
    log.info("Form 4 nuevos: %d", len(filings))
    alerts = 0
    for f in filings:
        try:
            doc = parse_form4(fetch_form4_xml(sec, f))
            alerts += process_filing(con, doc, f, dry_run, index_map)
            db_mark(con, f["accession"])
        except Exception as ex:  # se reintentará en la próxima pasada
            log.warning("Error en %s: %s", f["accession"], ex)
    log.info("Avisos enviados: %d", alerts)
    con.close()


def main():
    ap = argparse.ArgumentParser(description="Alertas de insider trading (SEC Form 4) por Telegram")
    ap.add_argument("--loop", type=int, metavar="SEG", help="ejecutar en bucle cada SEG segundos")
    ap.add_argument("--dry-run", action="store_true", help="imprimir avisos en vez de enviarlos")
    ap.add_argument("--test", action="store_true", help="enviar mensaje de prueba a Telegram")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.test:
        send_telegram("✅ <b>Insider alerts</b> conectado correctamente.")
        print("Mensaje de prueba enviado.")
        return
    if args.loop:
        while True:
            try:
                run_once(args.dry_run)
            except Exception as ex:
                log.error("Fallo en la pasada: %s", ex)
            time.sleep(args.loop)
    else:
        run_once(args.dry_run)


if __name__ == "__main__":
    main()

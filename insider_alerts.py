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
import html
import logging
import os
import re
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone

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

    # Watchlist opcional: si tiene tickers, solo se avisa de esas empresas
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
    """)
    return con


def db_seen(con, acc):
    return con.execute("SELECT 1 FROM seen WHERE accession=?", (acc,)).fetchone() is not None


def db_mark(con, acc):
    con.execute("INSERT OR IGNORE INTO seen VALUES (?, ?)",
                (acc, datetime.now(timezone.utc).isoformat()))
    con.commit()


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
             e(doc["issuer_name"]),
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
def process_filing(con, doc, f, dry_run=False):
    """Evalúa un Form 4 ya parseado. Devuelve el nº de avisos enviados."""
    if not passes_watchlist(doc) or not passes_role(doc):
        return 0
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
    filings = fetch_new_filings(sec, con)
    log.info("Form 4 nuevos: %d", len(filings))
    alerts = 0
    for f in filings:
        try:
            doc = parse_form4(fetch_form4_xml(sec, f))
            alerts += process_filing(con, doc, f, dry_run)
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

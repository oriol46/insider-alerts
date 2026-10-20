#!/usr/bin/env python3
"""
Earnings Alerts — resultados del S&P 500 por Telegram.

Detecta los comunicados de resultados que las empresas del S&P 500 presentan a la
SEC (formulario 8-K, item 2.02), los analiza con Claude y te envía un resumen
diario por Telegram agrupado por sector.

Uso:
  python earnings_alerts.py               # revisa resultados nuevos y los analiza
  python earnings_alerts.py --summary     # envía el resumen de lo pendiente
  python earnings_alerts.py --ticker MSFT # analiza el último resultado de una empresa y te lo envía
  python earnings_alerts.py --test        # mensaje de prueba a Telegram
  Añade --dry-run para imprimir en pantalla en lugar de enviar.
"""
import argparse
import html
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from io import StringIO

import requests

# ---------------------------------------------------------------- configuración
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
SEC_USER_AGENT = os.environ.get("SEC_USER_AGENT", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ALPHAVANTAGE_API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY", "")

# Empresas con aviso inmediato de resultados y análisis de la call.
# Se leen de watchlist.txt (un ticker por línea; se edita desde Telegram con /añadir y /quitar).
WATCHLIST_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "watchlist.txt")


def read_watchlist():
    if os.path.exists(WATCHLIST_FILE):
        with open(WATCHLIST_FILE, encoding="utf-8") as f:
            items = [l.split("#")[0].strip().upper() for l in f]
        return [t for t in items if t]
    return [t.strip().upper() for t in os.environ.get("WATCHLIST", "UBER,NFLX,META,MSFT,SPGI,V").split(",")
            if t.strip()]


def write_watchlist(tickers):
    with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
        f.write("# Watchlist de Earnings Alerts: un ticker por línea. Se edita desde Telegram con /añadir y /quitar.\n")
        f.write("\n".join(tickers) + "\n")


WATCHLIST = set(read_watchlist())
CALL_MIN_HOURS = 2          # espera mínima desde los resultados antes de buscar la transcripción
CALL_RETRY_HOURS = 3        # cada cuánto se reintenta si aún no está publicada
CALL_MAX_TRIES = 8          # intentos por empresa (~1 día de búsqueda en horario de mercado)
AV_DAILY_LIMIT = int(os.environ.get("AV_DAILY_LIMIT", "20"))  # el plan gratis de Alpha Vantage es muy limitado
MAX_TRANSCRIPT_CHARS = 120_000

MODEL_FAST = os.environ.get("CLAUDE_MODEL_FAST", "claude-haiku-5-5")    # análisis de cada empresa
MODEL_SMART = os.environ.get("CLAUDE_MODEL_SMART", "claude-sonnet-5-5")  # lectura del día

DB_PATH = os.environ.get("EARNINGS_DB", "earnings.db")
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "2"))  # días hacia atrás que revisa
MAX_DOC_CHARS = 60_000      # recorta comunicados muy largos (ahorra coste)
MAX_ATTEMPTS = 3            # reintentos de análisis por filing
SEC_DELAY = 0.12            # la SEC permite 10 peticiones/segundo
TELEGRAM_LIMIT = 3900       # Telegram corta en 4096 caracteres

WIKI_SP500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
SECTOR_ES = {
    "Information Technology": "Tecnología",
    "Health Care": "Salud",
    "Financials": "Financieras",
    "Consumer Discretionary": "Consumo discrecional",
    "Consumer Staples": "Consumo básico",
    "Communication Services": "Comunicaciones",
    "Industrials": "Industriales",
    "Energy": "Energía",
    "Materials": "Materiales",
    "Real Estate": "Inmobiliario",
    "Utilities": "Utilities",
}

SESSION = requests.Session()


def log(msg):
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


# ---------------------------------------------------------------- base de datos
def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS filings (
        accession TEXT PRIMARY KEY, cik INTEGER, ticker TEXT, name TEXT, sector TEXT,
        filing_date TEXT, acceptance TEXT, url TEXT, analysis TEXT,
        status TEXT DEFAULT 'pending', attempts INTEGER DEFAULT 0,
        input_tokens INTEGER DEFAULT 0, output_tokens INTEGER DEFAULT 0,
        summarized INTEGER DEFAULT 0, created_at TEXT)""")
    conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT, updated TEXT)")
    existing = {r[1] for r in conn.execute("PRAGMA table_info(filings)")}
    for column, kind in (("notified", "INTEGER DEFAULT 0"), ("call_status", "TEXT DEFAULT 'pending'"),
                         ("call_tries", "INTEGER DEFAULT 0"), ("call_last_try", "TEXT"), ("call_analysis", "TEXT")):
        if column not in existing:
            conn.execute(f"ALTER TABLE filings ADD COLUMN {column} {kind}")
    conn.commit()
    return conn


# ---------------------------------------------------------------- red
def sec_get(url, retries=4):
    headers = {"User-Agent": SEC_USER_AGENT, "Accept-Encoding": "gzip, deflate"}
    for attempt in range(retries):
        time.sleep(SEC_DELAY)
        try:
            r = SESSION.get(url, headers=headers, timeout=30)
        except requests.RequestException:
            time.sleep(2 * (attempt + 1))
            continue
        if r.status_code == 200:
            return r
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(3 * (attempt + 1))
            continue
        r.raise_for_status()
    raise RuntimeError(f"La SEC no responde: {url}")


# ---------------------------------------------------------------- S&P 500
def load_sp500(conn):
    """Lista del S&P 500 desde Wikipedia (ticker, nombre, sector, CIK). Se refresca cada 7 días."""
    row = conn.execute("SELECT value, updated FROM meta WHERE key='sp500'").fetchone()
    if row:
        age = datetime.now(timezone.utc) - datetime.fromisoformat(row[1])
        if age < timedelta(days=7):
            return json.loads(row[0])
    try:
        import pandas as pd
        page = SESSION.get(WIKI_SP500, headers={"User-Agent": SEC_USER_AGENT or "earnings-alerts"}, timeout=30)
        page.raise_for_status()
        df = pd.read_html(StringIO(page.text), attrs={"id": "constituents"})[0]
        companies, seen = [], set()
        for _, r in df.iterrows():
            cik = int(r["CIK"])
            if cik in seen:  # GOOGL/GOOG, FOX/FOXA… comparten CIK
                continue
            seen.add(cik)
            companies.append({
                "ticker": str(r["Symbol"]).strip(),
                "name": str(r["Security"]).strip(),
                "sector": SECTOR_ES.get(str(r["GICS Sector"]).strip(), str(r["GICS Sector"]).strip()),
                "cik": cik,
            })
        if len(companies) < 400:
            raise RuntimeError(f"Lista sospechosa: {len(companies)} empresas")
        conn.execute("REPLACE INTO meta VALUES ('sp500', ?, ?)",
                     (json.dumps(companies), datetime.now(timezone.utc).isoformat()))
        conn.commit()
        log(f"Lista del S&P 500 actualizada: {len(companies)} empresas")
        return companies
    except Exception as e:
        if row:
            log(f"No se pudo actualizar la lista ({e}); uso la guardada")
            return json.loads(row[0])
        raise


def sec_ticker_map(conn):
    """{TICKER: [CIK, nombre]} de todas las empresas que presentan en la SEC (se refresca cada 7 días)."""
    row = conn.execute("SELECT value, updated FROM meta WHERE key='sec_tickers'").fetchone()
    if row and datetime.now(timezone.utc) - datetime.fromisoformat(row[1]) < timedelta(days=7):
        return json.loads(row[0])
    try:
        raw = sec_get("https://www.sec.gov/files/company_tickers.json").json()
        sec_map = {v["ticker"].upper(): [int(v["cik_str"]), v["title"]] for v in raw.values()}
        conn.execute("REPLACE INTO meta VALUES ('sec_tickers', ?, ?)",
                     (json.dumps(sec_map), datetime.now(timezone.utc).isoformat()))
        conn.commit()
        return sec_map
    except Exception as e:
        log(f"No se pudo cargar la lista de tickers de la SEC ({e})")
        return json.loads(row[0]) if row else {}


def load_companies(conn):
    """S&P 500 + los tickers de la watchlist que no están en el índice (p. ej. SPCX), buscados en la SEC."""
    companies = load_sp500(conn)
    known = {c["ticker"].upper().replace("-", ".") for c in companies}
    missing = sorted(t for t in WATCHLIST if t.replace("-", ".") not in known)
    if not missing:
        return companies
    sec_map = sec_ticker_map(conn)
    extra = []
    for t in missing:
        hit = sec_map.get(t) or sec_map.get(t.replace(".", "-"))
        if hit:
            extra.append({"ticker": t, "name": hit[1].title(), "sector": "Watchlist (fuera del S&P 500)",
                          "cik": hit[0]})
        else:
            log(f"Watchlist: {t} no aparece en la SEC")
    return companies + extra


# ---------------------------------------------------------------- SEC: detectar resultados
def earnings_filings(company, since_date):
    """8-K con item 2.02 (Results of Operations) desde since_date (YYYY-MM-DD).
    Para emisores extranjeros de la watchlist, también los 6-K (luego se filtra cuáles son de resultados)."""
    data = sec_get(f"https://data.sec.gov/submissions/CIK{company['cik']:010d}.json").json()
    rec = data["filings"]["recent"]
    accept_6k = company["ticker"].upper() in WATCHLIST
    out = []
    for i, form in enumerate(rec["form"]):
        if rec["filingDate"][i] < since_date:
            continue
        if form == "8-K":
            if not re.search(r"(^|,)\s*2\.02\s*(,|$)", rec["items"][i] or ""):
                continue
        elif not (form == "6-K" and accept_6k):
            continue
        out.append({
            "form": form,
            "accession": rec["accessionNumber"][i],
            "filing_date": rec["filingDate"][i],
            "acceptance": rec["acceptanceDateTime"][i],
            "primary": rec["primaryDocument"][i],
        })
    return out


def html_to_text(raw_html):
    """Convierte el HTML del comunicado en texto, conservando las tablas como filas 'a | b | c'."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(raw_html, "lxml")
    for tag in soup(["script", "style"]):
        tag.decompose()
    for tr in soup.find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
        cells = [c for c in cells if c and c not in ("$", "%", ")", "(")]
        tr.replace_with(soup.new_string(" | ".join(cells) + "\n"))
    text = soup.get_text("\n")
    text = re.sub(r"[ \t\xa0\u200b]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()


def fetch_press_release(cik, filing):
    """Descarga el comunicado (anexo EX-99.x). Si no hay anexo, usa el 8-K principal."""
    from bs4 import BeautifulSoup
    acc = filing["accession"]
    base = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc.replace('-', '')}/"
    docs = []
    try:
        index = sec_get(f"{base}{acc}-index.htm").text
        soup = BeautifulSoup(index, "lxml")
        for tr in soup.select("table.tableFile tr"):
            tds = tr.find_all("td")
            if len(tds) < 4:
                continue
            link, doc_type = tds[2].find("a"), tds[3].get_text(strip=True).upper()
            if link and doc_type.startswith("EX-99"):
                href = link["href"].replace("/ix?doc=", "")
                if href.lower().endswith((".htm", ".html", ".txt")):
                    docs.append((doc_type, "https://www.sec.gov" + href if href.startswith("/") else base + href))
    except Exception as e:
        log(f"  índice no disponible ({e})")
    docs.sort(key=lambda d: d[0])  # EX-99.1 primero
    if not docs:
        docs = [("8-K", base + filing["primary"])]

    parts, total = [], 0
    for doc_type, url in docs[:3]:
        text = html_to_text(sec_get(url).text)
        parts.append(f"=== {doc_type} ===\n{text}")
        total += len(text)
        if total >= MAX_DOC_CHARS:
            break
    return "\n\n".join(parts)[:MAX_DOC_CHARS], docs[0][1]


# ---------------------------------------------------------------- consenso (gratis, Yahoo)
def eps_consensus(ticker, filing_date):
    """BPA estimado por el consenso para ese trimestre (Yahoo Finance). None si no lo encuentra."""
    try:
        import yfinance as yf
        df = yf.Ticker(ticker.replace(".", "-")).get_earnings_dates(limit=8)
        if df is None or df.empty or "EPS Estimate" not in df.columns:
            return None
        target = datetime.fromisoformat(filing_date).date()
        for ts, r in df.iterrows():
            if abs((ts.date() - target).days) <= 3 and r["EPS Estimate"] == r["EPS Estimate"]:
                return float(r["EPS Estimate"])
    except Exception as e:
        log(f"  consenso no disponible para {ticker} ({e})")
    return None


# ---------------------------------------------------------------- Claude
ANALYSIS_PROMPT = """Eres un analista de renta variable. Analiza el comunicado de resultados de {name} ({ticker}, sector {sector}), publicado el {date}.
{consensus_line}

Registra tu análisis llamando a la herramienta "registrar_resultado". Si no puedes usarla, devuelve SOLO un JSON válido con esta estructura:
{{
  "periodo": "periodo PRINCIPAL del comunicado: trimestre si es trimestral ('3T FY2026'); semestre ('1S 2026') o año ('FY2026') si el comunicado es semestral o anual",
  "es_resultados": true si el documento es un comunicado de resultados o una actualización trimestral de ventas; false si es otra cosa (recompras, juntas, nombramientos…),
  "moneda": código ISO de la moneda en que presenta las cuentas (USD, EUR, DKK, GBP, CHF, TWD, SEK…),
  "ingresos_musd": número: ingresos totales de ESE periodo principal (no de un trimestre suelto dentro de un semestre) en MILLONES de esa moneda (p. ej. 94930.0 para 94,93 mil millones), o null,
  "crec_ingresos_pct": número: variación interanual de esos ingresos en % (p. ej. 8.2 o -3.1); si la empresa destaca el crecimiento orgánico o a tipo constante, usa ese, o null,
  "bpa": número: BPA diluido de ese periodo en esa moneda (ajustado si la empresa lo da; si no, GAAP/IFRS), o null (muchas europeas solo publican ventas en el 1T y 3T),
  "bpa_tipo": "ajustado" | "GAAP",
  "vs_consenso": "bate" | "en linea" | "decepciona" | "sin consenso",
  "guidance": "sube" | "mantiene" | "baja" | "inicia" | "retira" | "no da",
  "guidance_detalle": "una frase con las cifras, o null",
  "destacados": ["exactamente 3 puntos, máximo 20 palabras cada uno, con cifras: segmentos, márgenes, KPIs clave del sector"],
  "alertas": ["normalmente lista VACÍA; ver reglas"],
  "tono": "positivo" | "neutro" | "negativo",
  "resumen": "una frase de máximo 20 palabras con la LECTURA del trimestre (qué lo explica y qué implica), sin repetir las cifras de ingresos ni BPA"
}}

Reglas:
- Español con formato numérico español en los textos. Usa solo datos del documento (y el consenso indicado); no inventes; si un dato no aparece, null.
- Ingresos: una sola cifra, la de ingresos totales que la empresa destaca (en bancos, ingresos netos totales; en aseguradoras, ingresos totales). Revisa las unidades de las tablas ("in millions", "in thousands") antes de convertir a millones.
- "vs_consenso": compara el BPA ajustado con el consenso indicado ("en linea" si la diferencia es menor del 2%); "sin consenso" si no se te ha dado.
- "tono" refleja la calidad del trimestre y del guidance, no el lenguaje de la empresa.
- "alertas" es SOLO para hechos excepcionales que un gestor deba conocer aunque no siga la empresa: recorte o suspensión del dividendo, subida del dividendo superior al 20%, programa de recompra NUEVO de más del 5% de la capitalización, salida o cambio de CEO/CFO, deterioros o provisiones extraordinarias, reformulación de cuentas, adquisición o desinversión relevante anunciada, reestructuración con despidos, investigación regulatoria o litigio material. NO son alertas: recompras o dividendos ordinarios, provisiones habituales del negocio, caídas de un segmento, variaciones normales de márgenes. Máximo 2. En la gran mayoría de trimestres la lista debe estar vacía.

DOCUMENTO:
{doc}"""

DAILY_PROMPT = """Eres un gestor de renta variable. Estos son los resultados del S&P 500 publicados en las últimas horas (JSON por empresa):

{data}

Escribe la "lectura del día" en español: entre 3 y 5 viñetas breves (máximo 30 palabras cada una) sobre tendencias por sector, sorpresas relevantes y cambios de guidance que merezcan atención. Cita los tickers. Sin introducción ni conclusión. Usa '• ' al inicio de cada viñeta y no uses markdown."""

_client = None


_nullable = lambda t: {"type": [t, "null"]}
ANALYSIS_TOOL = {
    "name": "registrar_resultado",
    "description": "Registra el análisis estructurado del comunicado de resultados.",
    "input_schema": {
        "type": "object",
        "properties": {
            "es_resultados": {"type": "boolean", "description": "Es un comunicado de resultados o de ventas trimestrales"},
            "moneda": {"type": "string", "description": "Código ISO de la moneda de presentación, p. ej. USD, EUR, DKK"},
            "periodo": {"type": ["string", "null"],
                        "description": "Periodo principal del comunicado (trimestre, semestre o año)"},
            "ingresos_musd": {"type": ["number", "null"],
                              "description": "Ingresos totales del periodo principal en millones de la moneda"},
            "crec_ingresos_pct": {"type": ["number", "null"],
                                  "description": "Variación interanual en % (orgánica si la empresa la destaca)"},
            "bpa": {"type": ["number", "null"], "description": "BPA diluido en la moneda"},
            "bpa_tipo": {"type": "string", "enum": ["ajustado", "GAAP"]},
            "vs_consenso": {"type": "string", "enum": ["bate", "en linea", "decepciona", "sin consenso"]},
            "guidance": {"type": "string", "enum": ["sube", "mantiene", "baja", "inicia", "retira", "no da"]},
            "guidance_detalle": _nullable("string"),
            "destacados": {"type": "array", "items": {"type": "string"}, "maxItems": 3},
            "alertas": {"type": "array", "items": {"type": "string"}, "maxItems": 2,
                        "description": "Solo hechos excepcionales según las reglas; normalmente vacía."},
            "tono": {"type": "string", "enum": ["positivo", "neutro", "negativo"]},
            "resumen": {"type": "string"},
        },
        "required": ["es_resultados", "moneda", "periodo", "ingresos_musd", "crec_ingresos_pct", "bpa", "bpa_tipo", "guidance", "destacados", "alertas", "tono",
                     "resumen"],
    },
}


def claude_message(prompt, model, max_tokens, tools=None):
    global _client
    if _client is None:
        import anthropic
        _client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    kwargs = {"model": model, "max_tokens": max_tokens, "messages": [{"role": "user", "content": prompt}]}
    if tools:
        kwargs["tools"] = tools
    return _client.messages.create(**kwargs)


def claude(prompt, model, max_tokens):
    msg = claude_message(prompt, model, max_tokens)
    text = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text")
    return text, msg.usage.input_tokens, msg.usage.output_tokens


def extract_analysis(msg):
    """Saca el análisis de la respuesta: primero de la herramienta, si no, de un JSON en el texto."""
    for block in msg.content:
        if getattr(block, "type", "") == "tool_use" and block.name == ANALYSIS_TOOL["name"]:
            return dict(block.input)
    text = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text")
    text = re.sub(r"```(?:json)?", "", text)
    start, end = text.find("{"), text.rfind("}")
    if start >= 0 and end > start:
        return json.loads(text[start:end + 1])
    kinds = ", ".join(getattr(b, "type", "?") for b in msg.content) or "vacía"
    raise ValueError(f"Claude no devolvió el análisis (stop_reason={msg.stop_reason}; bloques: {kinds}; "
                     f"texto: {text[:200]!r})")


def analyze(company, filing, doc):
    # El consenso de Yahoo para un ADR va en $ por ADR y no es comparable con el BPA en moneda local
    consensus = None if filing.get("form") == "6-K" else eps_consensus(company["ticker"], filing["filing_date"])
    consensus_line = (f"Consenso de BPA para este trimestre (Yahoo Finance, normalmente ajustado): {consensus:.2f} $."
                      if consensus is not None else "No hay dato de consenso disponible.")
    prompt = ANALYSIS_PROMPT.format(name=company["name"], ticker=company["ticker"], sector=company["sector"],
                                    date=filing["filing_date"], consensus_line=consensus_line, doc=doc)
    msg = claude_message(prompt, MODEL_FAST, 4000, tools=[ANALYSIS_TOOL])
    data = extract_analysis(msg)
    data["consenso_bpa"] = consensus
    return data, msg.usage.input_tokens, msg.usage.output_tokens


# ---------------------------------------------------------------- conference call (Alpha Vantage)
class AlphaVantageLimit(Exception):
    pass


def _av_budget_ok(conn):
    today = datetime.now(timezone.utc).date().isoformat()
    row = conn.execute("SELECT value, updated FROM meta WHERE key='av_calls'").fetchone()
    used = int(row[0]) if row and row[1] == today else 0
    return used < AV_DAILY_LIMIT


def _av_count(conn):
    today = datetime.now(timezone.utc).date().isoformat()
    row = conn.execute("SELECT value, updated FROM meta WHERE key='av_calls'").fetchone()
    used = int(row[0]) if row and row[1] == today else 0
    conn.execute("REPLACE INTO meta VALUES ('av_calls', ?, ?)", (str(used + 1), today))
    conn.commit()


def quarter_candidates(periodo, filing_date):
    """Trimestres a probar en Alpha Vantage: el fiscal que indica el comunicado y el natural anterior a la fecha."""
    out = []
    p = periodo or ""
    q = re.search(r"([1-4])\s*T|Q\s*([1-4])|([1-4])Q", p, re.I)
    y = re.search(r"(20\d\d)", p) or re.search(r"FY\s*'?(\d\d)\b", p, re.I)
    if q and y:
        year = y.group(1) if len(y.group(1)) == 4 else "20" + y.group(1)
        out.append(f"{year}Q{next(g for g in q.groups() if g)}")
    d = datetime.fromisoformat(filing_date).date()
    cal_q, cal_y = (d.month - 1) // 3, d.year     # trimestre natural anterior a la publicación
    if cal_q == 0:
        cal_q, cal_y = 4, cal_y - 1
    if f"{cal_y}Q{cal_q}" not in out:
        out.append(f"{cal_y}Q{cal_q}")
    return out


def fetch_transcript(conn, ticker, quarter):
    """Transcripción de la call desde Alpha Vantage. None si todavía no está."""
    if not _av_budget_ok(conn):
        raise AlphaVantageLimit("límite diario propio alcanzado")
    r = SESSION.get("https://www.alphavantage.co/query",
                    params={"function": "EARNINGS_CALL_TRANSCRIPT", "symbol": ticker.replace(".", "-"),
                            "quarter": quarter, "apikey": ALPHAVANTAGE_API_KEY}, timeout=60)
    _av_count(conn)
    r.raise_for_status()
    data = r.json()
    if "Information" in data or "Note" in data:
        raise AlphaVantageLimit(str(data.get("Information") or data.get("Note"))[:200])
    if "Error Message" in data:
        raise RuntimeError(data["Error Message"][:200])
    turns = data.get("transcript") or []
    if not turns:
        return None
    text = "\n\n".join(f"{t.get('speaker', '?')} ({t.get('title', '')}): {t.get('content', '')}" for t in turns)
    return text[:MAX_TRANSCRIPT_CHARS]


CALL_PROMPT = """Eres un gestor de renta variable fundamental. Tienes el análisis del comunicado de resultados de {name} ({ticker}) del trimestre {periodo}, publicado el {date}, y la transcripción de una conference call.

ANÁLISIS DEL COMUNICADO:
{analysis}

Analiza la call y registra el resultado con la herramienta "registrar_call". Céntrate en lo que la call AÑADE al comunicado: matices de la directiva, explicaciones, cambios de mensaje y el turno de preguntas. Escribe en español, concreto y con cifras cuando las haya. No inventes nada que no esté en la transcripción.

Cobertura y concisión:
- No omitas ningún TEMA relevante para un inversor (cambios en guidance o en el mensaje, KPIs, capital, márgenes, demanda, competencia, regulación, M&A, riesgos).
- Pero cada punto en 1-2 frases densas, con solo las 2-3 cifras que de verdad importan. Nada de contexto narrativo, citas largas ni enumerar todas las cifras.
- No repitas información entre secciones: lo que vaya en "guidance_call" no va en "mensajes_clave", y "balance" no repite lo anterior.
- El mensaje se lee en el móvil: la sección completa de la call debe ocupar unas 300-450 palabras en total.

Cuando cites a alguien, usa siempre nombre, apellido y cargo, por ejemplo "Ted Pick (CEO)" o "Sharon Yeshaya (CFO)"; para analistas, nombre y banco. Nunca solo el nombre de pila.

Primero comprueba si la transcripción corresponde a ESE trimestre (por las cifras y las fechas que se mencionan). Si es de otro trimestre, marca "corresponde": false y rellena el resto con lo mínimo.

TRANSCRIPCIÓN:
{transcript}"""

CALL_TOOL = {
    "name": "registrar_call",
    "description": "Registra el análisis de la conference call de resultados.",
    "input_schema": {
        "type": "object",
        "properties": {
            "corresponde": {"type": "boolean", "description": "La transcripción es del trimestre indicado"},
            "tono": {"type": "string", "enum": ["confiado", "neutral", "prudente", "defensivo"]},
            "tono_explicacion": {"type": "string", "description": "Una frase, máximo 30 palabras"},
            "mensajes_clave": {"type": "array", "items": {"type": "string"}, "maxItems": 6,
                               "description": "Un punto por tema relevante que no estaba en el comunicado (normalmente 4-6); cada uno 1-2 frases, máximo 45 palabras"},
            "guidance_call": {"type": ["string", "null"],
                              "description": "Matices sobre el guidance dados en la call, máximo 40 palabras, o null"},
            "preguntas": {"type": "array", "maxItems": 5, "items": {
                "type": "object",
                "properties": {"tema": {"type": "string", "description": "Tema en pocas palabras y analista, p. ej. 'Capex de IA (Mike Mayo, Wells Fargo)'"},
                               "respuesta": {"type": "string", "enum": ["clara", "vaga", "evasiva"]},
                               "detalle": {"type": "string",
                                           "description": "Quién respondió y qué dijo, máximo 35 palabras"}},
                "required": ["tema", "respuesta", "detalle"]},
                "description": "Las preguntas de analistas más relevantes (normalmente 3-5) y cómo respondió la directiva"},
            "riesgos": {"type": "array", "items": {"type": "string"}, "maxItems": 3,
                        "description": "Riesgos o señales de alerta mencionados, máximo 25 palabras cada uno; vacío si no hay"},
            "impacto": {"type": "string", "enum": ["mejora", "confirma", "empeora"],
                        "description": "Si la call mejora, confirma o empeora la lectura del comunicado"},
            "balance": {"type": "string",
                        "description": "Una frase, máximo 35 palabras, con el matiz principal que aporta la call. No empieces "
                                       "repitiendo 'confirma/mejora/empeora' ni 'la call', ve directo al contenido"},
        },
        "required": ["corresponde", "tono", "tono_explicacion", "mensajes_clave", "preguntas", "riesgos",
                     "impacto", "balance"],
    },
}


def analyze_call(row, transcript):
    a = {k: v for k, v in row["analysis"].items() if k != "consenso_bpa"}
    prompt = CALL_PROMPT.format(name=row["name"], ticker=row["ticker"], periodo=a.get("periodo"),
                                date=row["filing_date"], analysis=json.dumps(a, ensure_ascii=False),
                                transcript=transcript)
    msg = claude_message(prompt, MODEL_SMART, 8000, tools=[CALL_TOOL])
    for block in msg.content:
        if getattr(block, "type", "") == "tool_use" and block.name == CALL_TOOL["name"]:
            return dict(block.input)
    raise ValueError(f"Claude no devolvió el análisis de la call (stop_reason={msg.stop_reason})")


# ---------------------------------------------------------------- Telegram
def send_telegram(text, dry_run=False):
    if dry_run:
        print("\n" + "-" * 60 + "\n" + text + "\n" + "-" * 60)
        return
    r = SESSION.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                     data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML",
                           "disable_web_page_preview": "true"}, timeout=30)
    if r.status_code != 200:
        log(f"Error de Telegram: {r.text[:300]}")
    time.sleep(1)


def send_blocks(header, blocks, dry_run=False):
    """Envía bloques completos sin partir ninguno, en mensajes de menos de 4096 caracteres."""
    current = header
    for block in blocks:
        if len(current) + len(block) + 2 > TELEGRAM_LIMIT:
            send_telegram(current, dry_run)
            current = ""
        current = f"{current}\n\n{block}" if current else block
    if current:
        send_telegram(current, dry_run)


# ---------------------------------------------------------------- formato
TONE = {"positivo": "🟢", "neutro": "🟡", "negativo": "🔴"}
VS = {"bate": " ✅", "en linea": " ➖", "decepciona": " ❌"}
GUIDE = {"sube": "Guía ⬆️", "mantiene": "Guía ➡️", "baja": "Guía ⬇️", "inicia": "Guía 🆕", "retira": "Guía ⛔"}


def esc(value):
    return html.escape(str(value)) if value not in (None, "") else ""


VS_TXT = {"bate": "✅ bate", "en linea": "➖ en línea", "decepciona": "❌ decepciona"}
GUIDE_TXT = {"sube": "⬆️ sube", "mantiene": "➡️ mantiene", "baja": "⬇️ baja",
             "inicia": "🆕 inicia", "retira": "⛔ retira", "no da": "— no da"}


def _es(x, decimals):
    """Número con formato español: 12.412,5"""
    s = f"{x:,.{decimals}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


CURRENCY = {"USD": "$", "EUR": "€", "GBP": "£", "DKK": "DKK", "CHF": "CHF", "SEK": "SEK", "NOK": "NOK",
            "TWD": "NT$", "JPY": "¥", "CNY": "CNY", "HKD": "HK$", "CAD": "C$"}


def _cur(a):
    code = str((a or {}).get("moneda") or "USD").upper()
    return CURRENCY.get(code, code)


def _money(x, a=None):
    return f"{_es(x, 2)} {_cur(a)}"


def _name(row):
    return re.sub(r"\s*\(The\)$", "", row["name"])


def _to_float(v):
    """Acepta números o textos como '3,03 $', '+11,6%' o '20.300'. None si no se puede."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    t = re.sub(r"[^\d,.\-+]", "", str(v))
    if "," in t and "." in t:
        t = t.replace(".", "").replace(",", ".")
    elif "," in t:
        t = t.replace(",", ".")
    elif re.fullmatch(r"[-+]?\d{1,3}(\.\d{3})+", t):  # '20.300' = veinte mil trescientos
        t = t.replace(".", "")
    try:
        return float(t)
    except ValueError:
        return None


def _num(v):
    return _to_float(v) is not None


def _revenue(a):
    musd = _to_float(a.get("ingresos_musd"))
    cur = _cur(a)
    unit = f"M{cur}" if len(cur) <= 2 else f"M {cur}"     # M$, M€, M£ · M DKK, M CHF
    if musd is not None:
        rev = f"{_es(musd / 1000, 1)} mil {unit}" if abs(musd) >= 10000 else f"{_es(musd, 0)} {unit}"
    else:
        rev = esc(a.get("ingresos")) or "—"  # formato antiguo
    pct = _to_float(a.get("crec_ingresos_pct"))
    if pct is not None:
        rev += f" ({'+' if pct > 0 else ''}{_es(pct, 1)}% a/a)"
    elif a.get("crec_ingresos"):
        rev += f" ({esc(a['crec_ingresos'])} a/a)"
    return rev


def _eps_value(a):
    if _num(a.get("bpa")):
        return _money(_to_float(a["bpa"]), a) + (" (GAAP)" if a.get("bpa_tipo") == "GAAP" else "")
    return esc(a.get("bpa")) or "—"


def _eps(a, with_consensus=True):
    eps = _eps_value(a)
    if with_consensus and a.get("consenso_bpa") is not None:
        eps += f" vs {_money(a['consenso_bpa'])} est."
    if a.get("vs_consenso") in VS:
        eps += VS[a["vs_consenso"]]
    return eps


def company_line(row):
    """Bloque compacto para el resumen diario."""
    a = row["analysis"]
    lines = [f"{TONE.get(a.get('tono'), '⚪')} <a href=\"{esc(row['url'])}\"><b>{esc(row['ticker'])}</b></a>"
             f" · {esc(_name(row))} · {esc(a.get('periodo'))}",
             f"Ingresos {_revenue(a)}",
             f"BPA {_eps(a)} · Guía {GUIDE.get(a.get('guidance'), '—').replace('Guía ', '')}"]
    if a.get("resumen"):
        lines.append(f"↳ <i>{esc(a['resumen'])}</i>")
    for alert in a.get("alertas") or []:
        lines.append(f"⚠️ {esc(alert)}")
    return "\n".join(lines)


def company_detail(row):
    """Mensaje completo de una empresa, por bloques."""
    a = row["analysis"]
    date = row.get("filing_date") or ""
    if date:
        date = f" · publicado {date[8:10]}/{date[5:7]}"
    out = [f"{TONE.get(a.get('tono'), '⚪')} <b>{esc(row['ticker'])} · {esc(_name(row))}</b>",
           f"{esc(row.get('sector', ''))} · {esc(a.get('periodo'))}{date}"]

    figures = [f"Ingresos: {_revenue(a)}", f"BPA: {_eps_value(a)}"]
    if a.get("consenso_bpa") is not None:
        verdict = VS_TXT.get(a.get("vs_consenso"))
        figures.append(f"Consenso BPA: {_money(a['consenso_bpa'])}" + (f" → {verdict}" if verdict else ""))
    guide = GUIDE_TXT.get(a.get("guidance"), "—")
    if a.get("guidance_detalle"):
        guide += f" · {esc(a['guidance_detalle'])}"
    figures.append(f"Guidance: {guide}")
    out.append("\n📊 <b>Cifras</b>\n" + "\n".join(figures))

    if a.get("resumen"):
        out.append(f"\n💬 <b>Lectura</b>\n<i>{esc(a['resumen'])}</i>")
    if a.get("destacados"):
        out.append("\n🔎 <b>Destacados</b>\n" + "\n".join(f"• {esc(d)}" for d in a["destacados"]))
    if a.get("alertas"):
        out.append("\n⚠️ <b>Alertas</b>\n" + "\n".join(f"• {esc(x)}" for x in a["alertas"]))
    if row.get("url"):
        where = " en la SEC" if "sec.gov" in row["url"] else ""
        out.append(f"\n🔗 <a href=\"{esc(row['url'])}\">Comunicado{where}</a>")
    return "\n".join(out)


CALL_TONE = {"confiado": "💪 confiado", "neutral": "😐 neutral", "prudente": "🤔 prudente", "defensivo": "🛡️ defensivo"}
ANSWER = {"clara": "🟢", "vaga": "🟡", "evasiva": "🔴"}
IMPACT = {"mejora": "⬆️ Mejora", "confirma": "➡️ Confirma", "empeora": "⬇️ Empeora"}


def call_section(c):
    """Sección de la call como lista de bloques (para poder partir mensajes largos sin romper nada)."""
    head = f"🎙️ <b>La call</b> · tono {CALL_TONE.get(c.get('tono'), esc(c.get('tono')))}"
    if c.get("tono_explicacion"):
        head += f"\n<i>{esc(c['tono_explicacion'])}</i>"
    blocks = [head]
    if c.get("mensajes_clave"):
        blocks.append("<b>Mensajes clave</b>\n" + "\n".join(f"• {esc(m)}" for m in c["mensajes_clave"]))
    if c.get("guidance_call"):
        blocks.append(f"<b>Guidance en la call:</b> {esc(c['guidance_call'])}")
    for i, p in enumerate(c.get("preguntas") or []):
        line = f"{ANSWER.get(p.get('respuesta'), '⚪')} <b>{esc(p.get('tema'))}</b>: {esc(p.get('detalle'))}"
        blocks.append(("<b>Preguntas de analistas</b>\n" if i == 0 else "") + line)
    if c.get("riesgos"):
        blocks.append("<b>Riesgos</b>\n" + "\n".join(f"• {esc(r)}" for r in c["riesgos"]))
    blocks.append(f"📌 <b>{IMPACT.get(c.get('impacto'), 'Balance')}</b> la lectura del comunicado. "
                  f"{esc(c.get('balance'))}")
    return blocks


def send_full_update(row, dry_run=False):
    """Mensaje completo de resultados + sección de la call, sin partir bloques."""
    header = "🎙️ <b>Actualización tras la call</b>"
    send_blocks(header, [company_detail(row), *call_section(row["call"])], dry_run)


# ---------------------------------------------------------------- buzón: transcripciones enviadas por Telegram
import copy

DOC_PROMPT = """Eres un gestor de renta variable fundamental. Te paso la transcripción de una conference call de resultados (descargada de TIKR u otra fuente). {hint}

Analízala y registra el resultado con la herramienta "registrar_call_documento". Escribe en español, concreto y con cifras cuando las haya. No inventes nada que no esté en el documento.

Cobertura y concisión:
- Identifica la empresa, su ticker y el trimestre.
- "cifras": las cifras clave del trimestre que da la directiva (ventas, crecimiento orgánico, márgenes, BPA, caja, KPIs del sector), con su variación.
- No omitas ningún TEMA relevante para un inversor (guidance, cambios de mensaje, KPIs, capital, márgenes, demanda, competencia, regulación, M&A, riesgos), pero cada punto en 1-2 frases densas con las 2-3 cifras que importan.
- No repitas información entre secciones. La sección completa debe ocupar unas 350-500 palabras.
- Cuando cites a alguien, usa nombre, apellido y cargo, p. ej. "Lars Fruergaard Jørgensen (CEO)"; para analistas, nombre y banco.

DOCUMENTO:
{text}"""

DOC_TOOL = copy.deepcopy(CALL_TOOL)
DOC_TOOL["name"] = "registrar_call_documento"
DOC_TOOL["description"] = "Registra el análisis de una conference call enviada como documento."
_props = DOC_TOOL["input_schema"]["properties"]
_props.pop("corresponde", None)
_props.pop("impacto", None)
_props.update({
    "empresa": {"type": "string"},
    "ticker": {"type": ["string", "null"]},
    "periodo": {"type": "string", "description": "Trimestre y año, p. ej. '3T 2026'"},
    "cifras": {"type": "array", "items": {"type": "string"}, "maxItems": 6,
               "description": "Cifras clave del trimestre dadas en la call, una por punto, máximo 20 palabras"},
    "lectura": {"type": "string", "enum": ["positiva", "neutra", "negativa"],
                "description": "Lectura global del trimestre según la call"},
})
_props["balance"]["description"] = "Una frase, máximo 35 palabras, con la conclusión principal para un inversor"
DOC_TOOL["input_schema"]["required"] = ["empresa", "periodo", "cifras", "tono", "tono_explicacion",
                                        "mensajes_clave", "preguntas", "riesgos", "lectura", "balance"]

DOC_EXTENSIONS = (".pdf", ".txt", ".md", ".docx", ".htm", ".html")
MAX_DOC_BYTES = 20 * 1024 * 1024  # límite de descarga de los bots de Telegram


def telegram_api(method, **params):
    r = SESSION.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}", data=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram {method}: {data}")
    return data["result"]


def download_telegram_file(file_id):
    info = telegram_api("getFile", file_id=file_id)
    r = SESSION.get(f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{info['file_path']}", timeout=120)
    r.raise_for_status()
    return r.content


def document_to_text(name, content):
    """Texto de un PDF, Word, HTML o texto plano."""
    low = name.lower()
    if low.endswith(".pdf"):
        from pypdf import PdfReader
        from io import BytesIO
        reader = PdfReader(BytesIO(content))
        text = "\n".join(page.extract_text() or "" for page in reader.pages)
    elif low.endswith(".docx"):
        from docx import Document
        from io import BytesIO
        text = "\n".join(p.text for p in Document(BytesIO(content)).paragraphs)
    elif low.endswith((".htm", ".html")):
        text = html_to_text(content.decode("utf-8", errors="ignore"))
    else:
        text = content.decode("utf-8", errors="ignore")
    text = re.sub(r"[ \t\xa0]+", " ", text)
    return re.sub(r"\n\s*\n+", "\n", text).strip()


def analyze_document(text, hint):
    prompt = DOC_PROMPT.format(hint=f"Indicación del usuario: {hint}." if hint else "", text=text[:MAX_TRANSCRIPT_CHARS])
    msg = claude_message(prompt, MODEL_SMART, 5000, tools=[DOC_TOOL])
    for block in msg.content:
        if getattr(block, "type", "") == "tool_use" and block.name == DOC_TOOL["name"]:
            return dict(block.input)
    raise ValueError(f"Claude no devolvió el análisis (stop_reason={msg.stop_reason})")


READING = {"positiva": "🟢", "neutra": "🟡", "negativa": "🔴"}


def document_blocks(d):
    ticker = f" ({esc(d['ticker'])})" if d.get("ticker") else ""
    head = (f"{READING.get(d.get('lectura'), '⚪')} <b>{esc(d.get('empresa'))}{ticker}</b> · {esc(d.get('periodo'))}")
    blocks = [head]
    if d.get("cifras"):
        blocks.append("📊 <b>Cifras de la call</b>\n" + "\n".join(f"• {esc(x)}" for x in d["cifras"]))
    call = call_section(d)
    call[-1] = f"📌 <b>Conclusión:</b> {esc(d.get('balance'))}"   # sin comunicado con el que comparar
    return blocks + call


HELP_TEXT = ("🎙️ <b>Análisis de calls</b>\n"
             "• <b>Pega la transcripción</b> directamente aquí (aunque Telegram la parta en varios mensajes, "
             "la junto yo), o\n"
             "• envíamela como <b>archivo</b> (PDF, TXT o Word).\n"
             "Opcional: escribe antes el ticker en un mensaje corto (p. ej. <i>NOVO 3T26</i>). "
             "Escribe <i>analiza</i> si no quieres esperar.\n\n"
             "📋 <b>Watchlist</b>: <i>/añadir AMZN</i> · <i>/quitar NKE</i> · <i>/lista</i>")

PASTE_MIN_CHARS = 300        # un mensaje más largo se considera parte de una transcripción pegada
PASTE_QUIET_SECONDS = 60     # se analiza cuando lleva este tiempo sin llegar texto nuevo


def _get_buffer(conn):
    row = conn.execute("SELECT value FROM meta WHERE key='tg_buffer'").fetchone()
    return json.loads(row[0]) if row else None


def _set_buffer(conn, buf):
    if buf is None:
        conn.execute("DELETE FROM meta WHERE key='tg_buffer'")
    else:
        conn.execute("REPLACE INTO meta VALUES ('tg_buffer', ?, ?)",
                     (json.dumps(buf, ensure_ascii=False), datetime.now(timezone.utc).isoformat()))
    conn.commit()


def _set_hint(conn, text):
    conn.execute("REPLACE INTO meta VALUES ('tg_hint', ?, ?)", (text, datetime.now(timezone.utc).isoformat()))
    conn.commit()


def _pop_hint(conn):
    """Ticker o indicación escrita antes de la transcripción (válida 30 minutos)."""
    row = conn.execute("SELECT value, updated FROM meta WHERE key='tg_hint'").fetchone()
    conn.execute("DELETE FROM meta WHERE key='tg_hint'")
    conn.commit()
    if row and datetime.now(timezone.utc) - datetime.fromisoformat(row[1]) < timedelta(minutes=30):
        return row[0]
    return ""


def _analyze_and_send(text, hint, label, dry_run):
    try:
        if len(text) < 2000:
            raise ValueError("hay muy poco texto para ser una transcripción completa")
        log(f"Analizando {label} ({len(text):,} caracteres)")
        analysis = analyze_document(text, hint)
        send_blocks("🎙️ <b>Análisis de la call</b>", document_blocks(analysis), dry_run)
    except Exception as e:
        log(f"Error con {label}: {e}")
        send_telegram(f"⚠️ No he podido analizar {esc(label)}: {esc(str(e)[:300])}", dry_run)


def _flush_buffer(conn, dry_run, force=False):
    buf = _get_buffer(conn)
    if not buf:
        return
    if not force and time.time() - buf["last"] < PASTE_QUIET_SECONDS:
        return  # aún puede estar llegando texto; se analiza en la próxima revisión
    _set_buffer(conn, None)
    _analyze_and_send("\n".join(buf["parts"]), buf.get("hint", ""), "el texto pegado", dry_run)


EU_SUFFIXES = {"PA", "AS", "CO", "L", "MC", "DE", "F", "MI", "SW", "BR", "ST", "HE", "OL", "LS", "VI", "IR", "LI"}
CMD_ADD = {"añadir", "anadir", "add", "agregar", "afegir"}
CMD_REMOVE = {"quitar", "eliminar", "borrar", "remove", "treure"}
CMD_LIST = {"lista", "list", "watchlist", "llista"}


def _is_european(ticker):
    return "." in ticker and ticker.rsplit(".", 1)[1] in EU_SUFFIXES


def _foreign_filer(cik):
    """True si la empresa presenta 6-K/20-F (emisor extranjero) en lugar de 10-Q/8-K."""
    try:
        forms = set(sec_get(f"https://data.sec.gov/submissions/CIK{cik:010d}.json").json()
                    ["filings"]["recent"]["form"][:200])
        return bool(forms & {"6-K", "20-F"}) and not (forms & {"10-Q", "10-K"})
    except Exception:
        return False


def watchlist_command(conn, text):
    """Comandos /añadir, /quitar y /lista. Devuelve la respuesta, o None si el texto no es un comando."""
    words = text.replace(",", " ").split()
    cmd = words[0].lower().lstrip("/").split("@")[0] if words else ""
    if cmd not in CMD_ADD | CMD_REMOVE | CMD_LIST:
        return None
    global WATCHLIST
    current = read_watchlist()
    if cmd in CMD_LIST:
        us = [t for t in current if not _is_european(t)]
        eu = [t for t in current if _is_european(t)]
        out = f"📋 <b>Watchlist</b> ({len(current)})\n🇺🇸 " + (", ".join(us) or "—")
        if eu:
            out += "\n🇪🇺 " + ", ".join(eu)
        return out

    tickers = [w.upper().strip() for w in words[1:]]
    if not tickers:
        return f"Escribe el ticker después del comando, p. ej. <i>/{cmd} AMZN</i>"
    lines = []
    if cmd in CMD_ADD:
        sec_map = None
        for t in tickers:
            if t in current:
                lines.append(f"• {esc(t)}: ya estaba")
                continue
            if _is_european(t):
                current.append(t)
                flag = EU_COUNTRY.get(t.rsplit(".", 1)[1], "🇪🇺")
                if eu_supported(t):
                    lines.append(f"• ✅ {flag} {esc(t)}: añadida (aviso al publicar resultados)")
                else:
                    lines.append(f"• {flag} {esc(t)}: añadida, pero aún no tengo fuente automática para ella. "
                                 f"Si cotiza también en EE. UU. (ADR), añade ese ticker; si no, dímelo y la configuramos")
                continue
            if sec_map is None:
                sec_map = sec_ticker_map(conn)
            hit = sec_map.get(t) or sec_map.get(t.replace(".", "-"))
            if not hit:
                lines.append(f"• ❌ {esc(t)}: no la encuentro en la SEC (¿ticker correcto? Las europeas llevan sufijo, p. ej. MC.PA)")
                continue
            current.append(t)
            note = ""
            if _foreign_filer(hit[0]):
                note = " · 🌍 emisor extranjero: sigo sus 6-K de resultados (cifras en su moneda, sin consenso de BPA)"
            lines.append(f"• ✅ {esc(t)} ({esc(hit[1].title())}): añadida{note}")
    else:
        for t in tickers:
            if t in current:
                current.remove(t)
                lines.append(f"• 🗑️ {esc(t)}: quitada")
            else:
                lines.append(f"• {esc(t)}: no estaba en la watchlist")
    write_watchlist(current)
    WATCHLIST = set(current)
    return "\n".join(lines) + f"\n\n📋 Watchlist: {len(current)} empresas. Escribe /lista para verla."


def run_inbox(conn, dry_run=False):
    """Lee los mensajes nuevos del bot: archivos o texto pegado con transcripciones de calls."""
    row = conn.execute("SELECT value FROM meta WHERE key='tg_offset'").fetchone()
    offset = int(row[0]) if row else 0
    updates = telegram_api("getUpdates", offset=offset, timeout=0, allowed_updates='["message"]')
    for upd in updates:
        conn.execute("REPLACE INTO meta VALUES ('tg_offset', ?, ?)",
                     (str(upd["update_id"] + 1), datetime.now(timezone.utc).isoformat()))
        conn.commit()
        msg = upd.get("message") or {}
        if str(msg.get("chat", {}).get("id")) != str(TELEGRAM_CHAT_ID):
            log(f"Mensaje ignorado de otro chat: {msg.get('chat', {}).get('id')}")
            continue

        text = (msg.get("text") or "").strip()
        if text and len(text) < PASTE_MIN_CHARS:
            reply = watchlist_command(conn, text)
            if reply:
                send_telegram(reply, dry_run)
                continue
        if text:
            buf = _get_buffer(conn)
            if len(text) >= PASTE_MIN_CHARS:                       # trozo de una transcripción pegada
                if not buf:
                    buf = {"parts": [], "hint": _pop_hint(conn), "last": 0}
                    send_telegram("📝 Recibiendo la transcripción…", dry_run)
                buf["parts"].append(text)
                buf["last"] = msg.get("date") or time.time()
                _set_buffer(conn, buf)
            elif text.lower().lstrip("/") in ("analiza", "analizar", "fin", "ya"):
                if buf:
                    _flush_buffer(conn, dry_run, force=True)
                else:
                    send_telegram("No tengo ningún texto pendiente. " + HELP_TEXT, dry_run)
            elif buf:                                              # mensaje corto: ticker o indicación
                buf["hint"] = (buf.get("hint", "") + " " + text).strip()
                _set_buffer(conn, buf)
            elif text.lower().lstrip("/") in ("start", "ayuda", "help", "hola"):
                send_telegram(HELP_TEXT, dry_run)
            else:                                                  # ticker antes de pegar o enviar el archivo
                _set_hint(conn, text)
                send_telegram(f"📌 Anotado: <i>{esc(text)}</i>. Ahora pega la transcripción o envía el archivo.",
                              dry_run)
            continue

        doc = msg.get("document")
        if not doc:
            continue
        name = doc.get("file_name") or "documento"
        if not name.lower().endswith(DOC_EXTENSIONS):
            send_telegram(f"⚠️ No sé leer <b>{esc(name)}</b>. Envíamelo en PDF, TXT o Word.", dry_run)
            continue
        if (doc.get("file_size") or 0) > MAX_DOC_BYTES:
            send_telegram("⚠️ El archivo supera los 20 MB que permite Telegram a los bots.", dry_run)
            continue
        send_telegram(f"📥 Recibido <b>{esc(name)}</b>. Analizando la call…", dry_run)
        try:
            content = document_to_text(name, download_telegram_file(doc["file_id"]))
        except Exception as e:
            send_telegram(f"⚠️ No he podido leer <b>{esc(name)}</b>: {esc(str(e)[:300])}", dry_run)
            continue
        hint = (msg.get("caption") or "").strip() or _pop_hint(conn)
        _analyze_and_send(content, hint, name, dry_run)

    _flush_buffer(conn, dry_run)


# ---------------------------------------------------------------- 6-K: ¿es un comunicado de resultados?
RESULTS_RE = re.compile(
    r"interim report|financial report for the period|quarterly report|half[- ]year(ly)? (report|results)|"
    r"(first|second|third|fourth) quarter( and (first )?(half|nine months|full[- ]year))?( 20\d\d)? results|"
    r"results for the (first|second|third|fourth) quarter|\b(q[1-4]|[1-4]q|h[12])\s?20\d\d (results|earnings|report)|"
    r"(full[- ]year|annual) results|nine months|first half|trading (update|statement)|"
    r"(net )?(sales|revenue)s? (for|in) the (first|second|third|fourth) quarter|earnings release|"
    r"consolidated (financial )?results|financial results|reports? (first|second|third|fourth) quarter|"
    r"quarter(ly)? eps|net (sales|income|revenue).{0,80}\b(q[1-4]|quarter|first half|nine months)\b", re.I)


def looks_like_results(text):
    """Filtro barato para 6-K: busca expresiones de resultados en el principio del documento."""
    head = text[:6000]
    if re.search(r"share repurchase|buy-?back programme|transactions in own shares|notice (convening|of)|"
                 r"major shareholder|managers'? transactions", head[:800], re.I):
        return False
    return bool(RESULTS_RE.search(head))


# ---------------------------------------------------------------- Europa
# Fuentes de resultados para empresas europeas sin 6-K en la SEC.
# - Londres (.L): Investegate, anuncios regulatorios (RNS) de cualquier empresa.
# - Resto: la web de inversores de cada empresa (se vigila la aparición de enlaces nuevos).
EU_SOURCES = {
    "ADYEN.AS": {"name": "Adyen", "sector": "Europa · Pagos", "page": "https://investors.adyen.com/financials",
                 "pattern": r"https?://(?:www\.)?adyen\.com/press-and-media/adyen-publishes-[^\"'#?]+|/financials/(?:q[1-4]|h[12])-20\d\d[^\"'#?]*"},
    # Empresas que distribuyen sus comunicados por MFN (mfn.se): página por empresa con el texto completo
    "RMS.PA": {"name": "Hermès", "sector": "Europa · Lujo", "mfn": "hermes"},
    "KER.PA": {"name": "Kering", "sector": "Europa · Lujo", "mfn": "kering"},
    "MC.PA": {"name": "LVMH", "sector": "Europa · Lujo", "page": "https://www.lvmh.com/en/investors",
              "pattern": r"/en/financial-calendar/20\d\d-[a-z0-9-]*(?:revenue|results)[a-z0-9-]*"},
}
EU_COUNTRY = {"L": "🇬🇧", "PA": "🇫🇷", "AS": "🇳🇱", "CO": "🇩🇰", "MC": "🇪🇸", "DE": "🇩🇪", "F": "🇩🇪", "MI": "🇮🇹",
              "SW": "🇨🇭", "BR": "🇧🇪", "ST": "🇸🇪", "HE": "🇫🇮", "OL": "🇳🇴", "LS": "🇵🇹", "VI": "🇦🇹", "IR": "🇮🇪"}
BROWSER_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; earnings-alerts)",
                   "Accept": "text/html,application/xhtml+xml,application/pdf,*/*;q=0.8", "Accept-Language": "en-GB,en;q=0.9"}
RNS_RESULTS_RE = re.compile(r"results|trading (update|statement)|interim (report|management)|half[- ]year|"
                            r"\bq[1-4]\b|quarter|preliminary|annual (financial )?report", re.I)
RNS_NOT_RESULTS_RE = re.compile(r"notice of|date of|timing|conference call|webcast|presentation (details|of)|"
                                r"transaction in own shares|holding\(s\)|director|voting rights|block listing", re.I)


def eu_supported(ticker):
    return ticker.endswith(".L") or ticker in EU_SOURCES


def _web_get(url):
    r = SESSION.get(url, headers=BROWSER_HEADERS, timeout=45)
    r.raise_for_status()
    return r


def period_key(text):
    """'q3-2026', 'h1-2026'… para no avisar dos veces del mismo periodo (Adyen enlaza nota y página)."""
    t = text.lower()
    m = re.search(r"\b(q[1-4]|h[12])[-_ ]?(20\d\d)", t) or re.search(r"(20\d\d)[-_ ](q[1-4]|h[12])\b", t)
    if m:
        a, b = m.groups()
        return f"{a}-{b}" if a.startswith(("q", "h")) else f"{b}-{a}"
    m = re.search(r"(20\d\d)[-_ ](?:first|1st)[-_ ]half", t)
    if m:
        return f"h1-{m.group(1)}"
    year = re.search(r"\b(20\d\d)\b", t)
    if year:
        for words, code in ((r"first quarter|three months", "q1"), (r"second quarter", "q2"),
                            (r"third quarter|nine months|9 months", "q3"), (r"fourth quarter", "q4"),
                            (r"first[- ]half|half[- ]year|interim|six months", "h1"),
                            (r"full[- ]year|annual|twelve months", "fy")):
            if re.search(words, t):
                return f"{code}-{year.group(1)}"
    m = re.search(r"(20\d\d)[-_ ](?:annual|full[-_ ]year)", t)
    return f"fy-{m.group(1)}" if m else text


def investegate_list(tidm, pages=1):
    """[(id, fecha, título, url)] de los anuncios RNS de una empresa de Londres (más recientes primero)."""
    from bs4 import BeautifulSoup
    out = []
    for page in range(1, pages + 1):
        url = f"https://www.investegate.co.uk/company/{tidm}" + (f"?page={page}" if page > 1 else "")
        soup = BeautifulSoup(_web_get(url).text, "lxml")
        links = soup.select("a.announcement-link")
        if not links:
            break
        out.extend(_investegate_rows(links))
    return out


def _investegate_rows(links):
    out = []
    for a in links:
        tds = a.find_parent("tr").find_all("td") if a.find_parent("tr") else []
        date = tds[0].get_text(strip=True) if tds else ""
        out.append((a["href"], date, a.get_text(" ", strip=True), a["href"]))
    return out


def investegate_text(url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(_web_get(url).text, "lxml")
    box = soup.select_one("div.news-window") or soup.select_one("div.aspose-root") or soup.body
    return html_to_text(str(box))


MFN_RESULTS_RE = re.compile(r"results|revenue|sales|quarter|half[- ]year|first half|nine months|trading update|"
                            r"interim report", re.I)
MFN_NOT_RE = re.compile(r"availability|notice|webcast|conference call|statement number|voting rights|"
                        r"share buyback|transactions? in own shares|disclosure of trading|liquidity", re.I)


def mfn_list(slug):
    """[(url, título)] de los comunicados de una empresa en MFN (más recientes primero)."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(_web_get(f"https://mfn.se/all/a/{slug}").text, "lxml")
    out, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"] if a["href"].startswith("http") else "https://mfn.se" + a["href"]
        title = a.get_text(" ", strip=True)
        if f"/a/{slug}/" in href and title and href not in seen:
            seen.add(href)
            out.append((href, title))
    return out


def mfn_items(slug):
    """Comunicados de resultados en MFN: [(clave de periodo, título, url)]."""
    return [(period_key(t), t, u) for u, t in mfn_list(slug) if MFN_RESULTS_RE.search(t) and not MFN_NOT_RE.search(t)]


def mfn_text(url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(_web_get(url).text, "lxml")
    box = soup.find("main") or soup.find("article") or soup.body
    return html_to_text(str(box))


def ir_links(cfg):
    """Enlaces de la web de inversores que encajan con el patrón de publicaciones de resultados."""
    from urllib.parse import urljoin
    html = _web_get(cfg["page"]).text
    seen, out = set(), []
    for m in re.finditer(cfg["pattern"], html, re.I):
        url = urljoin(cfg["page"], m.group(0))
        if url not in seen:
            seen.add(url)
            out.append(url)
    return out


def _period_tokens(text):
    k = period_key(text)
    m = re.fullmatch(r"(q[1-4]|h[12]|fy)-(20\d\d)", k)
    return m.groups() if m else None


def period_rank(k):
    """Orden cronológico de una clave de periodo ('q3-2026' > 'h1-2026' > 'fy-2025')."""
    m = re.fullmatch(r"(q[1-4]|h[12]|fy)-(20\d\d)", k or "")
    if not m:
        return (0, 0)
    order = {"q1": 1, "h1": 2, "q2": 2, "q3": 3, "h2": 4, "q4": 4, "fy": 4}
    return (int(m.group(2)), order[m.group(1)])


PERIOD_WORDS = {"q1": r"first quarter|\bq1\b|three months|1st quarter",
                "q2": r"second quarter|\bq2\b|first half|half[- ]year|\bh1\b|six months",
                "q3": r"third quarter|\bq3\b|nine months|9 months|3rd quarter",
                "q4": r"fourth quarter|\bq4\b|full[- ]year|annual|twelve months",
                "h1": r"first half|\bh1\b|half[- ]year|six months|interim",
                "h2": r"second half|\bh2\b|full[- ]year|annual",
                "fy": r"full[- ]year|annual|fiscal year"}


def mentions_period(text, tokens):
    """El texto habla del periodo esperado (año + trimestre/semestre) en su parte principal."""
    if not tokens:
        return True
    head = text[:15000]
    return tokens[1] in head and bool(re.search(PERIOD_WORDS[tokens[0]], head, re.I))


def ir_document(url, depth=0, tokens=None):
    """Texto del documento de resultados de una publicación de la web de inversores.
    Orden: nota de prensa enlazada > PDF del mismo periodo > texto de la página.
    Devuelve (texto, url_documento) o (None, None) si todavía no está publicado."""
    from urllib.parse import urljoin
    from bs4 import BeautifulSoup
    tokens = tokens or _period_tokens(url)
    r = _web_get(url)
    if r.content[:4] == b"%PDF" or "pdf" in r.headers.get("content-type", ""):
        text = document_to_text("doc.pdf", r.content)
        return (text, url) if len(text) > 2000 and mentions_period(text, tokens) else (None, None)
    html = r.text.replace("\\u002F", "/").replace("\\/", "/")
    soup = BeautifulSoup(html, "lxml")
    press, docs = {}, {}
    for a in soup.find_all("a", href=True):
        href, label = urljoin(url, a["href"]), a.get_text(" ", strip=True)
        if re.search(r"\.pdf($|\?)|/download($|\?)|/api/asset/", href, re.I):
            docs.setdefault(href, label)
        elif depth == 0 and (re.search(r"/publications/[a-z0-9-]{12,}|press-release", href, re.I)
                             or re.fullmatch(r"press release", label, re.I)) and "?" not in href:
            press.setdefault(href, label)
    for u in re.findall(r'["\'](https?://[^"\'\s]+\.pdf)', html, re.I):
        docs.setdefault(u, "")

    def rank(item):
        u, label = item
        flat = re.sub(r"[^a-z0-9]", "", (u + " " + label).lower())
        same_period = bool(tokens) and tokens[0] in flat and tokens[1] in flat
        doc_kind = re.search(r"press|release|communiqu|letter|update|results|revenue", u + " " + label, re.I)
        slides = re.search(r"presentation|slides", u + " " + label, re.I)
        return (1 if slides else 0, 0 if same_period else 1, 0 if doc_kind else 1)

    # 1. Nota de prensa enlazada (p. ej. LVMH: calendario -> /publications/continued-growth-in-the-third-quarter-...)
    for href, label in sorted(press.items(), key=lambda i: 0 if re.search("press release", i[1], re.I) else 1)[:2]:
        try:
            text, doc_url = ir_document(href, depth=1, tokens=tokens)
            if text:
                return text, doc_url
        except Exception as e:
            log(f"  nota de prensa no legible {href}: {e}")
    # 2. PDF del periodo (las presentaciones, al final: suelen ser imágenes con poco texto)
    for doc_url, _label in sorted(docs.items(), key=rank)[:3]:
        try:
            d = _web_get(doc_url)
            if d.content[:4] != b"%PDF" and "pdf" not in d.headers.get("content-type", ""):
                continue
            text = document_to_text("doc.pdf", d.content)
            if len(text) > 2000 and mentions_period(text, tokens):
                return text, doc_url
        except Exception as e:
            log(f"  documento no legible {doc_url}: {e}")
    # 3. Texto de la propia página, solo si habla del periodo correcto
    main = soup.find("main") or soup.find("article") or soup.body or soup
    text = html_to_text(str(main))
    if len(text) > (3000 if depth else 6000) and RESULTS_RE.search(text) and mentions_period(text, tokens):
        return text, url
    return None, None


def eu_candidates(conn, ticker):
    """Publicaciones nuevas de una empresa europea: [(clave, título, url)]. La primera vez solo guarda la línea base."""
    key = f"eu_seen_{ticker}"
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    seen = set(json.loads(row[0])) if row else None
    if ticker.endswith(".L"):
        items = [(i, f"{t} ({d})", u) for i, d, t, u in investegate_list(ticker[:-2])
                 if RNS_RESULTS_RE.search(t) and not RNS_NOT_RESULTS_RE.search(t)]
    elif "mfn" in EU_SOURCES[ticker]:
        items = mfn_items(EU_SOURCES[ticker]["mfn"])
    else:
        items = [(period_key(u), u.rsplit("/", 1)[-1], u) for u in ir_links(EU_SOURCES[ticker])]
    if seen is None:                       # primera vez: todo lo existente es historia
        conn.execute("REPLACE INTO meta VALUES (?, ?, ?)",
                     (key, json.dumps([i[0] for i in items]), datetime.now(timezone.utc).isoformat()))
        conn.commit()
        log(f"  {ticker}: línea base guardada ({len(items)} publicaciones anteriores)")
        return []
    new, keys = [], set()
    for k, title, url in items:
        if k not in seen and k not in keys:
            keys.add(k)
            new.append((k, title, url))
    return new


def eu_mark_seen(conn, ticker, k):
    key = f"eu_seen_{ticker}"
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    seen = json.loads(row[0]) if row else []
    seen.append(k)
    conn.execute("REPLACE INTO meta VALUES (?, ?, ?)", (key, json.dumps(seen[-300:]), datetime.now(timezone.utc).isoformat()))
    conn.commit()


def eu_company(ticker):
    cfg = EU_SOURCES.get(ticker, {})
    suffix = ticker.rsplit(".", 1)[-1]
    return {"ticker": ticker, "name": cfg.get("name", ticker.rsplit(".", 1)[0]), "cik": 0,
            "sector": cfg.get("sector", f"Europa {EU_COUNTRY.get(suffix, '')}".strip())}


def eu_analyze(conn, ticker, k, url, notify=True, dry_run=False):
    """Descarga, analiza y avisa de una publicación europea. True si era de resultados y se ha enviado."""
    company = eu_company(ticker)
    if ticker.endswith(".L"):
        text, doc_url = investegate_text(url), url
    elif "mfn" in EU_SOURCES.get(ticker, {}):
        text, doc_url = mfn_text(url), url
    else:
        text, doc_url = ir_document(url)
        if not text:
            log(f"  {ticker}: {url} aún sin documento; se reintenta más tarde")
            return False
    eu_mark_seen(conn, ticker, k)
    if not looks_like_results(text) and not RESULTS_RE.search(text[:20000]):
        log(f"  {ticker}: {url} no parece de resultados")
        return False
    today = datetime.now(timezone.utc).date().isoformat()
    filing = {"accession": f"eu:{ticker}:{k}", "filing_date": today, "acceptance": datetime.now(timezone.utc).isoformat(),
              "form": "EU"}
    consensus = None
    prompt = ANALYSIS_PROMPT.format(name=company["name"], ticker=ticker, sector=company["sector"], date=today,
                                    consensus_line="No hay dato de consenso disponible.", doc=text[:MAX_DOC_CHARS])
    msg = claude_message(prompt, MODEL_FAST, 4000, tools=[ANALYSIS_TOOL])
    analysis = extract_analysis(msg)
    if analysis.get("es_resultados") is False:
        log(f"  {ticker}: Claude indica que no es de resultados")
        return False
    analysis["consenso_bpa"] = consensus
    conn.execute("""REPLACE INTO filings (accession, cik, ticker, name, sector, filing_date, acceptance, url, analysis,
                    status, attempts, input_tokens, output_tokens, created_at, notified, call_status)
                    VALUES (?,?,?,?,?,?,?,?,?, 'ok', 1, ?, ?, ?, ?, 'manual')""",
                 (filing["accession"], 0, ticker, company["name"], company["sector"], today, filing["acceptance"],
                  doc_url, json.dumps(analysis, ensure_ascii=False), msg.usage.input_tokens, msg.usage.output_tokens,
                  filing["acceptance"], 1 if notify else 0))
    conn.commit()
    if notify:
        row = {**company, "filing_date": today, "url": doc_url, "analysis": analysis}
        send_blocks("📢 <b>Resultados publicados</b>", [company_detail(row)], dry_run)
    return True


def run_europe(conn, dry_run=False):
    tickers = sorted(t for t in WATCHLIST if eu_supported(t))
    if not tickers:
        return
    log(f"Europa: revisando {', '.join(tickers)}")
    for t in tickers:
        try:
            for k, title, url in eu_candidates(conn, t)[:3]:
                log(f"  {t}: nueva publicación {title}")
                eu_analyze(conn, t, k, url, dry_run=dry_run)
        except Exception as e:
            log(f"  Europa {t}: {e}")


def run_eu_latest(conn, ticker, dry_run=False):
    """Prueba: analiza la publicación de resultados más reciente de una empresa europea."""
    ticker = ticker.upper()
    if not eu_supported(ticker):
        sys.exit(f"No tengo fuente para {ticker}")
    eu_candidates(conn, ticker)            # guarda la línea base si es la primera vez
    if ticker.endswith(".L"):
        items = [(i, t, u) for i, d, t, u in investegate_list(ticker[:-2], pages=6)
                 if RNS_RESULTS_RE.search(t) and not RNS_NOT_RESULTS_RE.search(t)]
    elif "mfn" in EU_SOURCES[ticker]:
        items = sorted(mfn_items(EU_SOURCES[ticker]["mfn"]), key=lambda i: period_rank(i[0]), reverse=True)
    else:
        items = sorted(((period_key(u), u, u) for u in ir_links(EU_SOURCES[ticker])),
                       key=lambda i: period_rank(i[0]), reverse=True)
    for k, title, url in items[:4]:
        log(f"Probando {title}")
        if eu_analyze(conn, ticker, k, url, dry_run=dry_run):
            conn.execute("UPDATE filings SET summarized=1 WHERE accession=?", (f"eu:{ticker}:{k}",))  # prueba manual
            conn.commit()
            return
    send_telegram(f"⚠️ No he encontrado una publicación de resultados reciente de <b>{esc(ticker)}</b>.", dry_run)


# ---------------------------------------------------------------- flujos principales
def process(conn, company, filing):
    """Descarga y analiza un filing; guarda el resultado. Devuelve la fila o None."""
    acc = filing["accession"]
    row = conn.execute("SELECT status, attempts FROM filings WHERE accession=?", (acc,)).fetchone()
    if row and (row[0] in ("ok", "skip") or row[1] >= MAX_ATTEMPTS):
        return None
    if not row:
        conn.execute("""INSERT INTO filings (accession, cik, ticker, name, sector, filing_date, acceptance, created_at)
                        VALUES (?,?,?,?,?,?,?,?)""",
                     (acc, company["cik"], company["ticker"], company["name"], company["sector"],
                      filing["filing_date"], filing["acceptance"], datetime.now(timezone.utc).isoformat()))
        conn.commit()
    try:
        log(f"Analizando {company['ticker']} ({filing['filing_date']})")
        doc, url = fetch_press_release(company["cik"], filing)
        if filing.get("form") == "6-K" and not looks_like_results(doc):
            conn.execute("UPDATE filings SET status='skip', url=? WHERE accession=?", (url, acc))
            conn.commit()
            global LAST_SKIPPED
            LAST_SKIPPED = True
            log(f"  {company['ticker']}: 6-K que no es de resultados, se ignora")
            return None
        analysis, tin, tout = analyze(company, filing, doc)
        if filing.get("form") == "6-K" and analysis.get("es_resultados") is False:
            conn.execute("UPDATE filings SET status='skip', url=? WHERE accession=?", (url, acc))
            conn.commit()
            LAST_SKIPPED = True
            log(f"  {company['ticker']}: Claude indica que el 6-K no es de resultados")
            return None
        conn.execute("""UPDATE filings SET url=?, analysis=?, status='ok', attempts=attempts+1,
                        input_tokens=?, output_tokens=? WHERE accession=?""",
                     (url, json.dumps(analysis, ensure_ascii=False), tin, tout, acc))
        conn.commit()
        return {**company, "filing_date": filing["filing_date"], "url": url, "analysis": analysis,
                "tin": tin, "tout": tout}
    except Exception as e:
        global LAST_ERROR
        LAST_ERROR = str(e)[:300]
        log(f"  error con {company['ticker']}: {e}")
        conn.execute("UPDATE filings SET status='error', attempts=attempts+1 WHERE accession=?", (acc,))
        conn.commit()
        return None


LAST_ERROR = ""
LAST_SKIPPED = False


def run_check(conn):
    companies = load_companies(conn)
    since = (datetime.now(timezone.utc).date() - timedelta(days=LOOKBACK_DAYS)).isoformat()
    log(f"Revisando {len(companies)} empresas (8-K item 2.02 y 6-K de la watchlist desde {since})")
    done, tin, tout, errors = 0, 0, 0, []
    for company in companies:
        try:
            filings = earnings_filings(company, since)
        except Exception as e:
            log(f"  {company['ticker']}: {e}")
            continue
        for filing in filings:
            before = conn.execute("SELECT status, attempts FROM filings WHERE accession=?",
                                  (filing["accession"],)).fetchone()
            if before and (before[0] in ("ok", "skip") or before[1] >= MAX_ATTEMPTS):
                continue
            global LAST_SKIPPED
            LAST_SKIPPED = False
            result = process(conn, company, filing)
            if result:
                done, tin, tout = done + 1, tin + result["tin"], tout + result["tout"]
                if company["ticker"].upper() in WATCHLIST:
                    send_blocks("📢 <b>Resultados publicados</b>", [company_detail(result)])
                    conn.execute("UPDATE filings SET notified=1 WHERE accession=?", (filing["accession"],))
                    conn.commit()
            elif not LAST_SKIPPED:
                errors.append(f"{company['ticker']}: {LAST_ERROR}")
    log(f"Hecho: {done} resultados analizados · tokens entrada {tin:,} · salida {tout:,}")
    if errors:
        lines = "\n".join(f"• {esc(e[:150])}" for e in errors[:10])
        more = f"\n… y {len(errors) - 10} más" if len(errors) > 10 else ""
        send_telegram(f"⚠️ <b>Earnings Alerts</b>: {len(errors)} análisis fallidos (se reintentan solos)\n{lines}{more}")
    run_europe(conn)
    if summary_due(conn):          # por si la ejecución programada del resumen se ha retrasado o perdido
        run_summary(conn)
    if ALPHAVANTAGE_API_KEY:
        run_calls(conn)


def _load_row(conn, accession):
    r = conn.execute("""SELECT accession, ticker, name, sector, filing_date, acceptance, url, analysis,
                               call_tries FROM filings WHERE accession=?""", (accession,)).fetchone()
    return {"accession": r[0], "ticker": r[1], "name": r[2], "sector": r[3], "filing_date": r[4],
            "acceptance": r[5], "url": r[6], "analysis": json.loads(r[7]), "call_tries": r[8] or 0}


def try_call(conn, row, dry_run=False):
    """Busca la transcripción del trimestre, la analiza y envía el mensaje completo. True si lo consigue."""
    for quarter in quarter_candidates(row["analysis"].get("periodo"), row["filing_date"]):
        log(f"  {row['ticker']}: buscando transcripción {quarter}")
        transcript = fetch_transcript(conn, row["ticker"], quarter)
        if not transcript:
            continue
        call = analyze_call(row, transcript)
        if not call.get("corresponde", True):
            log(f"  {row['ticker']}: la transcripción {quarter} es de otro trimestre")
            continue
        conn.execute("UPDATE filings SET call_status='done', call_analysis=? WHERE accession=?",
                     (json.dumps(call, ensure_ascii=False), row["accession"]))
        conn.commit()
        send_full_update({**row, "call": call}, dry_run)
        return True
    return False


def run_calls(conn, dry_run=False):
    """Para la watchlist: busca la transcripción de la call de los resultados recientes."""
    if not WATCHLIST:
        return
    now = datetime.now(timezone.utc)
    since = (now.date() - timedelta(days=5)).isoformat()
    marks = ",".join("?" * len(WATCHLIST))
    pending = conn.execute(f"""SELECT accession, acceptance, call_last_try FROM filings
                               WHERE status='ok' AND call_status='pending' AND filing_date >= ?
                               AND call_tries < ? AND UPPER(ticker) IN ({marks})""",
                           (since, CALL_MAX_TRIES, *sorted(WATCHLIST))).fetchall()
    for accession, acceptance, last_try in pending:
        try:
            published = datetime.fromisoformat(acceptance.replace("Z", "+00:00"))
            if published.tzinfo is None:
                published = published.replace(tzinfo=timezone.utc)
        except (AttributeError, ValueError):
            published = now - timedelta(hours=CALL_MIN_HOURS)
        if now - published < timedelta(hours=CALL_MIN_HOURS):
            continue
        if last_try and now - datetime.fromisoformat(last_try) < timedelta(hours=CALL_RETRY_HOURS):
            continue
        row = _load_row(conn, accession)
        try:
            found = try_call(conn, row, dry_run)
        except AlphaVantageLimit as e:
            log(f"Alpha Vantage sin cupo por hoy ({e}); se reintenta más tarde")
            return
        except Exception as e:
            log(f"  error con la call de {row['ticker']}: {e}")
            found = False
        if found:
            continue
        tries = row["call_tries"] + 1
        status = "pending" if tries < CALL_MAX_TRIES else "not_found"
        conn.execute("UPDATE filings SET call_tries=?, call_last_try=?, call_status=? WHERE accession=?",
                     (tries, now.isoformat(), status, accession))
        conn.commit()
        if status == "not_found":
            send_telegram(f"🎙️ No he encontrado la transcripción de la call de <b>{esc(row['ticker'])}</b> "
                          f"({esc(row['analysis'].get('periodo'))}). Puedes analizarla a mano más adelante.", dry_run)


SUMMARY_TIME = os.environ.get("SUMMARY_TIME", "07:45")   # hora de Madrid a partir de la cual se envía el resumen


def _madrid_now():
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("Europe/Madrid"))


def summary_due(conn):
    """True si toca el resumen: de martes a sábado, a partir de SUMMARY_TIME (Madrid) y aún no enviado hoy."""
    now = _madrid_now()
    if now.isoweekday() not in (2, 3, 4, 5, 6) or now.strftime("%H:%M") < SUMMARY_TIME:
        return False
    row = conn.execute("SELECT value FROM meta WHERE key='summary_date'").fetchone()
    return not row or row[0] != now.date().isoformat()


def run_summary(conn, dry_run=False):
    rows = conn.execute("""SELECT accession, ticker, name, sector, filing_date, url, analysis FROM filings
                           WHERE status='ok' AND summarized=0 ORDER BY sector, ticker""").fetchall()
    if not rows:
        log("No hay resultados nuevos para el resumen")
        if not dry_run:
            _mark_summary_sent(conn)
        return
    items = [{"accession": r[0], "ticker": r[1], "name": r[2], "sector": r[3], "filing_date": r[4],
              "url": r[5], "analysis": json.loads(r[6])} for r in rows]

    tones = [i["analysis"].get("tono") for i in items]
    today = datetime.now(timezone.utc).strftime("%d/%m/%Y")
    header = (f"📊 <b>Resultados · {today}</b>\n"
              f"{len(items)} empresas · 🟢 {tones.count('positivo')} · 🟡 {tones.count('neutro')} · "
              f"🔴 {tones.count('negativo')}")

    blocks = []
    if len(items) >= 3:
        try:
            compact = [{"ticker": i["ticker"], "sector": i["sector"],
                        **{k: i["analysis"].get(k) for k in
                           ("ingresos_musd", "crec_ingresos_pct", "crec_ingresos", "vs_consenso", "guidance", "guidance_detalle", "tono", "resumen", "alertas")}}
                       for i in items]
            msg = claude_message(DAILY_PROMPT.format(data=json.dumps(compact, ensure_ascii=False)), MODEL_SMART, 3000)
            reading = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text").strip()
            if msg.stop_reason == "max_tokens":          # cortada: nos quedamos con las viñetas completas
                bullets = [b for b in reading.split("•") if b.strip()]
                reading = "\n".join("• " + b.strip() for b in bullets[:-1]) if len(bullets) > 1 else ""
            if reading:
                blocks.append("🧭 <b>Lectura del día</b>\n" + esc(reading))
        except Exception as e:
            log(f"Sin lectura del día: {e}")

    by_sector = {}
    for i in items:
        by_sector.setdefault(i["sector"], []).append(i)
    for sector, group in sorted(by_sector.items(), key=lambda kv: -len(kv[1])):
        blocks.append(f"<b>— {esc(sector)} ({len(group)}) —</b>")
        blocks.extend(company_line(i) for i in group)

    send_blocks(header, blocks, dry_run)
    if not dry_run:
        conn.executemany("UPDATE filings SET summarized=1 WHERE accession=?", [(i["accession"],) for i in items])
        _mark_summary_sent(conn)
    log(f"Resumen enviado: {len(items)} empresas")


def _mark_summary_sent(conn):
    conn.execute("REPLACE INTO meta VALUES ('summary_date', ?, ?)",
                 (_madrid_now().date().isoformat(), datetime.now(timezone.utc).isoformat()))
    conn.commit()


def run_ticker(conn, ticker, dry_run=False):
    """Prueba: analiza el último resultado (últimos 120 días) de una empresa y lo envía."""
    companies = load_companies(conn)
    company = next((c for c in companies if c["ticker"].upper() == ticker.upper()), None)
    if not company:
        sys.exit(f"{ticker} no está en el S&P 500 ni en la watchlist")
    since = (datetime.now(timezone.utc).date() - timedelta(days=120)).isoformat()
    filings = earnings_filings(company, since)
    if not filings:
        sys.exit(f"No encuentro resultados de {ticker} en los últimos 120 días")
    filing = max(filings, key=lambda f: f["filing_date"])
    conn.execute("DELETE FROM filings WHERE accession=?", (filing["accession"],))  # fuerza el reanálisis
    result = process(conn, company, filing)
    if result:
        conn.execute("UPDATE filings SET summarized=1 WHERE accession=?", (filing["accession"],))  # prueba manual
        conn.commit()
        send_telegram(company_detail(result), dry_run)
    else:
        send_telegram(f"⚠️ No se pudo analizar <b>{esc(ticker)}</b>: {esc(LAST_ERROR)}", dry_run)
    return filing if result else None


def run_call_now(conn, ticker, dry_run=False):
    """Prueba: resultados + call del último trimestre de una empresa, sin esperar."""
    if not ALPHAVANTAGE_API_KEY:
        sys.exit("Falta ALPHAVANTAGE_API_KEY")
    filing = run_ticker(conn, ticker, dry_run)
    if not filing:
        return
    row = _load_row(conn, filing["accession"])
    try:
        if not try_call(conn, row, dry_run):
            send_telegram(f"🎙️ Alpha Vantage aún no tiene la transcripción de <b>{esc(ticker)}</b> "
                          f"({esc(row['analysis'].get('periodo'))}).", dry_run)
    except Exception as e:
        send_telegram(f"⚠️ Error con la call de <b>{esc(ticker)}</b>: {esc(str(e)[:300])}", dry_run)


def main():
    p = argparse.ArgumentParser(description="Resultados del S&P 500 por Telegram")
    p.add_argument("--summary", action="store_true", help="envía el resumen de resultados pendientes")
    p.add_argument("--auto", action="store_true", help="con --summary: solo si toca (hora y día) y no se ha enviado hoy")
    p.add_argument("--ticker", help="analiza el último resultado de una empresa y lo envía")
    p.add_argument("--call", help="analiza el último resultado de una empresa junto con su call")
    p.add_argument("--inbox", action="store_true", help="analiza las transcripciones enviadas al bot por Telegram")
    p.add_argument("--eu", help="analiza la última publicación de resultados de una empresa europea (p. ej. LSEG.L)")
    p.add_argument("--test", action="store_true", help="mensaje de prueba a Telegram")
    p.add_argument("--dry-run", action="store_true", help="imprime en pantalla en lugar de enviar")
    args = p.parse_args()

    if args.test:
        send_telegram("✅ <b>Earnings Alerts</b> funcionando. Te llegará el resumen de resultados del S&amp;P 500 cada mañana.",
                      args.dry_run)
        return
    if args.inbox:
        if not ANTHROPIC_API_KEY:
            sys.exit("Falta ANTHROPIC_API_KEY")
        run_inbox(db_connect(), args.dry_run)
        return
    missing = [n for n, v in (("SEC_USER_AGENT", SEC_USER_AGENT), ("ANTHROPIC_API_KEY", ANTHROPIC_API_KEY)) if not v]
    if not args.summary and missing:
        sys.exit(f"Faltan variables: {', '.join(missing)}")

    conn = db_connect()
    if args.eu:
        run_eu_latest(conn, args.eu, args.dry_run)
    elif args.call:
        run_call_now(conn, args.call, args.dry_run)
    elif args.ticker:
        run_ticker(conn, args.ticker, args.dry_run)
    elif args.summary and args.auto and not summary_due(conn):
        log("El resumen de hoy ya se ha enviado o aún no es la hora")
    elif args.summary:
        run_summary(conn, args.dry_run)
    else:
        run_check(conn)


if __name__ == "__main__":
    main()

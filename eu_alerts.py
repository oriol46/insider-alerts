#!/usr/bin/env python3
"""
Alertas de operaciones de directivos europeos (MAR art. 19) por Telegram.

Fuentes (fase 1):
  🇳🇱 Holanda  — registro de la AFM (lista + página de detalle)
  🇩🇪 Alemania — base de datos de la BaFin (exportación CSV)
  🇨🇭 Suiza    — SIX Exchange Regulation (API JSON; no publica nombres)

Se ejecuta automáticamente desde insider_alerts.py (misma base de datos, mismo Telegram).
"""

import csv
import hashlib
import html
import logging
import re
import time
from datetime import date, datetime, timedelta, timezone

import insider_alerts as ia

log = logging.getLogger("insider.eu")

# ═══════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN EUROPA
# ═══════════════════════════════════════════════════════════════════════
EU_CONFIG = {
    "sources": ["AFM", "BAFIN", "SIX"],     # Holanda, Alemania, Suiza

    # Índices: solo se avisa de empresas que estén en ALGUNO de estos ([] = todas)
    # Disponibles: "STOXX600", "SX5E" (EURO STOXX 50), "IBEX35", "CAC40", "DAX"
    "indices": ["STOXX600", "SX5E", "IBEX35", "CAC40", "DAX"],

    "min_buy_value_eur": 100_000,           # importe mínimo de la compra (en euros)
    "alert_sells": True,
    "min_sell_value_eur": 100_000,          # importe mínimo de la venta (en euros)
    "include_related": True,                # incluir operaciones de personas vinculadas

    "max_age_days": 10,                     # ignorar operaciones con fecha más antigua
    "cluster_window_days": 30,
}

# ═══════════════════════════════════════════════════════════════════════
# Índices europeos
# ═══════════════════════════════════════════════════════════════════════
EU_INDEX_SOURCES = {
    "STOXX600": ("ishares", "https://www.ishares.com/ch/individual/en/products/251931/"
                 "ishares-stoxx-europe-600-ucits-etf-de/1495092304805.ajax"
                 "?fileType=csv&fileName=EXSA_holdings&dataType=fund", 550, "STOXX 600"),
    "SX5E": ("wiki", "https://en.wikipedia.org/wiki/EURO_STOXX_50", 45, "EURO STOXX 50"),
    "IBEX35": ("wiki", "https://en.wikipedia.org/wiki/IBEX_35", 30, "IBEX 35"),
    "CAC40": ("wiki", "https://en.wikipedia.org/wiki/CAC_40", 35, "CAC 40"),
    "DAX": ("wiki", "https://en.wikipedia.org/wiki/DAX", 35, "DAX"),
}

# Bolsa (nombre en el CSV de iShares) → sufijo de Yahoo
ISHARES_EXCH = {
    "Athens Exchange S.A. Cash Market": "AT", "Bolsa De Madrid": "MC", "Borsa Italiana": "MI",
    "Deutsche Boerse Xetra": "DE", "Xetra": "DE", "Euronext Amsterdam": "AS",
    "Irish Stock Exchange - All Market": "IR", "London Stock Exchange": "L",
    "Nasdaq Omx Helsinki Ltd.": "HE", "Nasdaq Omx Nordic": "ST",
    "Nyse Euronext - Euronext Brussels": "BR", "Nyse Euronext - Euronext Lisbon": "LS",
    "Nyse Euronext - Euronext Paris": "PA", "Omx Nordic Exchange Copenhagen A/S": "CO",
    "Oslo Bors Asa": "OL", "SIX Swiss Exchange": "SW",
    "Warsaw Stock Exchange/Equities/Main Market": "WA", "Wiener Boerse Ag": "VI",
}

# Código de bolsa de OpenFIGI (Bloomberg) → sufijo de Yahoo
FIGI_SUFFIX = {
    "NA": "AS", "GY": "DE", "GR": "DE", "FP": "PA", "SM": "MC", "LN": "L", "IM": "MI",
    "SW": "SW", "SE": "SW", "DC": "CO", "SS": "ST", "FH": "HE", "NO": "OL", "BB": "BR",
    "PL": "LS", "AV": "VI", "ID": "IR", "GA": "AT", "PW": "WA",
}
# Bolsa preferida según el país del ISIN
COUNTRY_PREF = {
    "NL": ["NA"], "DE": ["GY", "GR"], "FR": ["FP"], "ES": ["SM"], "GB": ["LN"], "IT": ["IM"],
    "CH": ["SW", "SE"], "DK": ["DC"], "SE": ["SS"], "FI": ["FH"], "NO": ["NO"], "BE": ["BB"],
    "PT": ["PL"], "AT": ["AV"], "IE": ["ID", "LN"], "LU": ["NA", "FP", "GY"],
    "JE": ["LN"], "GG": ["LN"], "IM": ["LN"],
}
GENERAL_PREF = ["NA", "GY", "FP", "SM", "LN", "IM", "SW", "SE", "DC", "SS", "FH",
                "NO", "BB", "PL", "AV", "ID", "GA", "PW", "GR"]

FLAGS = {"NL": "🇳🇱", "DE": "🇩🇪", "FR": "🇫🇷", "ES": "🇪🇸", "GB": "🇬🇧", "IT": "🇮🇹", "CH": "🇨🇭",
         "DK": "🇩🇰", "AT": "🇦🇹", "BE": "🇧🇪", "LU": "🇱🇺", "IE": "🇮🇪", "SE": "🇸🇪", "FI": "🇫🇮",
         "NO": "🇳🇴", "PT": "🇵🇹", "US": "🇺🇸", "JE": "🇯🇪", "GG": "🇬🇬", "PL": "🇵🇱", "GR": "🇬🇷"}
CUR_SYMBOL = {"EUR": "€", "USD": "$", "GBP": "£", "CHF": "CHF", "DKK": "DKK", "SEK": "SEK",
              "NOK": "NOK", "GBp": "GBp"}
# Solo se usan si Yahoo no responde
FX_FALLBACK = {"USD": 0.92, "GBP": 1.17, "CHF": 1.06, "DKK": 0.134, "SEK": 0.087, "NOK": 0.085}

SOURCE_NAME = {"AFM": "AFM", "BAFIN": "BaFin", "SIX": "SIX"}


# ═══════════════════════════════════════════════════════════════════════
# Utilidades
# ═══════════════════════════════════════════════════════════════════════
def eu_db(con):
    con.executescript("""
        CREATE TABLE IF NOT EXISTS eu_seen (id TEXT PRIMARY KEY, seen_at TEXT);
        CREATE TABLE IF NOT EXISTS eu_index_members (idx TEXT, key TEXT, sector TEXT);
        CREATE TABLE IF NOT EXISTS isin_map (isin TEXT PRIMARY KEY, yahoo TEXT, name TEXT, updated TEXT);
    """)
    con.commit()


def seen(con, i):
    return con.execute("SELECT 1 FROM eu_seen WHERE id=?", (i,)).fetchone() is not None


def mark(con, i):
    con.execute("INSERT OR IGNORE INTO eu_seen VALUES (?,?)", (i, datetime.now(timezone.utc).isoformat()))
    con.commit()


def has_history(con, prefix):
    return con.execute("SELECT 1 FROM eu_seen WHERE id LIKE ? LIMIT 1", (prefix + "%",)).fetchone() is not None


def num_eu(s):
    """'104.800,00 EUR' → 104800.0 · '52,40' → 52.4 · '4,86' → 4.86"""
    if s is None:
        return None
    m = re.search(r"-?[\d.,’' ]+", str(s))
    if not m:
        return None
    t = m.group(0).strip().replace("’", "").replace("'", "").replace(" ", "")
    if "," in t:
        t = t.replace(".", "").replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None


def currency_of(s, default="EUR"):
    m = re.search(r"\b([A-Z]{3})\b", s or "")
    return m.group(1) if m else default


def clean(s):
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", s or "")).split())


MONTHS = {"jan": 1, "feb": 2, "mar": 3, "mrt": 3, "apr": 4, "may": 5, "mei": 5, "jun": 6,
          "jul": 7, "aug": 8, "sep": 9, "oct": 10, "okt": 10, "nov": 11, "dec": 12}


def parse_date(s):
    s = (s or "").strip()
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%Y%m%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            pass
    m = re.match(r"(\d{1,2})\s+([A-Za-z]{3})[a-z]*\.?\s+(\d{4})", s)
    if m and m.group(2).lower() in MONTHS:
        return date(int(m.group(3)), MONTHS[m.group(2).lower()], int(m.group(1)))
    return None


def fmt_amount(x, cur):
    sym = CUR_SYMBOL.get(cur, cur)
    return f"{ia.fmt_num(x)} {sym}"


# ═══════════════════════════════════════════════════════════════════════
# Índices
# ═══════════════════════════════════════════════════════════════════════
def parse_ishares_eu(text):
    lines = text.lstrip("﻿").splitlines()
    start = next((i for i, l in enumerate(lines) if l.lstrip('"').startswith("Ticker")), None)
    if start is None:
        return []
    rows = []
    for row in csv.DictReader(lines[start:]):
        if (row.get("Asset Class") or "").strip() != "Equity":
            continue
        suffix = ISHARES_EXCH.get((row.get("Exchange") or "").strip())
        tk = (row.get("Ticker") or "").strip()
        if suffix and tk:
            rows.append((ia.norm_ticker(tk + suffix), (row.get("Sector") or "").strip() or None))
    return rows


def load_eu_indices(con, http):
    wanted = [k for k in EU_CONFIG["indices"] if k in EU_INDEX_SOURCES]
    now = datetime.now(timezone.utc)
    for k in wanted:
        row = con.execute("SELECT value FROM meta WHERE key=?", (f"euidx_{k}",)).fetchone()
        if row and now - datetime.fromisoformat(row[0]) < timedelta(hours=ia.CONFIG["index_refresh_hours"]):
            continue
        kind, url, min_rows, _ = EU_INDEX_SOURCES[k]
        rows = []
        try:
            r = http.get(url, timeout=40)
            r.raise_for_status()
            if kind == "ishares":
                rows = parse_ishares_eu(r.text)
            else:
                rows = [(key, sector) for key, _, sector in ia.parse_wiki_members(r.text, min_rows)]
        except Exception as ex:
            log.warning("Índice %s: error descargando (%s)", k, ex)
        if len(rows) < min_rows:
            log.warning("Índice %s: lista incompleta (%d), se mantiene la anterior", k, len(rows))
            continue
        con.execute("DELETE FROM eu_index_members WHERE idx=?", (k,))
        con.executemany("INSERT INTO eu_index_members VALUES (?,?,?)", [(k, key, s) for key, s in rows])
        con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (f"euidx_{k}", now.isoformat()))
        con.commit()
        log.info("Índice %s actualizado: %d valores", k, len(rows))
    out = {}
    for k in wanted:
        keys = {r[0] for r in con.execute("SELECT key FROM eu_index_members WHERE idx=?", (k,))}
        if keys:
            out[k] = keys
        else:
            log.warning("Índice %s sin datos: no se usará para filtrar", k)
    return out


def index_labels(key, index_map):
    return [EU_INDEX_SOURCES[k][3] for k, keys in index_map.items() if key and key in keys]


def sector_for(con, key):
    if not key:
        return None
    row = con.execute("SELECT sector FROM eu_index_members WHERE key=? AND sector IS NOT NULL "
                      "ORDER BY CASE idx WHEN 'STOXX600' THEN 0 ELSE 1 END LIMIT 1", (key,)).fetchone()
    return row[0] if row else None


# ═══════════════════════════════════════════════════════════════════════
# ISIN → ticker de Yahoo (OpenFIGI, con caché)
# ═══════════════════════════════════════════════════════════════════════
def _pick_listing(isin, data, all_keys):
    cands = [(d.get("exchCode"), d.get("ticker"), d.get("name")) for d in data
             if d.get("exchCode") in FIGI_SUFFIX and d.get("ticker")]
    pref = COUNTRY_PREF.get(isin[:2], []) + [x for x in GENERAL_PREF if x not in COUNTRY_PREF.get(isin[:2], [])]
    cands.sort(key=lambda c: pref.index(c[0]) if c[0] in pref else 99)

    def yahoo(c):
        return c[1].replace("/", "-").replace(" ", "-") + "." + FIGI_SUFFIX[c[0]]

    for c in cands:  # 1º: la cotización que esté en algún índice
        if ia.norm_ticker(yahoo(c)) in all_keys:
            return yahoo(c), c[2]
    if cands:
        return yahoo(cands[0]), cands[0][2]
    return None, (data[0].get("name") if data else None)


def map_isins(con, http, isins, all_keys):
    out, missing = {}, []
    week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    for isin in set(isins):
        row = con.execute("SELECT yahoo, name, updated FROM isin_map WHERE isin=?", (isin,)).fetchone()
        if row and (row[0] or row[2] > week_ago):
            out[isin] = (row[0], row[1])
        else:
            missing.append(isin)
    for i in range(0, len(missing), 10):
        batch = missing[i:i + 10]
        try:
            r = http.post("https://api.openfigi.com/v3/mapping", timeout=30,
                          json=[{"idType": "ID_ISIN", "idValue": x, "marketSecDes": "Equity"} for x in batch])
            if r.status_code == 429:
                log.info("OpenFIGI: límite de peticiones, se reintentará en la próxima pasada")
                break
            res = r.json()
            for isin, blk in zip(batch, res):
                yahoo, name = _pick_listing(isin, blk.get("data") or [], all_keys)
                out[isin] = (yahoo, name)
                con.execute("INSERT OR REPLACE INTO isin_map VALUES (?,?,?,?)",
                            (isin, yahoo, name, datetime.now(timezone.utc).isoformat()))
            con.commit()
        except Exception as ex:
            log.info("OpenFIGI: %s", ex)
        time.sleep(2.5)  # sin clave: ~25 peticiones/minuto
    return out


# ═══════════════════════════════════════════════════════════════════════
# Precios (Yahoo) y divisas
# ═══════════════════════════════════════════════════════════════════════
def yahoo_quote(http, symbol):
    for host in ("query1", "query2"):
        try:
            r = http.get(f"https://{host}.finance.yahoo.com/v8/finance/chart/{symbol}"
                         "?range=1y&interval=1d", timeout=20)
            if r.status_code != 200:
                continue
            res = r.json()["chart"]["result"][0]
            m, q = res["meta"], res["indicators"]["quote"][0]
            highs = [x for x in q.get("high") or [] if x]
            lows = [x for x in q.get("low") or [] if x]
            price = m.get("regularMarketPrice")
            if price:
                return {"price": float(price), "currency": m.get("currency"),
                        "hi": m.get("fiftyTwoWeekHigh") or (max(highs) if highs else None),
                        "lo": m.get("fiftyTwoWeekLow") or (min(lows) if lows else None)}
        except Exception as ex:
            log.info("Yahoo %s: %s", symbol, ex)
    return None


_FX = {}


def fx_eur(http, cur):
    cur = (cur or "EUR").upper()
    if cur == "EUR":
        return 1.0
    if cur not in _FX:
        q = yahoo_quote(http, f"{cur}EUR=X")
        _FX[cur] = q["price"] if q else FX_FALLBACK.get(cur)
    return _FX[cur]


# ═══════════════════════════════════════════════════════════════════════
# Fuentes
# Cada fuente devuelve [(id_visto, [operaciones])]; una operación es un dict con:
#   source, issuer, isin, person, role, related, side ('P'/'S'), date, price, qty,
#   value, currency, link, role_level
# ═══════════════════════════════════════════════════════════════════════
AFM_REG = "https://www.afm.nl/en/sector/registers/meldingenregisters/transacties-leidinggevenden-mar19-"
AFM_XML = "https://www.afm.nl/export.aspx?type=0ee836dc-5520-459d-bcf4-a4a689de6614&format=xml"


def parse_afm_detail(text, mid):
    t = re.sub(r"\s+", " ", text)
    fields = {}
    for label in ("Notifiable", "Issuing institution", "LEI", "Position/Status", "Transaction"):
        m = re.search(r"<span[^>]*>\s*" + re.escape(label) + r"\s*</span>\s*<span[^>]*>\s*<span[^>]*>(.*?)</span>", t)
        if m:
            fields[label] = clean(m.group(1))
    m = re.search(r"<h2[^>]*>\s*Transactions\s*</h2>(.*?)</table>", t)
    rows = []
    if m:
        for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", m.group(1)):
            cells = dict((clean(k), clean(v)) for k, v in
                         re.findall(r"<td[^>]*>\s*<span[^>]*>(.*?)</span>(.*?)</td>", tr))
            if cells:
                rows.append(cells)
    trades = {}
    for c in rows:
        inst = (c.get("Instrument type") or "").lower()
        if "aandeel" not in inst and "share" not in inst:
            continue
        if (c.get("Stock option program") or "").lower() in ("ja", "yes"):
            continue
        typ = (c.get("Transaction type") or "").lower()
        side = "P" if typ in ("koop", "aankoop", "purchase", "buy") else \
               "S" if typ in ("verkoop", "sale", "sell") else None
        price, qty = num_eu(c.get("Price")), num_eu(c.get("Quantity"))
        if not side or not price or not qty:
            continue
        t_ = trades.setdefault(side, {"qty": 0.0, "value": 0.0, "isin": c.get("ISIN"),
                                      "currency": c.get("Unit") or "EUR", "venue": c.get("Trading place")})
        t_["qty"] += qty
        t_["value"] += qty * price
    out = []
    for side, a in trades.items():
        out.append({
            "source": "AFM", "issuer": fields.get("Issuing institution", "?"), "isin": a["isin"],
            "person": fields.get("Notifiable", "?"), "role": fields.get("Position/Status", ""),
            "related": False, "side": side, "date": parse_date(fields.get("Transaction")),
            "price": a["value"] / a["qty"], "qty": a["qty"], "value": a["value"], "currency": a["currency"],
            "link": f"{AFM_REG}/details?id={mid}",
        })
    return out


def fetch_afm(con, http):
    r = http.get(AFM_REG, timeout=40)
    r.raise_for_status()
    ids = list(dict.fromkeys(re.findall(r"details\?id=([0-9A-F-]{20,})", r.text)))
    new = [i for i in ids if not seen(con, "AFM:" + i)]
    if ids and len(new) == len(ids) and has_history(con, "AFM:"):
        # Toda la primera página es nueva: puede haber más; se mira el registro completo
        x = http.get(AFM_XML, timeout=90)
        more = re.findall(r"<meldingid>([^<]+)</meldingid>", x.text)[:150]
        new = [i for i in dict.fromkeys(more) if not seen(con, "AFM:" + i)]
    out = []
    for mid in new[:60]:
        try:
            d = http.get(f"{AFM_REG}/details?id={mid}", timeout=40)
            d.raise_for_status()
            out.append(("AFM:" + mid, parse_afm_detail(d.text, mid)))
        except Exception as ex:
            log.info("AFM %s: %s", mid, ex)
    return out


BAFIN = "https://portal.mvp.bafin.de/database/DealingsInfo/"
BAFIN_ROLES = {"vorstand": "Directivo · Vorstand", "aufsichtsrat": "Consejero · Aufsichtsrat",
               "in enger beziehung": "Persona vinculada", "geschäftsführer": "Director general",
               "verwaltungsrat": "Consejero · Verwaltungsrat"}


def parse_bafin_csv(text):
    rows = list(csv.DictReader(text.lstrip("﻿").splitlines(), delimiter=";"))
    out = []
    for r in rows:
        r = {(k or "").strip(): (v or "").strip() for k, v in r.items()}
        art = r.get("Art des Geschäfts", "").lower()
        side = "P" if art == "kauf" else "S" if art == "verkauf" else None
        if not side or "aktie" not in r.get("Art des Instruments", "").lower():
            continue
        price, value = num_eu(r.get("Durchschnittspreis")), num_eu(r.get("Aggregiertes Volumen"))
        if not price or not value:
            continue
        pos = r.get("Position / Status", "")
        uid = hashlib.sha1("|".join([r.get("Bafin-ID", ""), r.get("Meldepflichtiger", ""),
                                     r.get("Datum des Geschäfts", ""), r.get("Aggregiertes Volumen", ""),
                                     art, r.get("Datum der Aktivierung", "")]).encode()).hexdigest()[:16]
        out.append(("BAFIN:" + uid, [{
            "source": "BAFIN", "issuer": r.get("Emittent", "?"), "isin": r.get("ISIN"),
            "person": r.get("Meldepflichtiger", "?"),
            "role": BAFIN_ROLES.get(pos.lower(), pos), "related": pos.lower() == "in enger beziehung",
            "side": side, "date": parse_date(r.get("Datum des Geschäfts")),
            "price": price, "qty": value / price, "value": value,
            "currency": currency_of(r.get("Aggregiertes Volumen")),
            "link": f"{BAFIN}sucheForm.do?emittentIsin={r.get('ISIN', '')}&emittentButton=Suche+Emittent",
        }]))
    return out


def fetch_bafin(con, http):
    http.get(BAFIN, timeout=40)
    page = http.get(BAFIN + "sucheForm.do", timeout=60,
                    params={"zeitraum": "1", "emittentName": "", "emittentButton": "Suche Emittent"})
    links = [l.replace("&amp;", "&") for l in re.findall(r'href="([^"]+)"', page.text)
             if "6578706f7274" in l and re.search(r"-e=1\b", l.replace("&amp;", "&"))]
    if not links:
        log.info("BaFin: sin resultados en la última semana")
        return []
    r = http.get(ia.requests.compat.urljoin(page.url, links[0]), timeout=60)
    r.encoding = "utf-8-sig"
    return [x for x in parse_bafin_csv(r.text) if not seen(con, x[0])]


SIX_API = ("https://www.ser-ag.com/sheldon/management_transactions/v1/overview.json"
           "?pageSize=100&pageNumber=0&sortAttribute=byDate")
SIX_PAGE = "https://www.ser-ag.com/en/resources/notifications-market-participants/management-transactions.html"
SIX_FUNC = {"1": "Ejecutivo / alta dirección", "2": "Consejero no ejecutivo", "3": "Promotor (SPAC)"}


def parse_six(data, con=None):
    out = []
    for x in data.get("itemList", []):
        nid = x.get("notificationId")
        if not nid:
            continue
        if x.get("correcteeId") and con is not None and seen(con, "SIX:" + x["correcteeId"]):
            out.append(("SIX:" + nid, []))  # corrección de algo ya visto
            continue
        side = {"1": "P", "2": "S"}.get(str(x.get("buySellIndicator")))
        if not side or str(x.get("securityTypeCode")) not in ("6", "7"):
            out.append(("SIX:" + nid, []))
            continue
        rel = x.get("obligorRelatedPartyInd") or ""
        role = SIX_FUNC.get(str(x.get("obligorFunctionCode")), "Directivo")
        if rel:
            role += " · persona vinculada"
        value, price = x.get("transactionAmountCHF") or 0, x.get("transactionAmountPerSecurityCHF") or 0
        out.append(("SIX:" + nid, [{
            "source": "SIX", "issuer": x.get("notificationSubmitter", "?"), "isin": x.get("ISIN"),
            "person": "Nombre no publicado", "role": role, "related": bool(rel),
            "role_level": {"1": 15, "2": 12}.get(str(x.get("obligorFunctionCode")), 8),
            "side": side, "date": parse_date(str(x.get("transactionDate", ""))),
            "price": float(price), "qty": float(x.get("transactionSize") or 0), "value": float(value),
            "currency": "CHF", "link": SIX_PAGE,
        }]))
    return out


def fetch_six(con, http):
    r = http.get(SIX_API, timeout=40)
    r.raise_for_status()
    return [x for x in parse_six(r.json(), con) if not seen(con, x[0])]


FETCHERS = {"AFM": fetch_afm, "BAFIN": fetch_bafin, "SIX": fetch_six}


# ═══════════════════════════════════════════════════════════════════════
# Puntuación y mensaje
# ═══════════════════════════════════════════════════════════════════════
TOP_ROLE_EU = re.compile(
    r"\b(CEO|CFO|COO|Chief Executive|Chief Financial|Chief Operating|Chair|President|Président|"
    r"Directeur G[ée]n[ée]ral|Vorsitz|Consejero Delegado|Amministratore Delegato|Managing Director)", re.I)
EXEC_ROLE_EU = re.compile(r"(Officer|Executive|Management|Managing|Vorstand|Directeur|Directivo|"
                          r"Director|Bestuur|Ejecutivo)", re.I)
BOARD_ROLE_EU = re.compile(r"(Supervisory|Non-?executive|Aufsichtsrat|Verwaltungsrat|Board|"
                           r"Commissaris|Consejero|Raad)", re.I)


def role_points(t):
    if t.get("related"):
        return 6
    if "role_level" in t:
        return t["role_level"]
    r = t.get("role") or ""
    if TOP_ROLE_EU.search(r):
        return 25
    if BOARD_ROLE_EU.search(r) and not EXEC_ROLE_EU.search(r.replace("Board Member", "")):
        return 12
    if EXEC_ROLE_EU.search(r):
        return 15
    return 10


def eu_score(t, value_eur, ctx, cluster):
    pts = role_points(t)
    pts += ia._tier(value_eur, [(1_000_000, 20), (500_000, 15), (250_000, 10), (100_000, 5)])
    if t["side"] == "P":
        if cluster:
            pts += 20 if cluster["n"] >= 3 else 12
        dd = ctx.get("from_hi")
        pts += ia._tier(-dd if dd is not None else None, [(30, 10), (15, 5)])
        return min(100, round(pts / 75 * 100))  # sin datos de posición ni capitalización
    return min(100, round(pts / 60 * 100))


def eu_cluster(con, t):
    if t["side"] != "P" or t["source"] == "SIX":
        return None
    since = ((t["date"] or date.today()) - timedelta(days=EU_CONFIG["cluster_window_days"])).isoformat()
    rows = con.execute("SELECT owner, SUM(value) FROM trades WHERE region='EU' AND side='P' "
                       "AND issuer_cik=? AND trade_date>=? AND owner NOT LIKE 'Nombre no publicado%' "
                       "GROUP BY owner", (t["isin"], since)).fetchall()
    if len(rows) >= ia.CONFIG["cluster_min_insiders"]:
        return {"n": len(rows), "total": sum(v for _, v in rows)}
    return None


def eu_message(t, yahoo, labels, sector, ctx, cluster, score, value_eur):
    e = html.escape
    head = "🟢 <b>COMPRA DIRECTIVO</b>" if t["side"] == "P" else "🔴 <b>VENTA DIRECTIVO</b>"
    flame = ia.fires(score)
    lines = [f"{head} — <b>{e(yahoo or t['issuer'])}</b>" + (f"  {flame}" if flame else "")
             + ("  👥 <b>CLUSTER</b>" if cluster else "")]
    country = (t.get("isin") or "")[:2]
    lines.append(f"{FLAGS.get(country, '🇪🇺')} {e(t['issuer'])}"
                 + (f"  ·  🏷️ {e(' · '.join(labels))}" if labels else ""))
    if sector:
        lines.append(f"🏭 {e(sector)}")
    lines.append(f"👤 {e(t['person'])} ({e(t['role'] or 'Directivo')})")
    amount = f"💵 <b>{fmt_amount(t['value'], t['currency'])}</b>"
    if t["currency"] != "EUR" and value_eur:
        amount += f" (≈ {fmt_amount(value_eur, 'EUR')})"
    lines.append(f"{amount} · {ia.fmt_num(t['qty'])} acc. a {ia.fmt_num(t['price'], 2)} "
                 f"{CUR_SYMBOL.get(t['currency'], t['currency'])}")
    if cluster:
        lines.append(f"👥 {cluster['n']} directivos comprando en {EU_CONFIG['cluster_window_days']} días "
                     f"· total {fmt_amount(cluster['total'], 'EUR')}")
    if ctx.get("price"):
        cur = CUR_SYMBOL.get(ctx.get("currency"), ctx.get("currency") or "")
        px = f"📊 Cotiza a {ia.fmt_num(ctx['price'], 2)} {cur}"
        if ctx.get("vs_insider") is not None:
            px += f" ({ia.fmt_pct(ctx['vs_insider'])} vs. directivo)"
        lines.append(px)
        rng = []
        if ctx.get("from_hi") is not None:
            rng.append(f"{ia.fmt_pct(ctx['from_hi'])} desde máx. 52s")
        if ctx.get("from_lo") is not None:
            rng.append(f"{ia.fmt_pct(ctx['from_lo'])} desde mín. 52s")
        if rng:
            lines.append("📐 " + " · ".join(rng))
    lines.append(f"⭐ Puntuación: {score}/100")
    lines.append(f"📅 Operación: {t['date'] or '?'}")
    links = [f"<a href=\"{e(t['link'])}\">{SOURCE_NAME[t['source']]}</a>"]
    if t.get("isin"):
        links.append(f"<a href=\"https://es.investing.com/search/?q={e(t['isin'])}\">Investing</a>")
    lines.append("🔗 " + " · ".join(links))
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════
# Proceso
# ═══════════════════════════════════════════════════════════════════════
def eu_context(http, yahoo, t):
    q = yahoo_quote(http, yahoo) if yahoo else None
    if not q:
        return {}
    ctx = {"price": q["price"], "currency": q["currency"]}
    # El precio del directivo y el de Yahoo deben estar en la misma moneda para compararlos
    if q["currency"] == t["currency"] and t["price"]:
        ctx["vs_insider"] = (q["price"] / t["price"] - 1) * 100
    if q["hi"]:
        ctx["from_hi"] = (q["price"] / q["hi"] - 1) * 100
    if q["lo"]:
        ctx["from_lo"] = (q["price"] / q["lo"] - 1) * 100
    return ctx


def run(con, http, dry_run=False):
    eu_db(con)
    index_map = load_eu_indices(con, http)
    all_keys = set().union(*index_map.values()) if index_map else set()
    oldest = date.today() - timedelta(days=EU_CONFIG["max_age_days"])
    sent = 0

    for src in EU_CONFIG["sources"]:
        try:
            batches = FETCHERS[src](con, http)
        except Exception as ex:
            log.warning("Europa %s: %s", src, ex)
            continue
        log.info("Europa %s: %d notificaciones nuevas", src, len(batches))

        # Candidatas (antes de mirar el índice) para traducir sus ISIN de una vez
        cands = []
        for sid, trades in batches:
            for t in trades:
                if t["side"] == "S" and not EU_CONFIG["alert_sells"]:
                    continue
                if t["related"] and not EU_CONFIG["include_related"]:
                    continue
                if not t["date"] or t["date"] < oldest:
                    continue
                v = t["value"] * (fx_eur(http, t["currency"]) or 0)
                if v < (EU_CONFIG["min_buy_value_eur"] if t["side"] == "P" else EU_CONFIG["min_sell_value_eur"]):
                    continue
                cands.append((f"{sid}:{t['side']}", t, v))
        isin_info = map_isins(con, http, [t["isin"] for _, t, _ in cands if t.get("isin")], all_keys)

        for tid, t, value_eur in cands:
            yahoo, _ = isin_info.get(t.get("isin"), (None, None))
            key = ia.norm_ticker(yahoo) if yahoo else None
            labels = index_labels(key, index_map)
            if index_map and not labels:
                continue
            ctx = eu_context(http, yahoo, t)
            # Se guarda primero, para que el cluster cuente también esta compra
            con.execute("INSERT OR REPLACE INTO trades (accession, side, issuer_cik, ticker, owner, trade_date, "
                        "value, pct, score, alerted_at, region) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (tid, t["side"], t.get("isin"), yahoo or t["issuer"], f"{t['person']} ({t['role']})",
                         str(t["date"]), value_eur, None, 0, datetime.now(timezone.utc).isoformat(), "EU"))
            cluster = eu_cluster(con, t)
            score = eu_score(t, value_eur, ctx, cluster)
            if score < ia.CONFIG["min_score"]:
                con.execute("DELETE FROM trades WHERE accession=?", (tid,))
                con.commit()
                continue
            con.execute("UPDATE trades SET score=? WHERE accession=?", (score, tid))
            con.commit()
            ia.send_telegram(eu_message(t, yahoo, labels, sector_for(con, key), ctx, cluster, score, value_eur),
                             dry_run)
            sent += 1

        for sid, _ in batches:
            mark(con, sid)
    log.info("Europa: avisos enviados: %d", sent)
    return sent

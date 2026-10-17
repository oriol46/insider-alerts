#!/usr/bin/env python3
"""
Script de prueba (se ejecuta UNA vez): descarga muestras de las fuentes europeas
y las guarda en la carpeta probe/ para revisar su formato.
"""
import os
import requests

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
SEC_UA = os.environ.get("SEC_USER_AGENT", "insider-alerts probe")

TARGETS = {
    # Holanda (AFM)
    "afm_export.csv": "https://www.afm.nl/export.aspx?type=0ee836dc-5520-459d-bcf4-a4a689de6614&format=csv",
    "afm_export.xml": "https://www.afm.nl/export.aspx?type=0ee836dc-5520-459d-bcf4-a4a689de6614&format=xml",
    "afm_register.html": "https://www.afm.nl/en/sector/registers/meldingenregisters/transacties-leidinggevenden-mar19-",
    # Alemania (BaFin)
    "bafin_db.html": "https://portal.mvp.bafin.de/database/DealingsInfo/",
    "bafin_info.html": "https://www.bafin.de/ref/19590874",
    # STOXX 600 (iShares STOXX Europe 600 UCITS ETF DE)
    "ishares_exsa_page.html": "https://www.ishares.com/ch/individual/en/products/251931/ishares-stoxx-europe-600-ucits-etf-de",
    "ishares_exsa_1.csv": "https://www.ishares.com/ch/individual/en/products/251931/ishares-stoxx-europe-600-ucits-etf-de/1495092304805.ajax?fileType=csv&fileName=EXSA_holdings&dataType=fund",
    "ishares_exsa_2.csv": "https://www.ishares.com/ch/individual/en/products/251931/x/latest-holdings.csv",
    "ishares_exsa_3.csv": "https://www.ishares.com/de/privatanleger/de/produkte/251931/ishares-stoxx-europe-600-ucits-etf-de-fund/1478358465952.ajax?fileType=csv&fileName=EXSA_holdings&dataType=fund",
    # Fase 2 (para ir adelantando): Suiza y Francia
    "six_list.html": "https://disclosure.six-exchange-regulation.com/obligations/management_transaction/ddeal/disclosure_list_en",
    "amf_rss_page.html": "https://bdif.amf-france.org/fr/navigation-flux-rss",
    # Comprobación de precios europeos (Yahoo)
    "yahoo_SAP.DE.json": "https://query1.finance.yahoo.com/v8/finance/chart/SAP.DE?range=1y&interval=1d",
}

os.makedirs("probe", exist_ok=True)
s = requests.Session()
s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
summary = []
for name, url in TARGETS.items():
    try:
        r = s.get(url, timeout=40, allow_redirects=True)
        data = r.content[:3_000_000]
        with open(os.path.join("probe", name), "wb") as fh:
            fh.write(data)
        line = f"{name}: HTTP {r.status_code} · {len(r.content):,} bytes · {r.headers.get('Content-Type', '?')} · final URL {r.url}"
    except Exception as ex:
        line = f"{name}: ERROR {ex}"
    print(line)
    summary.append(line)

# OpenFIGI: ISIN → ticker (prueba con SAP y ASML)
try:
    r = s.post("https://api.openfigi.com/v3/mapping",
               json=[{"idType": "ID_ISIN", "idValue": "DE0007164600"},
                     {"idType": "ID_ISIN", "idValue": "NL0010273215"}], timeout=30)
    with open("probe/openfigi.json", "wb") as fh:
        fh.write(r.content[:200_000])
    line = f"openfigi.json: HTTP {r.status_code} · {len(r.content):,} bytes"
except Exception as ex:
    line = f"openfigi.json: ERROR {ex}"
print(line)
summary.append(line)

with open("probe/_summary.txt", "w", encoding="utf-8") as fh:
    fh.write("\n".join(summary))

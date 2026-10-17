#!/usr/bin/env python3
"""
Script de prueba (2ª ronda): descarga muestras de detalle de las fuentes europeas
y las guarda en la carpeta probe/ para revisar su formato.
"""
import os
import re
import requests

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
os.makedirs("probe", exist_ok=True)
summary = []


def save(name, r, limit=3_000_000):
    with open(os.path.join("probe", name), "wb") as fh:
        fh.write(r.content[:limit])
    line = (f"{name}: HTTP {r.status_code} · {len(r.content):,} bytes · "
            f"{r.headers.get('Content-Type', '?')} · {r.url}")
    print(line)
    summary.append(line)


def attempt(name, fn):
    try:
        fn()
    except Exception as ex:
        line = f"{name}: ERROR {ex}"
        print(line)
        summary.append(line)


def new_session():
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    return s


# ── Holanda (AFM): páginas de detalle de las últimas notificaciones ──
def afm():
    s = new_session()
    r = s.get("https://www.afm.nl/export.aspx?type=0ee836dc-5520-459d-bcf4-a4a689de6614&format=xml", timeout=60)
    ids = re.findall(r"<meldingid>([^<]+)</meldingid>", r.text)[:8]
    for i, mid in enumerate(ids, 1):
        d = s.get("https://www.afm.nl/en/sector/registers/meldingenregisters/"
                  f"transacties-leidinggevenden-mar19-/details?id={mid}", timeout=40)
        save(f"afm_detail_{i}.html", d)


# ── Alemania (BaFin): resultados de la búsqueda de la última semana ──
def bafin():
    base = "https://portal.mvp.bafin.de/database/DealingsInfo/"
    s = new_session()
    s.get(base, timeout=40)
    variants = {
        "bafin_post_week.html": dict(method="post", data={
            "emittentIsin": "", "emittentName": "", "meldepflichtigerName": "",
            "zeitraum": "1", "zeitraumVon": "", "zeitraumBis": "", "emittentButton": "Suche Emittent"}),
        "bafin_get_week.html": dict(method="get", params={
            "zeitraum": "1", "emittentName": "", "emittentButton": "Suche Emittent"}),
        "bafin_get_letterA.html": dict(method="get", params={"emittentName": "A", "zeitraum": "1"}),
    }
    for name, v in variants.items():
        if v["method"] == "post":
            r = s.post(base + "sucheForm.do", data=v["data"], timeout=60)
        else:
            r = s.get(base + "sucheForm.do", params=v["params"], timeout=60)
        save(name, r)
        # Si hay resultados, seguir el primer enlace de exportación y el primer detalle
        links = re.findall(r'href="([^"]+)"', r.text)
        exp = [l for l in links if "6578706f7274" in l]  # parámetro de exportación de displaytag
        det = [l for l in links if "detail" in l.lower() or "anzeige" in l.lower()]
        for j, l in enumerate(exp[:3], 1):
            url = requests.compat.urljoin(r.url, l.replace("&amp;", "&"))
            save(f"{name}_export{j}.txt", s.get(url, timeout=60))
        for j, l in enumerate(det[:2], 1):
            url = requests.compat.urljoin(r.url, l.replace("&amp;", "&"))
            save(f"{name}_detail{j}.html", s.get(url, timeout=60))


# ── Suiza (SIX Exchange Regulation) ──
def six():
    s = new_session()
    page = s.get("https://www.ser-ag.com/en/resources/notifications-market-participants/"
                 "management-transactions.html", timeout=40)
    save("six_page.html", page)
    for i, u in enumerate([
        "https://www.ser-ag.com/sheldon/management_transactions/v1/overview.json?pageSize=20&pageNumber=0&sortAttribute=byDate",
        "https://www.ser-ag.com/sheldon/management_transactions/v1/overview.json",
    ], 1):
        attempt(f"six_api_{i}", lambda u=u, i=i: save(f"six_api_{i}.json", s.get(u, timeout=40)))
    # Scripts de la página, para localizar la API real
    for j, src in enumerate(re.findall(r'<script[^>]+src="([^"]+)"', page.text)[:15], 1):
        url = requests.compat.urljoin(page.url, src)
        if "ser-ag" in url or url.startswith("/"):
            attempt(f"six_js_{j}", lambda url=url, j=j: save(f"six_js_{j}.js", s.get(url, timeout=40)))


# ── Francia (AMF BDIF) ──
def amf():
    s = new_session()
    page = s.get("https://bdif.amf-france.org/fr", timeout=40)
    save("amf_home.html", page)
    for src in re.findall(r'<script[^>]+src="([^"]+)"', page.text):
        if "main" in src:
            save("amf_main.js", s.get(requests.compat.urljoin("https://bdif.amf-france.org/", src), timeout=60),
                 limit=8_000_000)
    for i, u in enumerate([
        "https://bdif.amf-france.org/back/api/v1/informations?typesInformation=DD&from=0&size=20",
        "https://bdif.amf-france.org/back/api/v1/rss?lang=fr&types=DD",
        "https://bdif.amf-france.org/fr/rss?typesInformation=DD",
    ], 1):
        attempt(f"amf_try_{i}", lambda u=u, i=i: save(f"amf_try_{i}.txt", s.get(u, timeout=40)))


for name, fn in [("afm", afm), ("bafin", bafin), ("six", six), ("amf", amf)]:
    attempt(name, fn)

with open("probe/_summary.txt", "w", encoding="utf-8") as fh:
    fh.write("\n".join(summary))

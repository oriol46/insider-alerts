# Insider Alerts — compras de insiders (SEC Form 4) por Telegram

Revisa cada pocos minutos los Form 4 nuevos de EDGAR y te avisa por Telegram de las compras de insiders que pasan tus filtros, marcando el *cluster buying*.

## 1. Crear el bot de Telegram (5 min)

1. En Telegram, abre **@BotFather** → `/newbot` → elige nombre y usuario. Te dará un **token** (`123456:ABC...`).
2. Abre el chat con tu bot nuevo y envíale cualquier mensaje (por ejemplo, "hola").
3. En el navegador, abre `https://api.telegram.org/bot<TOKEN>/getUpdates` y busca `"chat":{"id":123456789` → ese número es tu **chat_id**.
   - Si quieres los avisos en un grupo o canal: añade el bot y usa el id del grupo (empieza por `-100...`).

## 2. Instalar y probar en local

```bash
pip install -r requirements.txt

export TELEGRAM_BOT_TOKEN="123456:ABC..."
export TELEGRAM_CHAT_ID="123456789"
export SEC_USER_AGENT="Tu Nombre tu@email.com"   # obligatorio para la SEC

python insider_alerts.py --test      # mensaje de prueba a Telegram
python insider_alerts.py --dry-run   # una pasada real, imprimiendo en pantalla
python insider_alerts.py             # una pasada enviando avisos
```

En Windows (PowerShell) usa `$env:TELEGRAM_BOT_TOKEN="..."` en lugar de `export`.

## 3. Ajustar filtros

Todo está en el bloque `CONFIG` al principio de `insider_alerts.py`:

| Parámetro | Por defecto | Qué hace |
|---|---|---|
| `min_buy_value` | 100.000 $ | Importe mínimo de la compra |
| `min_pct_increase` | 0 (off) | % mínimo de aumento sobre la posición previa |
| `exclude_10b5_1` | `True` | Descarta operaciones bajo plan 10b5-1 |
| `roles` | todos | `officer`, `director`, `ten_percent` |
| `watchlist` | vacía | Si tiene tickers, solo avisa de esos |
| `cluster_window_days` / `cluster_min_insiders` | 30 / 2 | Definición de cluster |
| `alert_sells` / `min_sell_value` | `False` / 5M $ | Avisos de ventas grandes |

Solo se tienen en cuenta compras en mercado abierto (código **P**). Las ventas usan el código **S**.

## 4. Ponerlo en marcha de forma continua

**Opción A — GitHub Actions (gratis, sin servidor)**
1. Crea un repositorio con `insider_alerts.py`, `requirements.txt` y el workflow en `.github/workflows/insider-alerts.yml`.
2. En *Settings → Secrets and variables → Actions*, añade `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` y `SEC_USER_AGENT`.
3. Lánzalo una vez a mano desde la pestaña *Actions* para comprobar que funciona.

Ten en cuenta:
- En repos **privados** el plan gratuito da unos 2.000 min/mes, que no llegan para cada 15 min (son unas 2.900 ejecuciones al mes). Usa un repo **público** (los secretos no se exponen) o baja la frecuencia a `*/30`.
- GitHub desactiva los cron de repos sin actividad durante 60 días. Basta con hacer un commit de vez en cuando.

**Opción B — VPS u ordenador siempre encendido**
```bash
python insider_alerts.py --loop 600     # cada 10 minutos
```
O con cron (`crontab -e`):
```
*/10 * * * * cd /ruta/insider && TELEGRAM_BOT_TOKEN=... TELEGRAM_CHAT_ID=... SEC_USER_AGENT="..." /usr/bin/python3 insider_alerts.py >> log.txt 2>&1
```

## Notas

- `insider.db` (SQLite) guarda los filings ya procesados y el histórico de compras, para no repetir avisos y detectar clusters. Si lo borras, el bot vuelve a empezar desde el feed actual.
- La primera ejecución procesa los últimos ~500 Form 4 del feed, así que puede mandar algunos avisos de golpe.
- El % de aumento de posición es aproximado: se calcula sobre la naturaleza (directa o indirecta) de la última operación del filing.
- Respeta las normas de la SEC: User-Agent con tu email y menos de 10 peticiones por segundo (ya configurado).

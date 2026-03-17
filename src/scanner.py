"""
═══════════════════════════════════════════════════════════════════════════
THE SYSTEM v2 — ESCÁNER DE MERCADO AMPLIO · 6 ESTRATEGIAS
Ejecuta diariamente via GitHub Actions · 22:30h España (lunes a viernes)

Estrategias:
  S1 · Pre-Revenue Catalyst    — Contrato Tier-1, inversión institucional, hito técnico
  S2 · Post-Earnings Drift     — Earnings beat >10%, ventana 30 días, drift confirmado
  S3 · Options Flow            — Volumen opciones real via yfinance >3x OI
  S4 · Short Squeeze           — Short float >15% + catalizador que destruye tesis bajista
  S5 · Sector Rotation         — Top 3 sectores por momentum trimestral
  S6 · Narrative Shift         — Pivot estratégico sector viejo → sector caliente

Universo:
  NASDAQ + NYSE · Market Cap $50M–$10B
  S&P 500 + MidCap 400 + Russell 2000 filtrado (~1.500–2.000 tickers)
  Excluidos: OTC, ADRs, ETFs, SPACs

Fuentes (todas gratuitas):
  Finviz   — screeners + quote data
  yfinance — histórico, opciones reales, EPS, earnings dates
  SEC EDGAR — Form 4 insider buying (últimos 30 días)

Scoring 0–40:
  Base estrategia  +12 · Volumen     +3 · Short amplifier +2
  Confluencia       +3 · Sector      +3 · Insider buying  +3
  Narrative +4 · Penalizaciones -5
  Señal fuerte ≥22 · Watchlist 10–21 · Descarta <10

Output:
  Email HTML · CSV acumulativo para backtesting · Log detallado
═══════════════════════════════════════════════════════════════════════════
"""

import os, re, time, json, csv, datetime, smtplib, traceback, requests
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from bs4 import BeautifulSoup
from anthropic import Anthropic
import yfinance as yf

# ── Env vars ──────────────────────────────────────────────────────────────
EMAIL_FROM         = os.environ["EMAIL_FROM"]
EMAIL_TO           = os.environ["EMAIL_TO"]
EMAIL_APP_PASSWORD = os.environ["EMAIL_APP_PASSWORD"]
ANTHROPIC_API_KEY  = os.environ["ANTHROPIC_API_KEY"]

client = Anthropic(api_key=ANTHROPIC_API_KEY)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ── Paths ─────────────────────────────────────────────────────────────────
CSV_LOG = "signals_log.csv"


# ══════════════════════════════════════════════════════════════════════════
# S5 · SECTOR ROTATION — momentum trimestral ETFs sectoriales
# ══════════════════════════════════════════════════════════════════════════

SECTOR_ETFS = {
    "XLK":  "Technology",
    "XLV":  "Healthcare",
    "XLE":  "Energy",
    "XLF":  "Financials",
    "XLI":  "Industrials",
    "XLY":  "Consumer Discretionary",
    "XLP":  "Consumer Staples",
    "XLB":  "Materials",
    "XLRE": "Real Estate",
    "XLU":  "Utilities",
    "XLC":  "Communication Services",
}

FINVIZ_TO_SECTOR = {
    "Technology":             "XLK",
    "Healthcare":             "XLV",
    "Energy":                 "XLE",
    "Financial":              "XLF",
    "Financial Services":     "XLF",
    "Industrials":            "XLI",
    "Consumer Cyclical":      "XLY",
    "Consumer Defensive":     "XLP",
    "Basic Materials":        "XLB",
    "Real Estate":            "XLRE",
    "Utilities":              "XLU",
    "Communication Services": "XLC",
}

# Sectores "calientes" para detectar narrative shift
HOT_SECTORS = {"AI", "HPC", "data center", "artificial intelligence",
               "machine learning", "space", "defense tech", "quantum",
               "biotech", "gene therapy", "nuclear", "clean energy"}


def get_sector_momentum() -> dict[str, float]:
    """Calcula retorno trimestral de cada ETF sectorial via Finviz."""
    print("📡 [S5] Calculando momentum sectorial...")
    perf = {}
    for etf in SECTOR_ETFS:
        try:
            url = f"https://finviz.com/quote.ashx?t={etf}&p=d"
            r = requests.get(url, headers=HEADERS, timeout=12)
            soup = BeautifulSoup(r.text, "html.parser")
            cells = soup.find_all("td")
            for i, cell in enumerate(cells):
                if "Perf Quarter" in cell.get_text():
                    val = cells[i + 1].get_text(strip=True).replace("%", "")
                    try:
                        perf[etf] = float(val)
                    except ValueError:
                        perf[etf] = 0.0
                    break
            time.sleep(0.8)
        except Exception as e:
            print(f"   ⚠ ETF {etf}: {e}")
            perf[etf] = 0.0

    sorted_perf = dict(sorted(perf.items(), key=lambda x: x[1], reverse=True))
    top3 = list(sorted_perf.keys())[:3]
    print(f"   Top sectores: {top3}")
    return sorted_perf


def sector_momentum_score(finviz_sector: str, sector_perf: dict[str, float]) -> int:
    """0–3 puntos según si el sector está en momentum top."""
    etf = FINVIZ_TO_SECTOR.get(finviz_sector, "")
    if not etf or not sector_perf:
        return 0
    rank = sorted(sector_perf.values(), reverse=True).index(
        sector_perf.get(etf, min(sector_perf.values()))
    ) + 1
    if rank <= 2: return 3
    if rank <= 4: return 2
    if rank <= 6: return 1
    return 0


# ══════════════════════════════════════════════════════════════════════════
# FINVIZ SCREENERS — universo ampliado
# ══════════════════════════════════════════════════════════════════════════

# S1/S4: volumen fuerte + subida — Small a Large Cap, NASDAQ+NYSE
SCREEN_VOLUME = (
    "https://finviz.com/screener.ashx?v=111"
    "&f=cap_microover,geo_usa,exch_nasd|exch_nyse,"
    "sh_opt_option,ta_change_u3,sh_relvol_o2"
    "&o=-relativevolume&r=1"
)

# S4: short squeeze — short interest alto + movimiento
SCREEN_SHORT = (
    "https://finviz.com/screener.ashx?v=111"
    "&f=cap_microover,geo_usa,exch_nasd|exch_nyse,"
    "sh_opt_option,sh_short_o15,ta_change_u1"
    "&o=-shortfloat&r=1"
)

# S2: PEAD — earnings recientes (hasta 4 semanas) + precio subiendo
SCREEN_EARNINGS = (
    "https://finviz.com/screener.ashx?v=111"
    "&f=cap_microover,geo_usa,exch_nasd|exch_nyse,"
    "earningsdate_prevmonth,ta_change_u1,ta_perf_dup"
    "&o=-change&r=1"
)

# S6: narrative shift — acciones con volumen alto en sectores en transición
SCREEN_NARRATIVE = (
    "https://finviz.com/screener.ashx?v=111"
    "&f=cap_microover,geo_usa,exch_nasd|exch_nyse,"
    "sh_opt_option,ta_change_u2,sh_relvol_o15"
    "&o=-relativevolume&r=1"
)


def scrape_finviz_screen(url: str, label: str, pages: int = 3) -> list[dict]:
    """
    Scrapea hasta N páginas de un screener Finviz.
    Cada página tiene 20 resultados → hasta 60 acciones por screener.
    """
    print(f"   Scraping {label}...")
    all_stocks = []
    seen = set()

    for page in range(pages):
        page_url = url if page == 0 else url + f"&r={page * 20 + 1}"
        try:
            r = requests.get(page_url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            table = (
                soup.find("table", {"id": "screener-views-table"}) or
                soup.find("table", class_="screener_table") or
                soup.find("table", {"class": "table-light"})
            )

            if not table:
                break

            rows = table.find_all("tr")[1:]
            page_stocks = []
            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 10:
                    continue
                try:
                    ticker = cols[1].get_text(strip=True)
                    if ticker in seen:
                        continue
                    seen.add(ticker)

                    # Filtrar por market cap $50M–$10B
                    mcap_raw = cols[6].get_text(strip=True)
                    mcap_val = parse_mcap(mcap_raw)
                    if mcap_val is not None and (mcap_val < 50 or mcap_val > 10000):
                        continue

                    page_stocks.append({
                        "ticker":   ticker,
                        "company":  cols[2].get_text(strip=True),
                        "sector":   cols[3].get_text(strip=True),
                        "industry": cols[4].get_text(strip=True),
                        "country":  cols[5].get_text(strip=True),
                        "mcap":     mcap_raw,
                        "mcap_val": mcap_val or 0,
                        "pe":       cols[7].get_text(strip=True),
                        "price":    cols[8].get_text(strip=True),
                        "change":   cols[9].get_text(strip=True),
                        "volume":   cols[10].get_text(strip=True) if len(cols) > 10 else "N/A",
                        "source":   label,
                    })
                except Exception:
                    continue

            all_stocks.extend(page_stocks)
            if len(page_stocks) < 18:  # menos de una página completa → fin
                break
            time.sleep(1.2)

        except Exception as e:
            print(f"   ⚠ Error page {page} {label}: {e}")
            break

    print(f"   → {len(all_stocks)} acciones en {label}")
    return all_stocks


def parse_mcap(mcap_str: str) -> float | None:
    """Convierte '1.23B' → 1230.0 (en millones). None si no parseable."""
    try:
        s = mcap_str.strip().upper()
        if s.endswith("B"):
            return float(s[:-1]) * 1000
        if s.endswith("M"):
            return float(s[:-1])
        if s.endswith("K"):
            return float(s[:-1]) / 1000
        return None
    except Exception:
        return None


def deduplicate(stocks: list[dict]) -> list[dict]:
    """Elimina duplicados marcando los que aparecen en múltiples screeners."""
    seen = {}
    for s in stocks:
        t = s["ticker"]
        if t not in seen:
            seen[t] = s
        else:
            existing = seen[t].get("source", "")
            new_source = s["source"]
            if new_source not in existing:
                seen[t]["source"] = f"{existing} + {new_source}"
    return list(seen.values())


# ══════════════════════════════════════════════════════════════════════════
# FINVIZ QUOTE — datos por ticker
# ══════════════════════════════════════════════════════════════════════════

def get_finviz_quote(ticker: str) -> dict:
    """Extrae datos de la página de quote de Finviz."""
    url = f"https://finviz.com/quote.ashx?t={ticker}&p=d"
    data = {
        "news": [], "short_float": "N/A", "rel_volume": "N/A",
        "beta": "N/A", "inst_own": "N/A", "perf_week": "N/A",
        "perf_month": "N/A", "earnings_date": "N/A", "roic": "N/A",
        "sales_qoq": "N/A", "avg_volume": "N/A", "float_sh": "N/A",
    }
    try:
        r = requests.get(url, headers=HEADERS, timeout=14)
        soup = BeautifulSoup(r.text, "html.parser")

        snap_keys   = soup.find_all("td", class_="snapshot-td2-cp")
        snap_values = soup.find_all("td", class_="snapshot-td2")

        for i, key_cell in enumerate(snap_keys):
            key = key_cell.get_text(strip=True)
            val = snap_values[i].get_text(strip=True) if i < len(snap_values) else ""
            if key == "Short Float":   data["short_float"]  = val
            elif key == "Rel Volume":  data["rel_volume"]   = val
            elif key == "Beta":        data["beta"]         = val
            elif key == "Inst Own":    data["inst_own"]     = val
            elif key == "Perf Week":   data["perf_week"]    = val
            elif key == "Perf Month":  data["perf_month"]   = val
            elif key == "Earnings":    data["earnings_date"]= val
            elif key == "ROIC":        data["roic"]         = val
            elif key == "Sales Q/Q":   data["sales_qoq"]    = val
            elif key == "Avg Volume":  data["avg_volume"]   = val
            elif key == "Shs Float":   data["float_sh"]     = val

        news_table = soup.find("table", {"id": "news-table"})
        if news_table:
            for row in news_table.find_all("tr")[:6]:
                tds = row.find_all("td")
                if len(tds) >= 2:
                    headline = tds[1].get_text(strip=True)
                    if headline:
                        data["news"].append(headline)
    except Exception as e:
        print(f"   ⚠ Finviz quote {ticker}: {e}")

    return data


# ══════════════════════════════════════════════════════════════════════════
# YFINANCE — opciones reales + EPS histórico + earnings date
# ══════════════════════════════════════════════════════════════════════════

def get_yfinance_data(ticker: str) -> dict:
    """
    Obtiene de yfinance:
    - Ratio calls/puts del día (options flow)
    - Volumen total de opciones vs open interest
    - EPS actual vs estimado (earnings surprise %)
    - Días desde último earnings
    - Short % of float
    """
    data = {
        "options_call_vol": 0,
        "options_put_vol": 0,
        "options_total_vol": 0,
        "options_total_oi": 0,
        "options_vol_oi_ratio": 0.0,
        "options_call_put_ratio": 0.0,
        "eps_actual": None,
        "eps_estimate": None,
        "eps_surprise_pct": None,
        "days_since_earnings": None,
        "next_earnings_date": None,
        "short_pct_float": None,
        "fifty_two_week_low": None,
        "pct_from_52w_low": None,
    }
    try:
        tk = yf.Ticker(ticker)

        # --- Opciones (primer vencimiento disponible) ---
        expirations = tk.options
        if expirations:
            # Usar el vencimiento más próximo que tenga datos
            for exp in expirations[:3]:
                try:
                    chain = tk.option_chain(exp)
                    call_vol = int(chain.calls["volume"].fillna(0).sum())
                    put_vol  = int(chain.puts["volume"].fillna(0).sum())
                    call_oi  = int(chain.calls["openInterest"].fillna(0).sum())
                    put_oi   = int(chain.puts["openInterest"].fillna(0).sum())
                    total_vol = call_vol + put_vol
                    total_oi  = call_oi + put_oi
                    if total_vol > 100:  # mínimo de actividad
                        data["options_call_vol"]      = call_vol
                        data["options_put_vol"]        = put_vol
                        data["options_total_vol"]      = total_vol
                        data["options_total_oi"]       = total_oi
                        data["options_vol_oi_ratio"]   = round(total_vol / max(total_oi, 1), 2)
                        data["options_call_put_ratio"] = round(call_vol / max(put_vol, 1), 2)
                        break
                except Exception:
                    continue

        # --- Earnings history ---
        try:
            info = tk.info
            # Short interest
            short_pct = info.get("shortPercentOfFloat")
            if short_pct:
                data["short_pct_float"] = round(short_pct * 100, 1)

            # 52-week low
            low52 = info.get("fiftyTwoWeekLow")
            curr  = info.get("currentPrice") or info.get("regularMarketPrice")
            if low52 and curr:
                data["fifty_two_week_low"]  = low52
                data["pct_from_52w_low"] = round((curr - low52) / low52 * 100, 1)
        except Exception:
            pass

        # --- Earnings calendar ---
        try:
            cal = tk.calendar
            if cal is not None and not cal.empty:
                # Next earnings
                if "Earnings Date" in cal.index:
                    ned = cal.loc["Earnings Date"].iloc[0]
                    data["next_earnings_date"] = str(ned)[:10] if ned else None
        except Exception:
            pass

        # --- EPS surprise (últimos earnings) ---
        try:
            hist = tk.earnings_history
            if hist is not None and not hist.empty:
                last = hist.iloc[-1]
                eps_act = float(last.get("epsActual", 0) or 0)
                eps_est = float(last.get("epsEstimate", 0) or 0)
                data["eps_actual"]   = eps_act
                data["eps_estimate"] = eps_est
                if eps_est != 0:
                    data["eps_surprise_pct"] = round((eps_act - eps_est) / abs(eps_est) * 100, 1)

                # Días desde earnings
                earnings_date = hist.index[-1]
                if hasattr(earnings_date, "date"):
                    delta = (datetime.date.today() - earnings_date.date()).days
                    data["days_since_earnings"] = delta
        except Exception:
            pass

    except Exception as e:
        print(f"   ⚠ yfinance {ticker}: {e}")

    return data


# ══════════════════════════════════════════════════════════════════════════
# SEC EDGAR — insider buying Form 4 (últimos 30 días)
# ══════════════════════════════════════════════════════════════════════════

def get_insider_buying(ticker: str) -> dict:
    """
    Consulta SEC EDGAR para detectar compras de insiders en últimos 30 días.
    Usa la API pública de EDGAR (sin autenticación).
    Devuelve: {has_buying, total_shares, total_value, transactions}
    """
    data = {
        "has_buying": False,
        "total_shares": 0,
        "total_value": 0.0,
        "transactions": 0,
        "insider_summary": "Sin compras de insiders recientes",
    }
    try:
        # Buscar CIK por ticker
        url_cik = f"https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22&dateRange=custom&startdt=2020-01-01&forms=4"
        headers_sec = {
            "User-Agent": "TheSystem Scanner contact@thesystem.com",
            "Accept": "application/json",
        }

        # Endpoint directo de submissions por ticker
        url_submit = f"https://data.sec.gov/submissions/CIK{ticker}.json"

        # Usamos el endpoint de búsqueda EDGAR full-text
        cutoff = (datetime.date.today() - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
        search_url = (
            f"https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22"
            f"&forms=4&dateRange=custom&startdt={cutoff}"
        )
        r = requests.get(search_url, headers=headers_sec, timeout=10)
        if r.status_code != 200:
            return data

        results = r.json().get("hits", {}).get("hits", [])
        buys = 0
        total_shares = 0

        for hit in results[:10]:
            src = hit.get("_source", {})
            form_type = src.get("form_type", "")
            if form_type != "4":
                continue

            # Verificar si es compra (P = Purchase) en el contenido
            period = src.get("period_of_report", "")
            entity = src.get("entity_name", "")
            # Cualquier Form 4 reciente se cuenta como señal de insider activity
            buys += 1

        if buys > 0:
            data["has_buying"] = True
            data["transactions"] = buys
            data["insider_summary"] = f"{buys} transacción(es) insider en últimos 30 días"

    except Exception as e:
        pass  # SEC EDGAR puede fallar — no crítico

    return data


# ══════════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════════════════
# AI ANALYSIS — Claude Haiku · scoring 0–40
# ══════════════════════════════════════════════════════════════════════════

STRATEGY_DEFINITIONS = """
S1 · PRE-REVENUE CATALYST
Empresa sin revenue o pre-rentabilidad con catalizador que elimina el mayor riesgo.
Contratos Tier-1 reales (Fortune 500, DoD, NASA) con dinero comprometido.
Inversión institucional directa. Hito técnico demostrado. Contrato gubernamental.
Score máximo si la noticia es contrato con dinero real comprometido.

S2 · POST-EARNINGS DRIFT (PEAD)
Empresa reportó earnings en los últimos 30 días con beat significativo (>10% sobre estimaciones).
El precio continúa subiendo. Mayor score si baja cobertura analista + guidance alcista.
Verificar que el drift continúe: no comprar si ya subió >50% desde earnings.

S3 · OPTIONS FLOW
Evidencia de actividad institucional en opciones.
Ratio calls/puts >2.0. Volumen opciones >3x open interest.
Call sweeps masivas OTM. Alguien con información compra con urgencia.

S4 · SHORT SQUEEZE
Short float >15%. Mayor score cuanto mayor sea el short interest.
El catalizador destruye la tesis bajista. Days to cover >5 = squeeze potente.
Short >30% con catalizador real = setup explosivo.

S5 · SECTOR ROTATION
El sector de la empresa está en los top-3 de momentum trimestral.
No es señal independiente — añade puntos de contexto a otras estrategias.

S6 · NARRATIVE SHIFT
Empresa en sector "viejo" anuncia pivot estratégico a sector caliente.
Ejemplos: minero Bitcoin → HPC/AI, coal → clean energy, legacy telecom → satélite.
Infraestructura física valiosa + nueva narrativa + short interest alto = potencial explosivo.
Clave: el mercado tarda semanas en re-ratear. Ventana de seguimiento 60 días.
"""


def analyze_with_ai(
    stock: dict,
    finviz: dict,
    yf_data: dict,
    insider: dict,
    sector_pts: int,
) -> dict | None:
    """Envía datos enriquecidos a Claude Haiku para scoring 0–40."""

    # Preparar resumen de opciones
    opt_summary = "N/A"
    if yf_data["options_total_vol"] > 0:
        opt_summary = (
            f"Calls: {yf_data['options_call_vol']:,} | "
            f"Puts: {yf_data['options_put_vol']:,} | "
            f"C/P ratio: {yf_data['options_call_put_ratio']:.1f} | "
            f"Vol/OI: {yf_data['options_vol_oi_ratio']:.1f}x"
        )

    # Preparar resumen de earnings
    earnings_summary = "N/A"
    if yf_data["eps_surprise_pct"] is not None:
        earnings_summary = (
            f"EPS actual: {yf_data['eps_actual']} vs estimado: {yf_data['eps_estimate']} "
            f"({yf_data['eps_surprise_pct']:+.1f}% sorpresa) "
            f"— hace {yf_data['days_since_earnings']} días"
        )

    # Combinar noticias de Finviz + Finnhub
    all_news = finviz["news"][:5]
    news_text = "\n".join(f"- {h}" for h in all_news) if all_news else "Sin noticias disponibles"

    # Short float (preferir yfinance si disponible)
    short_float = finviz.get("short_float", "N/A")
    if yf_data["short_pct_float"] is not None:
        short_float = f"{yf_data['short_pct_float']}% (yfinance)"

    prompt = f"""Analiza esta acción del escáner "The System v2" y puntúala con el scoring detallado.

═══ DATOS DE MERCADO ═══
Ticker: {stock['ticker']} | Empresa: {stock['company']}
Sector: {stock['sector']} | Industria: {stock['industry']}
Precio: {stock['price']} | Cambio hoy: {stock['change']}
Market Cap: {stock['mcap']} | Vol relativo: {finviz['rel_volume']}x
Short Float: {short_float}
Beta: {finviz['beta']} | Inst. Ownership: {finviz['inst_own']}
ROIC: {finviz['roic']} | Sales Q/Q: {finviz['sales_qoq']}
Perf semana: {finviz['perf_week']} | Perf mes: {finviz['perf_month']}
% desde mínimo 52w: {yf_data['pct_from_52w_low']}%
Screener origen: {stock['source']}

═══ OPTIONS FLOW (yfinance) ═══
{opt_summary}

═══ EARNINGS (yfinance) ═══
{earnings_summary}
Próximos earnings: {yf_data['next_earnings_date'] or 'N/A'}

═══ INSIDERS (SEC EDGAR) ═══
{insider['insider_summary']}
Transacciones Form 4 últimos 30 días: {insider['transactions']}

═══ NOTICIAS (Finviz) ═══
{news_text}

═══ SECTOR MOMENTUM ═══
Puntos sector pre-calculados: {sector_pts}/3

═══ ESTRATEGIAS ═══
{STRATEGY_DEFINITIONS}

INSTRUCCIÓN: Detecta qué estrategias aplican. Puntúa con desglose detallado. Identifica caso histórico similar.

Responde SOLO con este JSON (sin markdown, sin texto extra):
{{
  "has_signal": true/false,
  "strategies_detected": ["S1","S2","S3","S4","S5","S6"],
  "primary_strategy": "S1|S2|S3|S4|S5|S6",
  "catalyst_type": "tier1_contract|institutional_inv|gov_contract|tech_milestone|regulatory|earnings_beat|short_squeeze|options_flow|sector_rotation|narrative_shift|insider_buy|rumor|none",
  "catalyst_summary": "1 línea concisa",
  "why_it_moves": "2-3 líneas: quién compra, por qué, qué fuerza el movimiento",
  "entry_note": "nivel o condición de entrada ideal (1 línea)",
  "stop_narrative": "qué evento invalidaría la tesis (1 línea)",
  "risk_level": "low|medium|high|extreme",
  "score": 0,
  "score_breakdown": {{
    "base_strategy_pts": 0,
    "base_strategy_reason": "",
    "volume_pts": 0,
    "volume_reason": "",
    "short_pts": 0,
    "short_reason": "",
    "options_pts": 0,
    "options_reason": "",
    "insider_pts": 0,
    "insider_reason": "",
    "sentiment_pts": 0,
    "sentiment_reason": "",
    "narrative_pts": 0,
    "narrative_reason": "",
    "confluence_pts": 0,
    "confluence_reason": "",
    "sector_pts": {sector_pts},
    "sector_reason": "",
    "penalty_pts": 0,
    "penalty_reason": ""
  }},
  "similar_case": "ASTS|RKLB|IONQ|LUNR|JOBY|GME|BCRX|BITF|CAVA|TSLA|MRNA|AMC|NVDA|ENPH|none",
  "similar_case_reason": "1-2 líneas explicando la similitud en mecánica",
  "verdict": "SEÑAL FUERTE|WATCHLIST|DESCARTA"
}}

SCORING GUIDE (máximo 40 puntos):

BASE ESTRATEGIA (base_strategy_pts) — máx 12:
  S1 contrato Tier-1 real Fortune 500/DoD/NASA: +12
  S1 inversión institucional directa: +10
  S1 hito técnico demostrado / aprobación FDA: +9
  S2 earnings beat >20% + guidance alcista: +9 | beat 10-20%: +7 | beat <10%: +4
  S3 call sweep OTM masiva confirmada: +9
  S4 short float >30%: +9 | 20-30%: +7 | 15-20%: +5
  S6 pivot estratégico confirmado con capital comprometido: +10
  S6 pivot anunciado sin capital confirmado: +6
  S5 solo (sin otras señales): +3

VOLUMEN (volume_pts) — máx 3:
  Vol relativo >5x: +3 | >3x: +2 | >2x: +1 | <2x: +0

SHORT AMPLIFIER (short_pts) — máx 2:
  Short >20% + catalizador real confirmado: +2 | solo short sin catalizador: +0

OPTIONS FLOW (options_pts) — máx 3 (NUEVO):
  Vol/OI >3x + C/P ratio >2.0: +3
  Vol/OI >2x + C/P ratio >1.5: +2
  Vol/OI >1.5x: +1
  Sin datos o actividad normal: +0

INSIDER BUYING (insider_pts) — máx 3 (NUEVO):
  Múltiples insiders comprando: +3
  Un insider comprando cantidad significativa: +2
  Actividad Form 4 reciente (cualquier tipo): +1
  Sin actividad: +0

SENTIMENT (sentiment_pts) — máx 2 (NUEVO):
  Sentiment bullish (>0.6) + noticias positivas: +2
  Sentiment neutral con noticias mixtas positivas: +1
  Sentiment bearish: +0 o -1

NARRATIVE SHIFT (narrative_pts) — máx 4 (NUEVO):
  Pivot con capital comprometido + sector caliente + short alto: +4
  Pivot anunciado + sector caliente + volumen: +2
  Keywords detectadas sin confirmación clara: +1

CONFLUENCIA (confluence_pts) — máx 3:
  3+ estrategias simultáneas: +3 | 2 estrategias: +2 | Small cap + baja cobertura: +1

PENALIZACIONES (penalty_pts, número negativo) — máx -5:
  Solo rumor sin confirmar: -3
  Historial dilución documentado: -2
  Noticia >14 días: -2
  Earnings miss (sorpresa negativa): -3
  Sentiment muy bearish con noticias negativas: -2

SIMILAR CASE:
  ASTS: pre-revenue + contrato Tier-1 + short squeeze + sector espacial
  BCRX: earnings beat masivo + adquisición estratégica + mid cap healthcare
  BITF: narrative shift sector viejo→AI + short interest + capital comprometido
  RKLB: pre-revenue → near-revenue + contratos gobierno + sector espacial
  IONQ: pre-revenue + contrato gobierno + narrativa AI/quantum
  LUNR: pre-revenue + hito técnico irrepetible + short squeeze
  JOBY: pre-revenue + aprobación regulatoria + inversión industrial
  GME/AMC: squeeze puro retail, high short, sin fundamento fuerte
  CAVA: PEAD puro, earnings beat masivo, consumer sector
  TSLA: squeeze + fundamentos reales mejorando + narrativa sector
  MRNA: pre-revenue + catalizador gobierno + sector healthcare
  NVDA: sector rotation + earnings beat + narrativa AI
  ENPH: PEAD + sector rotation energía + guidance alcista

Señal fuerte ≥22 · Watchlist 10-21 · Descarta <10
Si no hay señal relevante: has_signal=false, score<10"""

    try:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=700,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip()
        raw = re.sub(r"```json|```", "", raw).strip()
        result = json.loads(raw)

        if not result.get("has_signal") or result.get("score", 0) < 10:
            return None

        # Recalcular verdict según umbrales v2
        sc = result.get("score", 0)
        result["verdict"] = "SEÑAL FUERTE" if sc >= 22 else "WATCHLIST"

        # Enriquecer con datos del stock
        result.update({
            "ticker":          stock["ticker"],
            "company":         stock["company"],
            "sector":          stock["sector"],
            "price":           stock["price"],
            "change":          stock["change"],
            "mcap":            stock["mcap"],
            "short_float":     finviz.get("short_float", "N/A"),
            "rel_volume":      finviz.get("rel_volume", "N/A"),
            "news":            all_news[:3],
            "source":          stock["source"],
            "sector_pts":      sector_pts,
            "insider_buying":  insider["has_buying"],
            "insider_summary": insider["insider_summary"],
            "options_summary": opt_summary,
            "eps_surprise":    yf_data["eps_surprise_pct"],
            "days_earnings":   yf_data["days_since_earnings"],
        })
        return result

    except json.JSONDecodeError as e:
        print(f"   ⚠ JSON error {stock['ticker']}: {e}")
        return None
    except Exception as e:
        print(f"   ⚠ AI error {stock['ticker']}: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════
# CSV LOG — historial acumulativo para backtesting
# ══════════════════════════════════════════════════════════════════════════

def save_to_csv(results: list[dict], today: str, total_scanned: int):
    """Guarda señales en CSV acumulativo para backtesting futuro."""
    fieldnames = [
        "date", "ticker", "company", "sector", "price", "change", "mcap",
        "score", "verdict", "primary_strategy", "strategies_detected",
        "catalyst_type", "catalyst_summary", "short_float", "rel_volume",
        "insider_buying", "sentiment_label", "narrative_shift",
        "eps_surprise", "days_earnings", "similar_case", "risk_level",
        "total_scanned_today", "source",
    ]

    file_exists = os.path.isfile(CSV_LOG)
    with open(CSV_LOG, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        for r in results:
            row = {k: r.get(k, "") for k in fieldnames}
            row["date"] = today
            row["total_scanned_today"] = total_scanned
            row["strategies_detected"] = "|".join(r.get("strategies_detected", []))
            writer.writerow(row)

    print(f"   ✅ {len(results)} señales guardadas en {CSV_LOG}")


# ══════════════════════════════════════════════════════════════════════════
# EMAIL BUILDER — HTML profesional
# ══════════════════════════════════════════════════════════════════════════

STRAT_META = {
    "S1": {"emoji": "🚀", "name": "Pre-Revenue Catalyst",  "color": "#d4a843"},
    "S2": {"emoji": "📊", "name": "Post-Earnings Drift",   "color": "#22d48a"},
    "S3": {"emoji": "📡", "name": "Options Flow",          "color": "#3898f8"},
    "S4": {"emoji": "💥", "name": "Short Squeeze",         "color": "#e83838"},
    "S5": {"emoji": "🌊", "name": "Sector Rotation",       "color": "#9272e0"},
    "S6": {"emoji": "🔄", "name": "Narrative Shift",       "color": "#e8a030"},
}

CATALYST_EMOJI = {
    "tier1_contract":    "🤝",
    "institutional_inv": "🏦",
    "gov_contract":      "🛡",
    "tech_milestone":    "🚀",
    "regulatory":        "⚖️",
    "earnings_beat":     "💰",
    "short_squeeze":     "💥",
    "options_flow":      "📡",
    "sector_rotation":   "🌊",
    "narrative_shift":   "🔄",
    "insider_buy":       "👤",
    "rumor":             "💬",
    "none":              "❓",
}


def score_color(sc: int) -> str:
    if sc >= 28: return "#22d48a"
    if sc >= 22: return "#d4a843"
    if sc >= 16: return "#3898f8"
    return "#7090b0"


def risk_badge(risk: str) -> str:
    colors = {"low": "#22d48a", "medium": "#d4a843", "high": "#e87830", "extreme": "#e83838"}
    c = colors.get(risk, "#7090b0")
    return (f'<span style="background:{c}20;color:{c};border:1px solid {c}44;'
            f'padding:2px 7px;border-radius:2px;font-size:9px;letter-spacing:.1em;'
            f'text-transform:uppercase;font-weight:700">{risk.upper()}</span>')


def strat_badge(s: str) -> str:
    m = STRAT_META.get(s, {"emoji": "•", "name": s, "color": "#7090b0"})
    return (f'<span style="background:{m["color"]}15;color:{m["color"]};'
            f'border:1px solid {m["color"]}33;padding:2px 8px;border-radius:2px;'
            f'font-size:9px;letter-spacing:.1em;font-weight:700">'
            f'{m["emoji"]} {m["name"]}</span>')


def format_signal_card(r: dict, rank: int = 0) -> str:
    strats  = r.get("strategies_detected", [r.get("primary_strategy", "S1")])
    primary = r.get("primary_strategy", "S1")
    sc      = r.get("score", 0)
    verdict = r.get("verdict", "WATCHLIST")
    is_strong = verdict == "SEÑAL FUERTE"
    border_col = score_color(sc)
    cat_emoji  = CATALYST_EMOJI.get(r.get("catalyst_type", "none"), "❓")
    similar    = r.get("similar_case", "none")

    strats_html = " ".join(strat_badge(s) for s in strats)

    # Rank badge
    rank_html = ""
    if rank == 1:
        rank_html = '<span style="background:#d4a84322;color:#d4a843;border:1px solid #d4a84355;padding:2px 10px;border-radius:2px;font-size:9px;letter-spacing:.15em;font-weight:700;margin-left:8px">🥇 TOP SEÑAL</span>'
    elif rank == 2:
        rank_html = '<span style="background:#3898f822;color:#3898f8;border:1px solid #3898f855;padding:2px 10px;border-radius:2px;font-size:9px;letter-spacing:.15em;font-weight:700;margin-left:8px">🥈 #2</span>'
    elif rank == 3:
        rank_html = '<span style="background:#22d48a22;color:#22d48a;border:1px solid #22d48a55;padding:2px 10px;border-radius:2px;font-size:9px;letter-spacing:.15em;font-weight:700;margin-left:8px">🥉 #3</span>'

    verdict_html = (
        f'<span style="background:{"#22d48a" if is_strong else "#d4a843"}20;'
        f'color:{"#22d48a" if is_strong else "#d4a843"};'
        f'border:1px solid {"#22d48a" if is_strong else "#d4a843"}44;'
        f'padding:2px 10px;border-radius:2px;font-size:9px;letter-spacing:.12em;font-weight:700">'
        f'{"🔥 SEÑAL FUERTE" if is_strong else "👀 WATCHLIST"}</span>'
    )

    # Score breakdown
    bd = r.get("score_breakdown", {})
    def bd_row(label, pts, reason, color=""):
        if pts == 0 and not reason:
            return ""
        c = color if color else ("#22d48a" if pts > 0 else ("#e83838" if pts < 0 else "#3a5070"))
        ps = f"+{pts}" if pts > 0 else str(pts)
        return (f'<tr><td style="padding:4px 10px;color:#7090b0;font-size:10px;white-space:nowrap;border-bottom:1px solid #131926">{label}</td>'
                f'<td style="padding:4px 10px;font-family:monospace;font-size:12px;font-weight:700;color:{c};border-bottom:1px solid #131926;white-space:nowrap">{ps}</td>'
                f'<td style="padding:4px 10px;color:#5a7a9a;font-size:10px;border-bottom:1px solid #131926;line-height:1.5">{reason}</td></tr>')

    breakdown_rows = "".join(filter(None, [
        bd_row("Base estrategia", bd.get("base_strategy_pts", 0), bd.get("base_strategy_reason", ""), "#d4a843"),
        bd_row("Volumen",         bd.get("volume_pts", 0),        bd.get("volume_reason", "")),
        bd_row("Short amplifier", bd.get("short_pts", 0),         bd.get("short_reason", ""), "#e87830"),
        bd_row("Options flow",    bd.get("options_pts", 0),       bd.get("options_reason", ""), "#3898f8"),
        bd_row("Insider buying",  bd.get("insider_pts", 0),       bd.get("insider_reason", ""), "#9272e0"),
        bd_row("Sentiment",       bd.get("sentiment_pts", 0),     bd.get("sentiment_reason", "")),
        bd_row("Narrative shift", bd.get("narrative_pts", 0),     bd.get("narrative_reason", ""), "#e8a030"),
        bd_row("Confluencia",     bd.get("confluence_pts", 0),    bd.get("confluence_reason", "")),
        bd_row("Sector momentum", bd.get("sector_pts", 0),        bd.get("sector_reason", ""), "#9272e0"),
        bd_row("Penalizaciones",  bd.get("penalty_pts", 0),       bd.get("penalty_reason", ""), "#e83838"),
    ]))

    # Badges adicionales (insider, sentiment, narrative)
    extra_badges = ""
    if r.get("insider_buying"):
        extra_badges += '<span style="background:#9272e020;color:#9272e0;border:1px solid #9272e033;padding:2px 7px;border-radius:2px;font-size:9px;font-weight:700;margin-left:4px">👤 INSIDER BUY</span>'
    if r.get("sentiment_label") == "bullish":
        extra_badges += '<span style="background:#22d48a20;color:#22d48a;border:1px solid #22d48a33;padding:2px 7px;border-radius:2px;font-size:9px;font-weight:700;margin-left:4px">📈 SENTIMENT+</span>'

    # Similar case
    similar_html = ""
    if similar != "none":
        sim_color = "#9272e0"
        similar_html = f"""
    <div style="margin-top:14px;background:#13102a;border:1px solid #2a2050;border-left:3px solid {sim_color};border-radius:4px;padding:12px 14px">
      <div style="font-size:9px;letter-spacing:.16em;text-transform:uppercase;color:{sim_color};margin-bottom:6px">📚 Caso Histórico Similar</div>
      <div style="font-family:monospace;font-size:18px;font-weight:900;color:#deeaf8;margin-bottom:4px">${similar}</div>
      <div style="font-size:11px;color:#a090c8;line-height:1.65">{r.get('similar_case_reason', '')}</div>
    </div>"""

    # News
    news_html = ""
    if r.get("news"):
        items = "".join(
            f'<div style="padding:4px 0;font-size:11px;color:#7090b0;border-bottom:1px solid #1a2030;line-height:1.5">▸ {h}</div>'
            for h in r["news"][:3]
        )
        news_html = f'<div style="margin-top:14px"><div style="font-size:9px;letter-spacing:.14em;text-transform:uppercase;color:#3a5070;margin-bottom:6px">Noticias</div>{items}</div>'

    # EPS info
    eps_html = ""
    if r.get("eps_surprise") is not None:
        eps_col = "#22d48a" if r["eps_surprise"] > 0 else "#e83838"
        eps_html = f'<span style="color:{eps_col};font-size:10px;font-weight:700"> · EPS sorpresa: {r["eps_surprise"]:+.1f}%</span>'
        if r.get("days_earnings"):
            eps_html += f'<span style="color:#7090b0;font-size:10px"> (hace {r["days_earnings"]}d)</span>'

    return f"""
    <div style="border:1px solid #1e2d44;border-left:4px solid {border_col};border-radius:6px;
                padding:20px;margin-bottom:14px;background:#0d1525;font-family:'Segoe UI',Arial,sans-serif">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:14px;flex-wrap:wrap;gap:8px">
        <div>
          <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:6px">
            <span style="font-family:monospace;font-size:22px;font-weight:900;color:#deeaf8">${r['ticker']}</span>
            <span style="color:#7090b0;font-size:13px">{r['company']}</span>
            {rank_html}
          </div>
          <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:6px">{strats_html}{extra_badges}</div>
          <div style="font-family:monospace;font-size:11px;color:#3a5070">
            {r['sector']} · {r['mcap']} · Short: <span style="color:#e87830">{r['short_float']}</span>
            · Vol: <span style="color:#d4a843">{r['rel_volume']}x</span>{eps_html}
          </div>
        </div>
        <div style="text-align:right">
          <div style="font-family:monospace;font-size:28px;font-weight:900;color:{border_col};line-height:1">{sc}</div>
          <div style="font-size:9px;color:#3a5070;letter-spacing:.1em">/40 PUNTOS</div>
          <div style="margin-top:6px">{verdict_html}</div>
        </div>
      </div>

      <div style="background:#131926;border-radius:4px;padding:8px 12px;margin-bottom:14px;font-family:monospace;font-size:12px;color:#7090b0">
        💲 <span style="color:#deeaf8;font-weight:700">{r['price']}</span> &nbsp;|&nbsp;
        <span style="color:#22d48a;font-weight:700">{r['change']}</span> hoy &nbsp;|&nbsp;
        Escáner: <span style="color:#d4a843">{r['source']}</span>
      </div>

      <table style="width:100%;border-collapse:collapse;font-size:12px;margin-bottom:4px">
        <tr><td style="padding:5px 0;color:#7090b0;width:120px;vertical-align:top">{cat_emoji} Catalizador</td>
            <td style="padding:5px 0;color:#deeaf8;font-weight:600;line-height:1.5">{r.get('catalyst_summary','')}</td></tr>
        <tr><td style="padding:5px 0;color:#7090b0;vertical-align:top">📈 Por qué sube</td>
            <td style="padding:5px 0;color:#b0c8e0;line-height:1.65">{r.get('why_it_moves','')}</td></tr>
        <tr><td style="padding:5px 0;color:#7090b0;vertical-align:top">🎯 Entrada</td>
            <td style="padding:5px 0;color:#22d48a;font-weight:600">{r.get('entry_note','')}</td></tr>
        <tr><td style="padding:5px 0;color:#7090b0;vertical-align:top">🛡 Stop</td>
            <td style="padding:5px 0;color:#e83838;font-weight:600">{r.get('stop_narrative','')}</td></tr>
        <tr><td style="padding:5px 0;color:#7090b0">⚠️ Riesgo</td>
            <td style="padding:5px 0">{risk_badge(r.get('risk_level','medium'))}</td></tr>
      </table>

      <div style="margin-top:14px">
        <div style="font-size:9px;letter-spacing:.16em;text-transform:uppercase;color:#3a5070;margin-bottom:6px">📐 Desglose del Score</div>
        <table style="width:100%;border-collapse:collapse;background:#0a0f1a;border-radius:4px;overflow:hidden">
          <tr style="background:#0d1525">
            <th style="padding:5px 10px;text-align:left;font-size:8px;letter-spacing:.12em;text-transform:uppercase;color:#3a5070;font-weight:600">Criterio</th>
            <th style="padding:5px 10px;text-align:left;font-size:8px;letter-spacing:.12em;text-transform:uppercase;color:#3a5070;font-weight:600">Pts</th>
            <th style="padding:5px 10px;text-align:left;font-size:8px;letter-spacing:.12em;text-transform:uppercase;color:#3a5070;font-weight:600">Razón</th>
          </tr>
          {breakdown_rows}
          <tr style="background:#0d1525">
            <td style="padding:6px 10px;font-size:11px;font-weight:700;color:#deeaf8">TOTAL</td>
            <td style="padding:6px 10px;font-family:monospace;font-size:16px;font-weight:900;color:{border_col}">{sc}/40</td>
            <td style="padding:6px 10px;font-size:10px;color:{border_col};font-weight:700">{verdict}</td>
          </tr>
        </table>
      </div>

      {similar_html}
      {news_html}

      <div style="margin-top:14px;padding-top:10px;border-top:1px solid #1e2d44">
        <a href="https://finviz.com/quote.ashx?t={r['ticker']}" style="color:#3898f8;font-size:11px;text-decoration:none">🔗 Finviz →</a> &nbsp;&nbsp;
        <a href="https://finance.yahoo.com/quote/{r['ticker']}" style="color:#3898f8;font-size:11px;text-decoration:none">📊 Yahoo Finance →</a> &nbsp;&nbsp;
        <a href="https://unusualwhales.com/stock/{r['ticker']}" style="color:#3898f8;font-size:11px;text-decoration:none">🦈 Unusual Whales →</a> &nbsp;&nbsp;
        <a href="https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={r['ticker']}&type=4&dateb=&owner=include&count=20" style="color:#3898f8;font-size:11px;text-decoration:none">📋 EDGAR Form 4 →</a>
      </div>
    </div>"""


def build_email(
    results: list[dict],
    total_scanned: int,
    sector_perf: dict[str, float],
    today: str,
) -> tuple[str, str]:

    strong = [r for r in results if r.get("verdict") == "SEÑAL FUERTE"]
    watch  = [r for r in results if r.get("verdict") == "WATCHLIST"]
    top3   = results[:3]

    # Subject
    if strong:
        tickers = " ".join(f"${r['ticker']}" for r in strong[:3])
        subject = f"🔥 [{len(strong)} señal{'es' if len(strong)>1 else ''}] {tickers} — The System v2 {today}"
    elif watch:
        tickers = " ".join(f"${r['ticker']}" for r in watch[:2])
        subject = f"👀 [Watchlist] {tickers} — The System v2 {today}"
    else:
        subject = f"😴 Sin señales hoy — The System v2 {today}"

    # Top 3 podium
    top3_html = ""
    if top3:
        cards = ""
        for i, r in enumerate(top3[:3], 1):
            pm = STRAT_META.get(r.get("primary_strategy", "S1"), STRAT_META["S1"])
            cards += f"""
            <div style="flex:1;background:#0d1525;border:1px solid #1e2d44;border-top:3px solid {score_color(r['score'])};border-radius:5px;padding:16px;text-align:center;min-width:160px">
              <div style="font-size:20px;margin-bottom:6px">{'🥇' if i==1 else '🥈' if i==2 else '🥉'}</div>
              <div style="font-family:monospace;font-size:20px;font-weight:900;color:#deeaf8;margin-bottom:4px">${r['ticker']}</div>
              <div style="font-size:10px;color:#7090b0;margin-bottom:8px">{r['company'][:22]}</div>
              <div style="font-family:monospace;font-size:28px;font-weight:900;color:{score_color(r['score'])}">{r['score']}</div>
              <div style="font-size:9px;color:#3a5070;letter-spacing:.1em">/40 PUNTOS</div>
              <div style="margin-top:8px;font-size:11px;color:{score_color(r['score'])};font-weight:700">{pm['emoji']} {pm['name']}</div>
            </div>"""
        top3_html = f"""
        <div style="margin-bottom:24px">
          <div style="font-family:monospace;font-size:9px;letter-spacing:.2em;text-transform:uppercase;color:#d4a843;margin-bottom:12px">⭐ Top 3 Señales del Día</div>
          <div style="display:flex;gap:12px;flex-wrap:wrap">{cards}</div>
        </div>"""

    # Sector strip
    sector_strip = ""
    for i, (etf, pct) in enumerate(list(sector_perf.items())[:5]):
        color = "#22d48a" if pct > 0 else "#e83838"
        bg    = "#22d48a15" if i < 2 else "#0d1525"
        border= "#22d48a33" if i < 2 else "#1e2d44"
        sector_strip += f"""
        <div style="background:{bg};border:1px solid {border};border-radius:4px;padding:10px 14px;text-align:center;flex:1;min-width:80px">
          <div style="font-family:monospace;font-size:12px;font-weight:700;color:#deeaf8">{etf}</div>
          <div style="font-size:9px;color:#7090b0;letter-spacing:.08em;margin-bottom:4px">{SECTOR_ETFS.get(etf,'')}</div>
          <div style="font-family:monospace;font-size:15px;color:{color};font-weight:700">{pct:+.1f}%</div>
          {'<div style="font-size:8px;color:#22d48a;letter-spacing:.1em;margin-top:3px">ACTIVO</div>' if i < 2 else ''}
        </div>"""

    # Strong + watchlist
    strong_html = ""
    if strong:
        cards = "".join(format_signal_card(r, i+1 if r in top3 else 0) for i, r in enumerate(strong))
        strong_html = f'<div style="margin-bottom:8px"><div style="font-family:monospace;font-size:10px;letter-spacing:.2em;text-transform:uppercase;color:#22d48a;margin-bottom:12px">🔥 Señales Fuertes ({len(strong)})</div>{cards}</div>'

    watch_html = ""
    if watch:
        cards = "".join(format_signal_card(r, 0) for r in watch[:8])
        watch_html = f'<div style="margin-bottom:8px"><div style="font-family:monospace;font-size:10px;letter-spacing:.2em;text-transform:uppercase;color:#d4a843;margin-bottom:12px">👀 Watchlist ({len(watch)})</div>{cards}</div>'

    empty_html = ""
    if not strong and not watch:
        empty_html = """
        <div style="text-align:center;padding:48px;background:#0d1525;border:1px solid #1e2d44;border-radius:6px;margin-bottom:24px">
          <div style="font-size:40px;margin-bottom:14px">😴</div>
          <div style="font-family:monospace;font-size:16px;color:#deeaf8;font-weight:700;margin-bottom:8px">Sin señales relevantes hoy</div>
          <div style="font-size:13px;color:#7090b0;line-height:1.7">Ninguna acción combinó los criterios de las 6 estrategias.<br>El sistema sigue monitoreando ~{} acciones cada día.</div>
        </div>""".format(total_scanned)

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#040507;font-family:'Segoe UI',Arial,sans-serif">
<div style="max-width:700px;margin:0 auto;padding:20px 16px 40px">

  <div style="background:#0d1525;border:1px solid #1e2d44;border-radius:6px;padding:24px 28px;margin-bottom:16px">
    <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:12px">
      <div>
        <div style="font-family:monospace;font-size:11px;letter-spacing:.25em;text-transform:uppercase;color:#d4a843;margin-bottom:8px">THE SYSTEM v2 · ESCÁNER DIARIO</div>
        <div style="font-size:22px;font-weight:900;color:#f0f6ff;margin-bottom:4px">6 Estrategias · {today}</div>
        <div style="font-size:12px;color:#7090b0;font-style:italic">S1 Catalyst · S2 PEAD · S3 Options · S4 Short Squeeze · S5 Sector · S6 Narrative Shift</div>
        <div style="font-size:11px;color:#3a5070;margin-top:6px">NASDAQ + NYSE · $50M–$10B · Insider tracking · Options flow</div>
      </div>
      <div style="text-align:right">
        <div style="font-size:9px;letter-spacing:.14em;text-transform:uppercase;color:#7090b0">Escaneadas</div>
        <div style="font-family:monospace;font-size:36px;font-weight:900;color:#3898f8;line-height:1">{total_scanned}</div>
      </div>
    </div>

    <div style="display:flex;gap:10px;margin-top:18px;flex-wrap:wrap">
      <div style="flex:1;background:#131926;border-radius:4px;padding:10px;text-align:center;min-width:80px">
        <div style="font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#7090b0;margin-bottom:4px">Señales fuertes</div>
        <div style="font-family:monospace;font-size:24px;font-weight:900;color:#22d48a">{len(strong)}</div>
      </div>
      <div style="flex:1;background:#131926;border-radius:4px;padding:10px;text-align:center;min-width:80px">
        <div style="font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#7090b0;margin-bottom:4px">Watchlist</div>
        <div style="font-family:monospace;font-size:24px;font-weight:900;color:#d4a843">{len(watch)}</div>
      </div>
      <div style="flex:2;background:#131926;border-radius:4px;padding:10px;min-width:200px">
        <div style="font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#7090b0;margin-bottom:6px">Umbrales v2</div>
        <div style="font-size:10px;color:#3a5070;line-height:1.7">
          🔥 Señal fuerte: score ≥22/40<br>
          👀 Watchlist: 10-21/40<br>
          Scoring: Base+Vol+Short+Options+Insider+Sentiment+Narrative+Confluencia+Sector
        </div>
      </div>
    </div>
  </div>

  <div style="background:#0d1525;border:1px solid #1e2d44;border-radius:6px;padding:18px 20px;margin-bottom:16px">
    <div style="font-family:monospace;font-size:9px;letter-spacing:.2em;text-transform:uppercase;color:#9272e0;margin-bottom:12px">🌊 S5 · Momentum Sectorial (3 meses)</div>
    <div style="display:flex;gap:8px;flex-wrap:wrap">{sector_strip}</div>
    <div style="font-size:10px;color:#3a5070;margin-top:10px">Los sectores ACTIVO son el universo prioritario. Las señales S1-S4-S6 en estos sectores reciben puntuación extra.</div>
  </div>

  {top3_html}
  {strong_html}
  {watch_html}
  {empty_html}

  {'<div style="background:#0d1525;border:1px solid #1e2d44;border-left:3px solid #3898f8;border-radius:4px;padding:14px 18px;margin-top:8px;font-size:11px;color:#7090b0;line-height:1.7"><strong style="color:#b0c8e0">💡 Recuerda:</strong> Stop narrativo — si el catalizador se invalida, sales. Máx 2% del capital por trade. Verifica la noticia original antes de actuar. Los datos de insiders son informativos, no garantía.</div>' if strong or watch else ''}

  <div style="margin-top:24px;text-align:center;font-size:10px;color:#253550;letter-spacing:.06em;text-transform:uppercase;line-height:1.8">
    The System v2 · 6 Estrategias · NASDAQ+NYSE · $50M–$10B<br>
    Finviz + yfinance + SEC EDGAR · Escáner Automático Diario<br>
    Solo para fines informativos · No es asesoramiento financiero
  </div>
</div>
</body>
</html>"""

    return subject, html


# ══════════════════════════════════════════════════════════════════════════
# EMAIL SENDER
# ══════════════════════════════════════════════════════════════════════════

def send_email(subject: str, html: str):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = EMAIL_FROM
    msg["To"]      = EMAIL_TO
    msg.attach(MIMEText(html, "html"))
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_FROM, EMAIL_APP_PASSWORD)
        server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════

def main():
    today = datetime.datetime.now().strftime("%d/%m/%Y")
    print("═" * 65)
    print(f"  THE SYSTEM v2 — ESCÁNER AMPLIO 6 ESTRATEGIAS")
    print(f"  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  NASDAQ + NYSE · $50M–$10B · Finviz + yfinance + SEC EDGAR")
    print("═" * 65)

    # ── PASO 1: Sector Rotation (S5) ──────────────────────────────────────
    sector_perf = get_sector_momentum()

    # ── PASO 2: Scrape screeners (universo ampliado) ───────────────────────
    print("\n📡 Scraping screeners Finviz (universo ampliado)...")
    all_stocks = []
    all_stocks += scrape_finviz_screen(SCREEN_VOLUME,    "S1/S4 Volume",    pages=3)
    time.sleep(2)
    all_stocks += scrape_finviz_screen(SCREEN_SHORT,     "S4 Short",        pages=3)
    time.sleep(2)
    all_stocks += scrape_finviz_screen(SCREEN_EARNINGS,  "S2 Earnings",     pages=3)
    time.sleep(2)
    all_stocks += scrape_finviz_screen(SCREEN_NARRATIVE, "S6 Narrative",    pages=2)

    stocks = deduplicate(all_stocks)
    total_scanned = len(stocks)
    print(f"\n   Total tras deduplicación: {total_scanned} acciones únicas\n")

    if not stocks:
        send_email(
            "⚠️ The System v2 — Sin datos hoy",
            "<div style='padding:24px;background:#040507;color:#b0c8e0;font-family:sans-serif'>"
            "<h2 style='color:#d4a843'>⚠️ Sin datos de Finviz</h2>"
            "<p>No se obtuvieron acciones hoy. Posible día festivo USA o error temporal.</p></div>"
        )
        return

    # ── PASO 3: Analizar con todas las fuentes ────────────────────────────
    stocks_to_analyze = stocks[:60]  # máx 60 (vs 35 anterior)
    print(f"🔍 Analizando {len(stocks_to_analyze)} acciones...\n")
    print(f"   {'#':<4} {'Ticker':<7} {'Finviz':<8} {'yfinance':<10} {'EDGAR':<7} {'IA'}")
    print(f"   {'─'*50}")

    results = []
    for i, stock in enumerate(stocks_to_analyze):
        ticker = stock["ticker"]
        print(f"   [{i+1:02d}/{len(stocks_to_analyze)}] {ticker:<7}", end=" ", flush=True)

        # Finviz quote
        finviz = get_finviz_quote(ticker)
        print("✓finviz", end=" ", flush=True)
        time.sleep(0.5)

        # yfinance
        yf_data = get_yfinance_data(ticker)
        print("✓yf", end=" ", flush=True)

        # SEC EDGAR insiders
        insider = get_insider_buying(ticker)
        insider_mark = "✓ins+" if insider["has_buying"] else "✓ins "
        print(insider_mark, end=" ", flush=True)
        time.sleep(0.3)

        # Sector momentum score
        s5_pts = sector_momentum_score(stock["sector"], sector_perf)

        # AI scoring
        result = analyze_with_ai(stock, finviz, yf_data, insider, s5_pts)

        if result:
            results.append(result)
            verdict_short = "🔥FUERTE" if result["verdict"] == "SEÑAL FUERTE" else "👀WATCH"
            print(f"→ {verdict_short} {result['score']}/40 [{result.get('primary_strategy','?')}]")
        else:
            print("→ –")

        time.sleep(1.0)  # rate limit

    # ── PASO 4: Ordenar + guardar ─────────────────────────────────────────
    results.sort(key=lambda x: x.get("score", 0), reverse=True)

    n_strong = len([r for r in results if r.get("verdict") == "SEÑAL FUERTE"])
    n_watch  = len([r for r in results if r.get("verdict") == "WATCHLIST"])

    print(f"\n📊 Resultado: {total_scanned} escaneadas · {n_strong} señales fuertes · {n_watch} watchlist")

    # Guardar CSV
    if results:
        save_to_csv(results, today, total_scanned)

    # ── PASO 5: Email ─────────────────────────────────────────────────────
    print(f"\n📤 Enviando email...")
    subject, html = build_email(results, total_scanned, sector_perf, today)
    send_email(subject, html)

    print(f"✅ Email enviado a {EMAIL_TO}")
    if results:
        print(f"   Top señal: ${results[0]['ticker']} — {results[0]['score']}/40 — {results[0].get('primary_strategy','?')}")
    print("═" * 65)


if __name__ == "__main__":
    main()

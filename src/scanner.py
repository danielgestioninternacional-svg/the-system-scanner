"""
═══════════════════════════════════════════════════════════════════
THE SYSTEM — ESCÁNER COMBINADO DE 5 ESTRATEGIAS
Ejecuta diariamente via GitHub Actions a las 22:30h España

Estrategias detectadas:
  S1 · Pre-Revenue Catalyst   (volumen + noticias + short interest)
  S2 · Post-Earnings Drift    (earnings beat reciente + drift)
  S3 · Options Flow           (volumen opciones inusual)
  S4 · Short Squeeze          (short interest >20% + catalizador)
  S5 · Sector Rotation        (sector en momentum 3 meses)

Output: Email HTML con top señales ordenadas por score 0-30
═══════════════════════════════════════════════════════════════════
"""

import os, re, time, json, datetime, smtplib, traceback
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from bs4 import BeautifulSoup
from anthropic import Anthropic

# ── Env vars ──────────────────────────────────────────────
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

# ══════════════════════════════════════════════════════════════
# SECTOR ROTATION — Estrategia 5 (filtro de contexto)
# ══════════════════════════════════════════════════════════════

# ETF de sector → nombre legible
SECTOR_ETFS = {
    "XLK": "Technology",
    "XLV": "Healthcare",
    "XLE": "Energy",
    "XLF": "Financials",
    "XLI": "Industrials",
    "XLY": "Consumer Discretionary",
    "XLP": "Consumer Staples",
    "XLB": "Materials",
    "XLRE": "Real Estate",
    "XLU": "Utilities",
    "XLC": "Communication Services",
}

# Mapeo sector Finviz → ETF sector
FINVIZ_TO_SECTOR = {
    "Technology":              "XLK",
    "Healthcare":              "XLV",
    "Energy":                  "XLE",
    "Financial":               "XLF",
    "Financial Services":      "XLF",
    "Industrials":             "XLI",
    "Consumer Cyclical":       "XLY",
    "Consumer Defensive":      "XLP",
    "Basic Materials":         "XLB",
    "Real Estate":             "XLRE",
    "Utilities":               "XLU",
    "Communication Services":  "XLC",
}


def get_sector_momentum() -> dict[str, float]:
    """
    Calcula el retorno de 3 meses de cada ETF sectorial via Finviz.
    Devuelve dict ETF -> pct_change_3m ordenado descendente.
    """
    print("📡 [S5] Calculando momentum sectorial...")
    perf = {}
    for etf in SECTOR_ETFS:
        try:
            url = f"https://finviz.com/quote.ashx?t={etf}&p=d"
            r = requests.get(url, headers=HEADERS, timeout=12)
            soup = BeautifulSoup(r.text, "html.parser")
            # Buscar "Perf Quarter" en la tabla snapshot
            cells = soup.find_all("td")
            for i, cell in enumerate(cells):
                if "Perf Quarter" in cell.get_text():
                    val_text = cells[i + 1].get_text(strip=True).replace("%", "")
                    try:
                        perf[etf] = float(val_text)
                    except ValueError:
                        perf[etf] = 0.0
                    break
            time.sleep(0.8)
        except Exception as e:
            print(f"   ⚠ ETF {etf}: {e}")
            perf[etf] = 0.0

    sorted_perf = dict(sorted(perf.items(), key=lambda x: x[1], reverse=True))
    top3 = list(sorted_perf.keys())[:3]
    print(f"   Top sectores: {top3} → {[f'{sorted_perf[e]:+.1f}%' for e in top3]}")
    return sorted_perf


def sector_momentum_score(finviz_sector: str, sector_perf: dict[str, float]) -> int:
    """Devuelve 0-3 puntos extra según si el sector está en momentum."""
    etf = FINVIZ_TO_SECTOR.get(finviz_sector, "")
    if not etf:
        return 0
    perf_values = sorted(sector_perf.values(), reverse=True)
    if not perf_values:
        return 0
    etf_perf = sector_perf.get(etf, 0)
    rank = sorted(sector_perf.values(), reverse=True).index(etf_perf) + 1
    if rank <= 2:
        return 3   # Top 2 sectores
    if rank <= 4:
        return 2   # Top 4
    if rank <= 6:
        return 1   # Top mitad
    return 0


# ══════════════════════════════════════════════════════════════
# FINVIZ SCREENERS — S1 + S4 (volumen fuerte + short squeeze)
# ══════════════════════════════════════════════════════════════

# S1: Pre-Revenue / Momentum — volumen alto + subida
SCREEN_VOLUME = (
    "https://finviz.com/screener.ashx?v=111"
    "&f=cap_smallover,cap_midunder,geo_usa,"
    "sh_opt_option,ta_change_u3,sh_relvol_o2"
    "&o=-relativevolume&r=1"
)

# S4: Short Squeeze candidates — short interest alto + movimiento
SCREEN_SHORT = (
    "https://finviz.com/screener.ashx?v=111"
    "&f=cap_smallover,cap_midunder,geo_usa,"
    "sh_opt_option,sh_short_o15,ta_change_u1"
    "&o=-shortfloat&r=1"
)

# S2: PEAD — acciones que reportaron earnings recientemente y siguen subiendo
SCREEN_EARNINGS = (
    "https://finviz.com/screener.ashx?v=111"
    "&f=cap_smallover,cap_midunder,geo_usa,"
    "earningsdate_prevweek,ta_change_u2,ta_perf_dup"
    "&o=-change&r=1"
)


def scrape_finviz_screen(url: str, label: str) -> list[dict]:
    """Scrapea una URL de Finviz screener y devuelve lista de stocks."""
    print(f"   Scraping {label}...")
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # Intentar ambos selectores conocidos
        table = (
            soup.find("table", {"id": "screener-views-table"}) or
            soup.find("table", class_="screener_table") or
            soup.find("table", {"class": "table-light"})
        )

        stocks = []
        if not table:
            print(f"   ⚠ No se encontró tabla en {label}")
            return stocks

        rows = table.find_all("tr")[1:]
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 10:
                continue
            try:
                stocks.append({
                    "ticker":   cols[1].get_text(strip=True),
                    "company":  cols[2].get_text(strip=True),
                    "sector":   cols[3].get_text(strip=True),
                    "industry": cols[4].get_text(strip=True),
                    "country":  cols[5].get_text(strip=True),
                    "mcap":     cols[6].get_text(strip=True),
                    "pe":       cols[7].get_text(strip=True),
                    "price":    cols[8].get_text(strip=True),
                    "change":   cols[9].get_text(strip=True),
                    "volume":   cols[10].get_text(strip=True) if len(cols) > 10 else "N/A",
                    "source":   label,
                })
            except Exception:
                continue
        print(f"   → {len(stocks)} acciones encontradas en {label}")
        return stocks
    except Exception as e:
        print(f"   ⚠ Error scraping {label}: {e}")
        return []


# ══════════════════════════════════════════════════════════════
# FINVIZ QUOTE — noticias + datos extra
# ══════════════════════════════════════════════════════════════

def get_quote_data(ticker: str) -> dict:
    """
    Extrae de la página de quote de Finviz:
    - Últimas noticias (hasta 6)
    - Short float %
    - Relative volume
    - EPS surprise (si disponible)
    - Beta
    - Inst. ownership
    """
    url = f"https://finviz.com/quote.ashx?t={ticker}&p=d"
    data = {
        "news": [],
        "short_float": "N/A",
        "rel_volume": "N/A",
        "eps_surprise": "N/A",
        "beta": "N/A",
        "inst_own": "N/A",
        "perf_week": "N/A",
        "perf_month": "N/A",
        "earnings_date": "N/A",
        "roic": "N/A",
        "roa": "N/A",
        "sales_qoq": "N/A",
    }
    try:
        r = requests.get(url, headers=HEADERS, timeout=14)
        soup = BeautifulSoup(r.text, "html.parser")

        # Snapshot table — buscar campos por nombre
        snap_keys   = soup.find_all("td", class_="snapshot-td2-cp")
        snap_values = soup.find_all("td", class_="snapshot-td2")

        for i, key_cell in enumerate(snap_keys):
            key = key_cell.get_text(strip=True)
            val = snap_values[i].get_text(strip=True) if i < len(snap_values) else ""
            if key == "Short Float":      data["short_float"]   = val
            elif key == "Rel Volume":     data["rel_volume"]    = val
            elif key == "Beta":           data["beta"]          = val
            elif key == "Inst Own":       data["inst_own"]      = val
            elif key == "Perf Week":      data["perf_week"]     = val
            elif key == "Perf Month":     data["perf_month"]    = val
            elif key == "Earnings":       data["earnings_date"] = val
            elif key == "ROIC":           data["roic"]          = val
            elif key == "ROA":            data["roa"]           = val
            elif key == "Sales Q/Q":      data["sales_qoq"]     = val
            elif key == "EPS this Y":     data["eps_surprise"]  = val

        # News table
        news_table = soup.find("table", {"id": "news-table"})
        if news_table:
            for row in news_table.find_all("tr")[:6]:
                tds = row.find_all("td")
                if len(tds) >= 2:
                    headline = tds[1].get_text(strip=True)
                    if headline:
                        data["news"].append(headline)

    except Exception as e:
        print(f"   ⚠ Quote data error {ticker}: {e}")

    return data


# ══════════════════════════════════════════════════════════════
# DEDUPLICATION
# ══════════════════════════════════════════════════════════════

def deduplicate(stocks: list[dict]) -> list[dict]:
    """Elimina duplicados conservando el de mayor relevancia."""
    seen = {}
    for s in stocks:
        t = s["ticker"]
        if t not in seen:
            seen[t] = s
        else:
            # Si aparece en múltiples screeners, marcar como multi-señal
            existing_sources = seen[t].get("source", "")
            seen[t]["source"] = f"{existing_sources} + {s['source']}"
    return list(seen.values())


# ══════════════════════════════════════════════════════════════
# AI ANALYSIS — Claude Haiku
# ══════════════════════════════════════════════════════════════

STRATEGY_DEFINITIONS = """
S1 · PRE-REVENUE CATALYST: Empresa sin revenue o pre-rentabilidad con catalizador que elimina el mayor riesgo. Contratos Tier-1, inversión institucional directa, hito técnico demostrado, contrato gubernamental. Score máximo si la noticia es contrato con dinero real comprometido.

S2 · POST-EARNINGS DRIFT (PEAD): Empresa reportó earnings en los últimos 7 días con beat significativo (>10% sobre estimaciones) en EPS y/o revenue. El precio continúa subiendo post-earnings. Mayor score si baja cobertura analista + guidance alcista.

S3 · OPTIONS FLOW: Evidencia de actividad institucional en opciones. Call sweeps masivas OTM, volumen de opciones inusual (>3x open interest), dark pool activity. Señal de que alguien con información compra con urgencia.

S4 · SHORT SQUEEZE: Short float >15%. Mayor score cuanto más alto sea el short interest. El catalizador destruye la tesis bajista. Days to cover > 5 es señal adicional de squeeze potente. Con short >30% el squeeze puede ser exponencial.

S5 · SECTOR ROTATION: El sector de la empresa está en los top-3 de momentum (calculado por el scanner). Esta estrategia añade puntos como contexto favorable, no es señal independiente.
"""


def analyze_with_ai(stock: dict, quote: dict, sector_pts: int) -> dict | None:
    """
    Envía datos a Claude Haiku para análisis.
    Devuelve dict con análisis o None si no hay señal relevante.
    """
    news_text = "\n".join(f"- {h}" for h in quote["news"][:5]) if quote["news"] else "Sin noticias disponibles"

    prompt = f"""Analiza esta acción que apareció en el escáner de trading "The System".

DATOS DE MERCADO:
Ticker: {stock['ticker']}
Empresa: {stock['company']}
Sector: {stock['sector']} / Industria: {stock['industry']}
Precio: {stock['price']} | Cambio hoy: {stock['change']}
Volumen relativo: {quote['rel_volume']}x
Market Cap: {stock['mcap']}
Short Float: {quote['short_float']}
Beta: {quote['beta']}
Inst. Ownership: {quote['inst_own']}
ROIC: {quote['roic']} | ROA: {quote['roa']}
Sales Q/Q: {quote['sales_qoq']}
Perf semana: {quote['perf_week']} | Perf mes: {quote['perf_month']}
Fecha earnings: {quote['earnings_date']}
Screener origen: {stock['source']}
Puntos por sector momentum: {sector_pts}/3

NOTICIAS DEL DÍA:
{news_text}

ESTRATEGIAS A DETECTAR:
{STRATEGY_DEFINITIONS}

INSTRUCCIÓN: Detecta qué estrategia(s) aplican, puntúa el setup con desglose detallado, e identifica el caso histórico más similar.

Responde SOLO con este JSON (sin markdown, sin texto extra):
{{
  "has_signal": true/false,
  "strategies_detected": ["S1","S2","S3","S4","S5"],
  "primary_strategy": "S1|S2|S3|S4|S5",
  "catalyst_type": "tier1_contract|institutional_inv|gov_contract|tech_milestone|regulatory|earnings_beat|short_squeeze|options_flow|sector_rotation|rumor|none",
  "catalyst_summary": "1 línea concisa explicando exactamente el catalizador o el motivo del movimiento",
  "why_it_moves": "2-3 líneas explicando la mecánica: quién compra, por qué y qué fuerza el movimiento",
  "entry_note": "nivel o condición de entrada ideal (1 línea)",
  "stop_narrative": "qué evento concreto invalidaría completamente la tesis (1 línea)",
  "risk_level": "low|medium|high|extreme",
  "score": 0,
  "score_breakdown": {{
    "base_strategy_pts": 0,
    "base_strategy_reason": "qué estrategia base y por qué ese puntaje",
    "volume_pts": 0,
    "volume_reason": "volumen relativo detectado y pts asignados",
    "short_pts": 0,
    "short_reason": "short float % y pts asignados",
    "confluence_pts": 0,
    "confluence_reason": "estrategias que confluyen y pts extra",
    "sector_pts": {sector_pts},
    "sector_reason": "sector en momentum top-N y pts",
    "penalty_pts": 0,
    "penalty_reason": "penalizaciones aplicadas y motivo"
  }},
  "similar_case": "ASTS|RKLB|IONQ|LUNR|JOBY|GME|CAVA|TSLA|MRNA|AMC|NVDA|ENPH|none",
  "similar_case_reason": "1-2 líneas explicando POR QUÉ se parece a ese caso histórico: qué tienen en común en mecánica, sector, catalizador o estructura",
  "verdict": "SEÑAL FUERTE|WATCHLIST|DESCARTA"
}}

SCORING GUIDE — calcula cada componente por separado y suma en "score" (máximo 30):

BASE STRATEGY (pon en base_strategy_pts):
  - S1 contrato Tier-1 real con dinero comprometido (Fortune 500, DoD, NASA): +10
  - S1 inversión institucional directa: +10
  - S1 hito técnico demostrado / aprobación regulatoria: +8
  - S2 earnings beat >10% + guidance alcista: +7
  - S3 evidencia de call sweep OTM inusual: +8
  - S4 short float >30%: +8 | 20-30%: +6 | 15-20%: +4
  - S5 solo (sin otras señales): +3

VOLUMEN (pon en volume_pts):
  - Vol relativo >5x: +3 | >3x: +2 | >2x: +1 | <2x: +0

SHORT AMPLIFIER (pon en short_pts):
  - Short >20% con catalizador real confirmado: +2 | solo short sin catalizador: +0

CONFLUENCIA (pon en confluence_pts):
  - 3+ estrategias detectadas simultáneamente: +3
  - 2 estrategias detectadas: +2
  - Baja cobertura analista + small cap: +1

PENALIZACIONES (pon en penalty_pts, número negativo):
  - Solo rumor sin confirmar: -3
  - Historial de dilución documentado: -2
  - Sin revenue ni catalizador real: -2
  - Noticia de hace >7 días: -2

SIMILAR CASE — elige el más parecido:
  ASTS: pre-revenue + contrato Tier-1 + short squeeze + sector espacial
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
  none: no hay caso histórico claro comparable

SEÑAL FUERTE si score >= 18, WATCHLIST si 9-17, DESCARTA si < 9.
Si no hay señal relevante, pon has_signal: false y score < 9."""

    try:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip()
        raw = re.sub(r"```json|```", "", raw).strip()
        data = json.loads(raw)

        if not data.get("has_signal") or data.get("score", 0) < 9:
            return None

        # Actualizar verdict según nuevo umbral 18
        sc = data.get("score", 0)
        if sc >= 18:
            data["verdict"] = "SEÑAL FUERTE"
        elif sc >= 9:
            data["verdict"] = "WATCHLIST"
        else:
            data["verdict"] = "DESCARTA"

        # Enriquecer con datos del stock
        data.update({
            "ticker":       stock["ticker"],
            "company":      stock["company"],
            "sector":       stock["sector"],
            "price":        stock["price"],
            "change":       stock["change"],
            "mcap":         stock["mcap"],
            "short_float":  quote["short_float"],
            "rel_volume":   quote["rel_volume"],
            "news":         quote["news"][:3],
            "source":       stock["source"],
            "sector_pts":   sector_pts,
        })
        return data

    except json.JSONDecodeError as e:
        print(f"   ⚠ JSON parse error {stock['ticker']}: {e}")
        return None
    except Exception as e:
        print(f"   ⚠ AI error {stock['ticker']}: {e}")
        return None


# ══════════════════════════════════════════════════════════════
# EMAIL BUILDER
# ══════════════════════════════════════════════════════════════

STRAT_META = {
    "S1": {"emoji": "🚀", "name": "Pre-Revenue Catalyst",  "color": "#d4a843"},
    "S2": {"emoji": "📊", "name": "Post-Earnings Drift",   "color": "#22d48a"},
    "S3": {"emoji": "🏦", "name": "Options Flow",          "color": "#3898f8"},
    "S4": {"emoji": "💥", "name": "Short Squeeze",         "color": "#e83838"},
    "S5": {"emoji": "🌊", "name": "Sector Rotation",       "color": "#9272e0"},
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
    "rumor":             "💬",
    "none":              "❓",
}


def score_color(score: int) -> str:
    if score >= 20: return "#22d48a"
    if score >= 16: return "#d4a843"
    if score >= 12: return "#3898f8"
    return "#7090b0"


def risk_badge(risk: str) -> str:
    colors = {"low": "#22d48a", "medium": "#d4a843", "high": "#e87830", "extreme": "#e83838"}
    c = colors.get(risk, "#7090b0")
    return f'<span style="background:{c}20;color:{c};border:1px solid {c}44;padding:2px 7px;border-radius:2px;font-size:9px;letter-spacing:.1em;text-transform:uppercase;font-weight:700">{risk.upper()}</span>'


def strat_badge(s: str) -> str:
    m = STRAT_META.get(s, {"emoji": "•", "name": s, "color": "#7090b0"})
    return f'<span style="background:{m["color"]}15;color:{m["color"]};border:1px solid {m["color"]}33;padding:2px 8px;border-radius:2px;font-size:9px;letter-spacing:.1em;font-weight:700">{m["emoji"]} {m["name"]}</span>'


def format_signal_html(r: dict, rank: int = 0) -> str:
    strats    = r.get("strategies_detected", [r.get("primary_strategy", "S1")])
    primary   = r.get("primary_strategy", "S1")
    sc        = r.get("score", 0)
    cat_emoji = CATALYST_EMOJI.get(r.get("catalyst_type", "none"), "❓")
    similar   = r.get("similar_case", "none")
    similar_reason = r.get("similar_case_reason", "")
    verdict   = r.get("verdict", "WATCHLIST")
    is_strong = verdict == "SEÑAL FUERTE"
    border_col = score_color(sc)
    pm        = STRAT_META.get(primary, STRAT_META["S1"])

    strats_html = " ".join(strat_badge(s) for s in strats)

    # ── Rank badge ──
    rank_badge = ""
    if rank == 1:
        rank_badge = '<span style="background:#d4a84322;color:#d4a843;border:1px solid #d4a84355;padding:2px 10px;border-radius:2px;font-size:9px;letter-spacing:.15em;font-weight:700;margin-left:8px">🥇 TOP SEÑAL</span>'
    elif rank == 2:
        rank_badge = '<span style="background:#3898f822;color:#3898f8;border:1px solid #3898f855;padding:2px 10px;border-radius:2px;font-size:9px;letter-spacing:.15em;font-weight:700;margin-left:8px">🥈 #2</span>'
    elif rank == 3:
        rank_badge = '<span style="background:#22d48a22;color:#22d48a;border:1px solid #22d48a55;padding:2px 10px;border-radius:2px;font-size:9px;letter-spacing:.15em;font-weight:700;margin-left:8px">🥉 #3</span>'

    verdict_badge = (
        f'<span style="background:{"#22d48a" if is_strong else "#d4a843"}20;'
        f'color:{"#22d48a" if is_strong else "#d4a843"};'
        f'border:1px solid {"#22d48a" if is_strong else "#d4a843"}44;'
        f'padding:2px 10px;border-radius:2px;font-size:9px;letter-spacing:.12em;font-weight:700">'
        f'{"🔥 SEÑAL FUERTE" if is_strong else "👀 WATCHLIST"}</span>'
    )

    # ── Score breakdown table ──
    bd = r.get("score_breakdown", {})
    def bd_row(label: str, pts: int, reason: str, color: str = "") -> str:
        if pts == 0 and not reason:
            return ""
        pts_color = "#22d48a" if pts > 0 else ("#e83838" if pts < 0 else "#3a5070")
        if color:
            pts_color = color
        pts_str = f"+{pts}" if pts > 0 else str(pts)
        return (
            f'<tr>'
            f'<td style="padding:4px 10px;color:#7090b0;font-size:10px;white-space:nowrap;border-bottom:1px solid #131926">{label}</td>'
            f'<td style="padding:4px 10px;font-family:monospace;font-size:12px;font-weight:700;color:{pts_color};border-bottom:1px solid #131926;white-space:nowrap">{pts_str}</td>'
            f'<td style="padding:4px 10px;color:#5a7a9a;font-size:10px;border-bottom:1px solid #131926;line-height:1.5">{reason}</td>'
            f'</tr>'
        )

    breakdown_rows = "".join(filter(None, [
        bd_row("Base estrategia",  bd.get("base_strategy_pts", 0),  bd.get("base_strategy_reason", ""), "#d4a843"),
        bd_row("Volumen",          bd.get("volume_pts", 0),          bd.get("volume_reason", "")),
        bd_row("Short amplifier",  bd.get("short_pts", 0),           bd.get("short_reason", ""), "#e87830"),
        bd_row("Confluencia",      bd.get("confluence_pts", 0),      bd.get("confluence_reason", "")),
        bd_row("Sector momentum",  bd.get("sector_pts", 0),          bd.get("sector_reason", ""), "#9272e0"),
        bd_row("Penalizaciones",   bd.get("penalty_pts", 0),         bd.get("penalty_reason", ""), "#e83838"),
    ]))

    score_breakdown_html = f"""
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
          <td style="padding:6px 10px;font-family:monospace;font-size:16px;font-weight:900;color:{border_col}">{sc}/30</td>
          <td style="padding:6px 10px;font-size:10px;color:{border_col};font-weight:700">{verdict}</td>
        </tr>
      </table>
    </div>"""

    # ── Similar case block ──
    similar_html = ""
    if similar != "none":
        sim_color = "#9272e0"
        similar_html = f"""
    <div style="margin-top:14px;background:#13102a;border:1px solid #2a2050;border-left:3px solid {sim_color};border-radius:4px;padding:12px 14px">
      <div style="font-size:9px;letter-spacing:.16em;text-transform:uppercase;color:{sim_color};margin-bottom:6px">📚 Caso Histórico Similar</div>
      <div style="display:flex;align-items:baseline;gap:10px;margin-bottom:6px">
        <span style="font-family:monospace;font-size:18px;font-weight:900;color:#deeaf8">${similar}</span>
        <span style="font-size:10px;color:#7090b0">— patrón más parecido</span>
      </div>
      <div style="font-size:11px;color:#a090c8;line-height:1.65">{similar_reason}</div>
    </div>"""

    # ── News ──
    news_html = ""
    if r.get("news"):
        items = "".join(
            f'<div style="padding:4px 0;font-size:11px;color:#7090b0;border-bottom:1px solid #1a2030;line-height:1.5">▸ {h}</div>'
            for h in r["news"][:2]
        )
        news_html = f"""
    <div style="margin-top:14px">
      <div style="font-size:9px;letter-spacing:.14em;text-transform:uppercase;color:#3a5070;margin-bottom:6px">Noticias detectadas</div>
      {items}
    </div>"""

    return f"""
    <div style="border:1px solid #1e2d44;border-left:4px solid {border_col};border-radius:6px;
                padding:20px;margin-bottom:14px;background:#0d1525;font-family:'Segoe UI',Arial,sans-serif">

      <!-- Header -->
      <div style="display:flex;justify-content:space-between;align-items:flex-start;
                  margin-bottom:14px;flex-wrap:wrap;gap:8px">
        <div>
          <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:6px">
            <span style="font-family:monospace;font-size:22px;font-weight:900;color:#deeaf8">${r['ticker']}</span>
            <span style="color:#7090b0;font-size:13px">{r['company']}</span>
            {rank_badge}
          </div>
          <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:6px">{strats_html}</div>
          <div style="font-family:monospace;font-size:11px;color:#3a5070">
            {r['sector']} · {r['mcap']} · Short:
            <span style="color:#e87830">{r['short_float']}</span> · Vol:
            <span style="color:#d4a843">{r['rel_volume']}x</span>
          </div>
        </div>
        <div style="text-align:right">
          <div style="font-family:monospace;font-size:28px;font-weight:900;color:{border_col};line-height:1">{sc}</div>
          <div style="font-size:9px;color:#3a5070;letter-spacing:.1em">/30 PUNTOS</div>
          <div style="margin-top:6px">{verdict_badge}</div>
        </div>
      </div>

      <!-- Price row -->
      <div style="background:#131926;border-radius:4px;padding:8px 12px;margin-bottom:14px;
                  font-family:monospace;font-size:12px;color:#7090b0">
        💲 <span style="color:#deeaf8;font-weight:700">{r['price']}</span> &nbsp;|&nbsp;
        <span style="color:#22d48a;font-weight:700">{r['change']}</span> hoy &nbsp;|&nbsp;
        Escáner: <span style="color:#d4a843">{r['source']}</span>
      </div>

      <!-- Catalyst + Why -->
      <table style="width:100%;border-collapse:collapse;font-size:12px;margin-bottom:4px">
        <tr>
          <td style="padding:5px 0;color:#7090b0;width:120px;vertical-align:top">{cat_emoji} Catalizador</td>
          <td style="padding:5px 0;color:#deeaf8;font-weight:600;line-height:1.5">{r.get('catalyst_summary','')}</td>
        </tr>
        <tr>
          <td style="padding:5px 0;color:#7090b0;vertical-align:top">📈 Por qué sube</td>
          <td style="padding:5px 0;color:#b0c8e0;line-height:1.65">{r.get('why_it_moves','')}</td>
        </tr>
        <tr>
          <td style="padding:5px 0;color:#7090b0;vertical-align:top">🎯 Entrada</td>
          <td style="padding:5px 0;color:#22d48a;font-weight:600">{r.get('entry_note','')}</td>
        </tr>
        <tr>
          <td style="padding:5px 0;color:#7090b0;vertical-align:top">🛡 Stop</td>
          <td style="padding:5px 0;color:#e83838;font-weight:600">{r.get('stop_narrative','')}</td>
        </tr>
        <tr>
          <td style="padding:5px 0;color:#7090b0">⚠️ Riesgo</td>
          <td style="padding:5px 0">{risk_badge(r.get('risk_level','medium'))}</td>
        </tr>
      </table>

      <!-- Score breakdown -->
      {score_breakdown_html}

      <!-- Similar case -->
      {similar_html}

      <!-- News -->
      {news_html}

      <!-- Links -->
      <div style="margin-top:14px;padding-top:10px;border-top:1px solid #1e2d44">
        <a href="https://finviz.com/quote.ashx?t={r['ticker']}" style="color:#3898f8;font-size:11px;text-decoration:none">🔗 Finviz →</a>
        &nbsp;&nbsp;
        <a href="https://finance.yahoo.com/quote/{r['ticker']}" style="color:#3898f8;font-size:11px;text-decoration:none">📊 Yahoo Finance →</a>
        &nbsp;&nbsp;
        <a href="https://unusualwhales.com/stock/{r['ticker']}" style="color:#3898f8;font-size:11px;text-decoration:none">🦈 Unusual Whales →</a>
      </div>
    </div>"""


def build_email(
    results: list[dict],
    total_scanned: int,
    sector_perf: dict[str, float],
    today: str
) -> tuple[str, str]:
    """Construye el email completo. Devuelve (subject, html)."""

    strong  = [r for r in results if r.get("verdict") == "SEÑAL FUERTE"]   # score >= 18
    watch   = [r for r in results if r.get("verdict") == "WATCHLIST"]       # score 9-17
    top3    = results[:3]

    # ── Subject ──
    if strong:
        tickers = " ".join(f"${r['ticker']}" for r in strong[:3])
        subject = f"🔥 [{len(strong)} señal{'es' if len(strong)>1 else ''}] {tickers} — The System {today}"
    elif watch:
        tickers = " ".join(f"${r['ticker']}" for r in watch[:2])
        subject = f"👀 [Watchlist] {tickers} — The System {today}"
    else:
        subject = f"😴 Sin señales hoy — The System {today}"

    # ── Top 3 podium ──
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
              <div style="font-size:9px;color:#3a5070;letter-spacing:.1em">/30 PUNTOS</div>
              <div style="margin-top:8px;font-size:11px;color:{score_color(r['score'])};font-weight:700">{pm['emoji']} {pm['name']}</div>
            </div>"""
        top3_html = f"""
        <div style="margin-bottom:24px">
          <div style="font-family:monospace;font-size:9px;letter-spacing:.2em;text-transform:uppercase;color:#d4a843;margin-bottom:12px">⭐ Top 3 Señales del Día</div>
          <div style="display:flex;gap:12px;flex-wrap:wrap">{cards}</div>
        </div>"""

    # ── Sector momentum strip ──
    top_sectors  = list(sector_perf.items())[:5]
    sector_strip = ""
    for i, (etf, pct) in enumerate(top_sectors):
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

    # ── Strong signals ──
    strong_html = ""
    if strong:
        cards = "".join(format_signal_html(r, i+1 if r in top3 else 0) for i, r in enumerate(strong))
        strong_html = f"""
        <div style="margin-bottom:8px">
          <div style="font-family:monospace;font-size:10px;letter-spacing:.2em;text-transform:uppercase;color:#22d48a;margin-bottom:12px">🔥 Señales Fuertes ({len(strong)})</div>
          {cards}
        </div>"""

    # ── Watchlist ──
    watch_html = ""
    if watch:
        cards = "".join(format_signal_html(r, i+1+len(strong) if r in top3 else 0) for i, r in enumerate(watch[:6]))
        watch_html = f"""
        <div style="margin-bottom:8px">
          <div style="font-family:monospace;font-size:10px;letter-spacing:.2em;text-transform:uppercase;color:#d4a843;margin-bottom:12px">👀 Watchlist ({len(watch)})</div>
          {cards}
        </div>"""

    # ── Empty ──
    empty_html = ""
    if not strong and not watch:
        empty_html = """
        <div style="text-align:center;padding:48px;background:#0d1525;border:1px solid #1e2d44;border-radius:6px;margin-bottom:24px">
          <div style="font-size:40px;margin-bottom:14px">😴</div>
          <div style="font-family:monospace;font-size:16px;color:#deeaf8;font-weight:700;margin-bottom:8px">Sin señales relevantes hoy</div>
          <div style="font-size:13px;color:#7090b0;line-height:1.7">Ninguna acción combinó los criterios necesarios de las 5 estrategias.<br>Mañana puede ser diferente. El sistema sigue monitoreando.</div>
        </div>"""

    # ── Full HTML ──
    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#040507;font-family:'Segoe UI',Arial,sans-serif">
<div style="max-width:700px;margin:0 auto;padding:20px 16px 40px">

  <!-- HEADER -->
  <div style="background:#0d1525;border:1px solid #1e2d44;border-radius:6px;padding:24px 28px;margin-bottom:16px">
    <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:12px">
      <div>
        <div style="font-family:monospace;font-size:11px;letter-spacing:.25em;text-transform:uppercase;color:#d4a843;margin-bottom:8px">THE SYSTEM · ESCÁNER DIARIO</div>
        <div style="font-size:22px;font-weight:900;color:#f0f6ff;margin-bottom:4px">5 Estrategias · {today}</div>
        <div style="font-size:12px;color:#7090b0;font-style:italic">S1 Catalyst · S2 PEAD · S3 Options Flow · S4 Short Squeeze · S5 Sector Rotation</div>
      </div>
      <div style="text-align:right">
        <div style="font-size:9px;letter-spacing:.14em;text-transform:uppercase;color:#7090b0">Escaneadas</div>
        <div style="font-family:monospace;font-size:36px;font-weight:900;color:#3898f8;line-height:1">{total_scanned}</div>
      </div>
    </div>

    <!-- Stats row -->
    <div style="display:flex;gap:10px;margin-top:18px;flex-wrap:wrap">
      <div style="flex:1;background:#131926;border-radius:4px;padding:10px;text-align:center;min-width:100px">
        <div style="font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#7090b0;margin-bottom:4px">Señales fuertes</div>
        <div style="font-family:monospace;font-size:24px;font-weight:900;color:#22d48a">{len(strong)}</div>
      </div>
      <div style="flex:1;background:#131926;border-radius:4px;padding:10px;text-align:center;min-width:100px">
        <div style="font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#7090b0;margin-bottom:4px">Watchlist</div>
        <div style="font-family:monospace;font-size:24px;font-weight:900;color:#d4a843">{len(watch)}</div>
      </div>
      <div style="flex:2;background:#131926;border-radius:4px;padding:10px;min-width:200px">
        <div style="font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#7090b0;margin-bottom:6px">Filtros activos</div>
        <div style="font-size:10px;color:#3a5070;line-height:1.7">
          S1: Vol &gt;2x + Subida &gt;3% + Noticias catalizador<br>
          S2: Earnings beat &lt;7 días + Drift confirmado<br>
          S3: Short Float &gt;15% + Movimiento fuerte<br>
          S4: Short Float &gt;20% + Catalizador<br>
          🔥 Señal fuerte: score ≥18 · 👀 Watchlist: 9-17
        </div>
      </div>
    </div>
  </div>

  <!-- SECTOR MOMENTUM -->
  <div style="background:#0d1525;border:1px solid #1e2d44;border-radius:6px;padding:18px 20px;margin-bottom:16px">
    <div style="font-family:monospace;font-size:9px;letter-spacing:.2em;text-transform:uppercase;color:#9272e0;margin-bottom:12px">🌊 S5 · Momentum Sectorial (3 meses) — Contexto del día</div>
    <div style="display:flex;gap:8px;flex-wrap:wrap">{sector_strip}</div>
    <div style="font-size:10px;color:#3a5070;margin-top:10px;line-height:1.6">Los sectores marcados ACTIVO son el universo prioritario esta semana. Las señales S1-S4 en estos sectores reciben puntuación extra.</div>
  </div>

  <!-- TOP 3 PODIUM -->
  {top3_html}

  <!-- SIGNALS -->
  {strong_html}
  {watch_html}
  {empty_html}

  <!-- TIP -->
  {'<div style="background:#0d1525;border:1px solid #1e2d44;border-left:3px solid #3898f8;border-radius:4px;padding:14px 18px;margin-top:8px;font-size:11px;color:#7090b0;line-height:1.7"><strong style="color:#b0c8e0">💡 Recuerda:</strong> El stop siempre es narrativo — si el catalizador se invalida, sales. Máx 2% del capital por trade. Verifica la noticia original antes de actuar.</div>' if strong or watch else ''}

  <!-- FOOTER -->
  <div style="margin-top:24px;text-align:center;font-size:10px;color:#253550;letter-spacing:.06em;text-transform:uppercase;line-height:1.8">
    The System · 5 Estrategias · Escáner Automático Diario<br>
    Solo para fines informativos · No es asesoramiento financiero
  </div>
</div>
</body>
</html>"""

    return subject, html


# ══════════════════════════════════════════════════════════════
# EMAIL SENDER
# ══════════════════════════════════════════════════════════════

def send_email(subject: str, html: str):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = EMAIL_FROM
    msg["To"]      = EMAIL_TO
    msg.attach(MIMEText(html, "html"))
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_FROM, EMAIL_APP_PASSWORD)
        server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def main():
    today = datetime.datetime.now().strftime("%d/%m/%Y")
    print("═" * 60)
    print(f"  THE SYSTEM — ESCÁNER COMBINADO 5 ESTRATEGIAS")
    print(f"  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print("═" * 60)

    # ── PASO 1: Sector Rotation (S5) ──
    sector_perf = get_sector_momentum()

    # ── PASO 2: Scrape screeners ──
    print("\n📡 Scraping screeners Finviz...")
    all_stocks = []
    all_stocks += scrape_finviz_screen(SCREEN_VOLUME,   "S1/S4 Volume")
    time.sleep(2)
    all_stocks += scrape_finviz_screen(SCREEN_SHORT,    "S4 Short")
    time.sleep(2)
    all_stocks += scrape_finviz_screen(SCREEN_EARNINGS, "S2 Earnings")

    # Deduplicar
    stocks = deduplicate(all_stocks)
    total_scanned = len(stocks)
    print(f"\n   Total tras deduplicación: {total_scanned} acciones únicas\n")

    if not stocks:
        send_email(
            "⚠️ The System — Sin datos hoy",
            "<div style='padding:24px;background:#040507;color:#b0c8e0;font-family:sans-serif'>"
            "<h2 style='color:#d4a843'>⚠️ Sin datos de Finviz</h2>"
            "<p>No se obtuvieron acciones hoy. Posible día festivo USA o error temporal.</p></div>"
        )
        return

    # ── PASO 3: Analizar con IA ──
    stocks_to_analyze = stocks[:35]  # máx 35 para controlar coste
    print(f"🤖 Analizando {len(stocks_to_analyze)} acciones con Claude Haiku...\n")

    results = []
    for i, stock in enumerate(stocks_to_analyze):
        ticker = stock["ticker"]
        print(f"   [{i+1:02d}/{len(stocks_to_analyze)}] {ticker:<6}", end=" ")

        # Datos del quote
        quote = get_quote_data(ticker)

        # Puntos de sector
        s5_pts = sector_momentum_score(stock["sector"], sector_perf)

        # Análisis IA
        result = analyze_with_ai(stock, quote, s5_pts)

        if result:
            results.append(result)
            print(f"✅ {result['verdict']} · {result['score']}/30 · {result.get('primary_strategy','?')}")
        else:
            print("– sin señal")

        time.sleep(1.2)  # rate limit Finviz

    # ── PASO 4: Ordenar por score ──
    results.sort(key=lambda x: x.get("score", 0), reverse=True)

    n_strong = len([r for r in results if r.get("verdict") == "SEÑAL FUERTE"])
    n_watch  = len([r for r in results if r.get("verdict") == "WATCHLIST"])

    # ── PASO 5: Email ──
    print(f"\n📤 Construyendo email → {n_strong} fuertes, {n_watch} watchlist")
    subject, html = build_email(results, total_scanned, sector_perf, today)
    send_email(subject, html)

    print(f"\n✅ Email enviado a {EMAIL_TO}")
    print(f"   Escaneadas: {total_scanned} | Señales: {n_strong} fuertes · {n_watch} watchlist")
    print(f"   Top señal: ${results[0]['ticker']} {results[0]['score']}/30" if results else "   Sin señales")


if __name__ == "__main__":
    main()

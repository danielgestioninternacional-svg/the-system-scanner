# 📡 THE SYSTEM — Escáner Combinado 5 Estrategias

Escáner automático diario que combina las 5 estrategias simultáneamente:

| # | Estrategia | Señal que detecta |
|---|-----------|-------------------|
| S1 | Pre-Revenue Catalyst | Volumen fuerte + noticias de contrato/hito |
| S2 | Post-Earnings Drift | Earnings beat reciente + drift confirmado |
| S3 | Options Flow | Short float alto + movimiento inusual |
| S4 | Short Squeeze | Short interest >20% + catalizador |
| S5 | Sector Rotation | Contexto sectorial (puntos extra) |

**Cada día a las 22:30h España** recibes un email con:
- 🥇 Top 3 señales del día destacadas (pódium)
- 🔥 Señales fuertes (score ≥16/30)
- 👀 Watchlist (score 9-15/30)
- 🌊 Momentum sectorial actualizado
- Por cada señal: catalizador, entrada, stop narrativo, caso histórico similar

---

## Setup — 15 minutos

### 1. Gmail App Password

1. `myaccount.google.com` → Seguridad → Verificación en 2 pasos (actívala)
2. Seguridad → **Contraseñas de aplicaciones**
3. App: `Correo` · Dispositivo: `Otro` → escribe `TheSystem`
4. Guarda las **16 letras** que aparecen (solo se muestran una vez)

### 2. Anthropic API Key

1. `console.anthropic.com` → API Keys → Create Key
2. Copia `sk-ant-api03-...`
3. Coste estimado: **$1–3/mes** (Claude Haiku analiza ~35 acciones/día)

### 3. Crear repositorio GitHub

1. `github.com` → New Repository → nombre: `the-system-scanner` → Public → Create
2. Sube estos archivos manteniendo la estructura exacta:
   ```
   src/scanner.py
   requirements.txt
   .github/workflows/daily_scan.yml
   ```

### 4. Configurar 4 Secrets

`Settings → Secrets and variables → Actions → New repository secret`

| Secret | Valor |
|--------|-------|
| `EMAIL_FROM` | tu@gmail.com |
| `EMAIL_TO` | donde recibes el email (puede ser el mismo) |
| `EMAIL_APP_PASSWORD` | las 16 letras del paso 1 |
| `ANTHROPIC_API_KEY` | sk-ant-api03-... |

### 5. Primer test

`Actions → The System — Escáner 5 Estrategias → Run workflow → Run workflow`

En ~8–10 minutos llega el primer email. ✅

---

## Ajustes

**Cambiar hora de envío** → edita `cron` en `daily_scan.yml`
- `"30 21 * * 1-5"` = 22:30h España invierno (nov–mar)
- `"30 20 * * 1-5"` = 22:30h España verano (mar–oct)

**Analizar más acciones** → cambia `stocks[:35]` a `stocks[:50]` en `scanner.py`

**Cambiar score mínimo watchlist** → cambia `score < 9` a `score < 12` para menos ruido

**Añadir un segundo email** → duplica el `send_email()` al final de `main()` con otro EMAIL_TO

---

## Troubleshooting

| Problema | Causa probable | Solución |
|---------|---------------|---------|
| No llega email | App Password incorrecta | Crea una nueva App Password |
| Actions en rojo | Ver logs → clic en el run fallido | El error estará en los logs |
| Pocas señales | Mercado tranquilo o día festivo | Normal, el sistema envía "Sin señales" |
| Demasiadas señales | Score mínimo bajo | Sube el umbral de 9 a 12 |
| Error Finviz | Finviz cambió HTML | Abrir issue en el repo |

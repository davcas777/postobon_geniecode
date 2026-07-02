---
customer: Postobon
type: workshop
status: active
created: 2026-07-01
owner: David Cascante
audience: equipos de datos y analítica de Postobón (Daniel Carvajal / Diego Vásquez teams)
duration: ~3.5 hours
delivery_week: 2026-07-02
---

# Genie Code Workshop — Postobón

Workshop práctico de **Genie Code dentro de Databricks**, re-skineado y re-domiciliado por completo para **Postobón**, con datos sintéticos de **ventas y distribución de bebidas**. Se publicó en un repo dedicado para la entrega.

- Repo destino: https://github.com/davcas777/postobon_geniecode
- Fecha del workshop: semana del 2026-07-02

## Objetivo
Enseñar a los equipos de datos y analítica de Postobón cómo usar Genie Code para acelerar el trabajo diario en cuatro tracks — Data Engineering, BI & Analytics, Data Science & ML, y Data Governance — usando datos sintéticos del dominio de bebidas/CPG.

## Contexto
- [[../../profile.md|Perfil de Postobón]]
- [[../../CLAUDE.md|Contexto Claude de Postobón]]
- [[../../knowledge/use-cases|UCOs activos]]

## Tablas del dominio (`workshop.gold`)
Todas se generan con `generate_workshop_data.py` (~360 defectos de calidad plantados) y usan **pesos colombianos (COP)** en las columnas monetarias:

| Tabla | Descripción |
|-------|-------------|
| `dim_distribution_centers` | Dimensión de centros de distribución (Planta / CD Regional / Cross-dock / Mega-CD) por región y departamento. |
| `fact_daily_sales` | Hechos diarios de ventas por CD: órdenes, unidades, `revenue_cop`, `cogs_cop`, fill rate, devoluciones. |
| `fact_pos_activity` | Actividad por punto de venta (~200k filas): canal, marca, categoría, unidades, `ticket_value_cop`, promo. |
| `fact_trade_promos` | Promociones de trade marketing (Combo Verano, 2x1 Hit, Rebate Mayorista, etc.) con tipo/valor de descuento y `budget_cop`. |
| `fact_daily_kpis` | KPIs agregados por región × fecha: revenue, unidades, ticket, fill rate, OSA, PDV activos, YoY growth. |

## Branding (Postobón = rojo + azul)
- Paleta: rojo Postobón `#E4032E`, rojo oscuro `#B00020`, rojo suave `#FFE3E7`, azul Postobón `#0057B8`, casi-negro `#1A1A1A`.
- Variables CSS renombradas a `--po-*`; logo wordmark propio `frontend/img/postobon-logo.svg`.
- Colores de track: `data-engineering` rojo, `bi-analytics` azul, `data-science` púrpura, `data-governance` cian.
- `track-engineering.svg` recoloreado a rojo, `track-bi.svg` a azul; hero-art reemplazado por una silueta abstracta de botella.

## Mapa de archivos
| Archivo | Propósito |
|------|---------|
| `generate_workshop_data.py` | Notebook Databricks que crea las 5 tablas de bebidas en `workshop.gold` con ~360 defectos de calidad. Ejecutar antes del workshop. |
| `data/tracks.json` | Contenido de los tracks (DE, BI, DS, Governance × 8 pasos cada uno). Todo el copy está localizado a ventas/distribución de Postobón. |
| `frontend/index.html` | SPA con marca Postobón (DM Sans, rojo + azul) que lee de `/api/tracks`. |
| `frontend/img/*.svg` | Íconos SVG de los tracks + logotipo Postobón + símbolo Databricks. |
| `main.py` | Backend FastAPI (3 endpoints + mount estático). |
| `app.yaml`, `requirements.txt` | Configuración para Databricks Apps. |

## Notas de diseño
- **Dominio**: ventas y distribución de bebidas. 6 regiones comerciales de Colombia (Andina, Caribe, Pacífica, Oriental, Eje Cafetero, Centro).
- **Moneda**: pesos colombianos (COP) en todas las columnas monetarias (`_cop`).
- **Contexto de migración**: reportes de **Cognos + DB2** y flujos de **Alteryx/SAP BO** hacia Databricks/Lakeflow.
- **DS track**: forecasting de demanda por CD + visión aplicada a **reconocimiento de nevera/anaquel en punto de venta** (agotados y share of shelf).
- **Marca**: rojo + azul Postobón. Paths MLflow `/postobon/`, endpoints `postobon-*`, grupos `postobon_*`.
- **Catálogo/schema**: `workshop.gold` (todos los prompts lo referencian).

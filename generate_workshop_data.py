# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Data Generator — Postobón Genie Code Workshop
# MAGIC
# MAGIC Genera 5 tablas sintéticas del dominio de bebidas / distribución de Postobón en `workshop.gold`.
# MAGIC Incluye problemas de calidad de datos intencionales para el track de Governance.
# MAGIC
# MAGIC **Idempotente** — seguro de re-ejecutar. Usa `CREATE OR REPLACE TABLE` / `saveAsTable` con `mode("overwrite")`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "workshop"
SCHEMA = "gold"

# Regiones comerciales de Postobón en Colombia y sus departamentos.
REGIONS = {
    "Andina": {"dcs": 14, "places": [
        ("Antioquia", "Medellín"), ("Antioquia", "Bello"), ("Antioquia", "Rionegro"),
        ("Cundinamarca", "Bogotá"), ("Cundinamarca", "Soacha"), ("Cundinamarca", "Facatativá"),
        ("Boyacá", "Tunja"), ("Boyacá", "Duitama"),
    ]},
    "Caribe": {"dcs": 9, "places": [
        ("Atlántico", "Barranquilla"), ("Atlántico", "Soledad"),
        ("Bolívar", "Cartagena"), ("Bolívar", "Magangué"),
        ("Magdalena", "Santa Marta"), ("Córdoba", "Montería"),
        ("Cesar", "Valledupar"), ("La Guajira", "Riohacha"),
    ]},
    "Pacífica": {"dcs": 8, "places": [
        ("Valle del Cauca", "Cali"), ("Valle del Cauca", "Palmira"),
        ("Valle del Cauca", "Buga"), ("Valle del Cauca", "Buenaventura"),
        ("Cauca", "Popayán"), ("Nariño", "Pasto"),
        ("Nariño", "Ipiales"), ("Chocó", "Quibdó"),
    ]},
    "Oriental": {"dcs": 8, "places": [
        ("Santander", "Bucaramanga"), ("Santander", "Floridablanca"),
        ("Santander", "Barrancabermeja"), ("Norte de Santander", "Cúcuta"),
        ("Meta", "Villavicencio"), ("Meta", "Acacías"),
        ("Casanare", "Yopal"), ("Arauca", "Arauca"),
    ]},
    "Eje Cafetero": {"dcs": 6, "places": [
        ("Risaralda", "Pereira"), ("Risaralda", "Dosquebradas"),
        ("Caldas", "Manizales"), ("Caldas", "La Dorada"),
        ("Quindío", "Armenia"), ("Quindío", "Calarcá"),
    ]},
    "Centro": {"dcs": 5, "places": [
        ("Tolima", "Ibagué"), ("Tolima", "Espinal"),
        ("Huila", "Neiva"), ("Huila", "Pitalito"),
        ("Caquetá", "Florencia"),
    ]},
}

DC_TYPES = ["Planta", "CD Regional", "Cross-dock", "Mega-CD"]
CHANNELS = ["Tienda de Barrio", "Supermercado", "Mayorista", "Autoservicio", "HORECA"]
BRANDS = ["Postobón", "Colombiana", "Hit", "Manzana", "Bretaña", "Mr Tea", "Pony Malta", "Agua Cristal", "H2O"]
CATEGORIES = ["Gaseosa", "Jugo", "Té", "Malta", "Agua", "Energizante"]
BRAND_CATEGORY = {
    "Postobón": "Gaseosa", "Colombiana": "Gaseosa", "Hit": "Jugo", "Manzana": "Gaseosa",
    "Bretaña": "Gaseosa", "Mr Tea": "Té", "Pony Malta": "Malta", "Agua Cristal": "Agua", "H2O": "Agua",
}
DISCOUNT_TYPES = ["percentage", "fixed_amount", "bonus_units", "rebate"]

# Prefijos cortos por región para IDs (DC-AND-0001, POS-AND-0001234, etc.).
REGION_PREFIX = {
    "Andina": "AND", "Caribe": "CAR", "Pacífica": "PAC",
    "Oriental": "ORI", "Eje Cafetero": "EJE", "Centro": "CEN",
}

print(f"Target: {CATALOG}.{SCHEMA}")
print(f"Total distribution centers: {sum(r['dcs'] for r in REGIONS.values())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog & Schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"✅ {CATALOG}.{SCHEMA} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 1: dim_distribution_centers

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import date, timedelta

np.random.seed(42)

# Rangos aproximados de lat/lon por región (Colombia continental).
LAT_RANGES = {"Andina": (4.0, 7.5), "Caribe": (8.0, 11.5), "Pacífica": (0.8, 4.5),
              "Oriental": (5.5, 8.5), "Eje Cafetero": (4.3, 5.3), "Centro": (1.5, 4.5)}
LON_RANGES = {"Andina": (-75.8, -73.0), "Caribe": (-75.6, -72.5), "Pacífica": (-77.8, -76.2),
              "Oriental": (-73.8, -70.7), "Eje Cafetero": (-75.9, -75.3), "Centro": (-76.0, -74.7)}

rows = []
dc_seq = 0

for region, info in REGIONS.items():
    prefix = REGION_PREFIX[region]
    for i in range(info["dcs"]):
        dept, city = info["places"][i % len(info["places"])]
        dc_seq += 1
        dc_id = f"DC-{prefix}-{dc_seq:04d}"
        dc_code = f"{prefix}{dc_seq:03d}"
        dtype = np.random.choice(DC_TYPES, p=[0.20, 0.45, 0.25, 0.10])
        opened = date(1995, 1, 1) + timedelta(days=int(np.random.uniform(0, 365 * 28)))
        lat = round(np.random.uniform(*LAT_RANGES[region]), 6)
        lon = round(np.random.uniform(*LON_RANGES[region]), 6)
        is_24h = np.random.random() < 0.45
        dock_count = int(np.random.choice([4, 6, 8, 12, 18, 30, 48], p=[0.15, 0.25, 0.25, 0.15, 0.10, 0.07, 0.03]))
        monthly_capacity_units = int(dock_count * np.random.uniform(80000, 220000))

        rows.append({
            "dc_id": dc_id,
            "dc_code": dc_code,
            "dc_name": f"CD {city}",
            "dc_type": dtype,
            "region": region,
            "department": dept,
            "city": city,
            "opened_date": opened,
            "latitude": lat,
            "longitude": lon,
            "is_24h": is_24h,
            "dock_count": dock_count,
            "monthly_capacity_units": monthly_capacity_units,
        })

df_dcs = pd.DataFrame(rows)

# ── Inject DQ issues ──
# 15 rows con region NULL
null_idx = np.random.choice(len(df_dcs), 15, replace=False)
df_dcs.loc[null_idx, "region"] = None

# 8 rows con opened_date en el futuro
future_idx = np.random.choice(len(df_dcs), 8, replace=False)
df_dcs.loc[future_idx, "opened_date"] = pd.Timestamp("2027-09-30").date()

# 12 rows con coordenadas (0,0)
zero_idx = np.random.choice(len(df_dcs), 12, replace=False)
df_dcs.loc[zero_idx, ["latitude", "longitude"]] = 0.0

# 5 dc_id duplicados
dup_idx = np.random.choice(len(df_dcs), 5, replace=False)
for idx in dup_idx:
    source_idx = np.random.choice([i for i in range(len(df_dcs)) if i != idx])
    df_dcs.loc[idx, "dc_id"] = df_dcs.loc[source_idx, "dc_id"]

# 20 rows con casing inconsistente en dc_type
case_idx = np.random.choice(len(df_dcs), 20, replace=False)
df_dcs.loc[case_idx, "dc_type"] = df_dcs.loc[case_idx, "dc_type"].str.lower()

# 8 dock_count outliers (0 / negativos / absurdos)
outlier_idx = np.random.choice(len(df_dcs), 8, replace=False)
outlier_vals = [0, 0, -3, 250, 320, 999, 1500, -7]
for i, idx in enumerate(outlier_idx):
    df_dcs.loc[idx, "dock_count"] = outlier_vals[i]

print(f"dim_distribution_centers: {len(df_dcs)} rows")
print(f"  DQ issues: 15 null region, 8 future dates, 12 zero coords, 5 dup IDs, 20 bad casing, 8 dock outliers")

sdf = spark.createDataFrame(df_dcs)
sdf.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.dim_distribution_centers")
print(f"✅ {CATALOG}.{SCHEMA}.dim_distribution_centers written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 2: fact_daily_sales

# COMMAND ----------

# dc_id válidos para generación de FK (descartar duplicados / nulls primero).
valid_dcs = df_dcs.dropna(subset=["region"])["dc_id"].unique().tolist()
dc_region = df_dcs.set_index("dc_id")["region"].to_dict()

date_range = pd.date_range("2024-01-01", "2026-04-13", freq="D")
all_sales = []

# Muestrear ~70% de los CD para mantener el volumen manejable en clusters de workshop.
sample_dcs = valid_dcs[:int(len(valid_dcs) * 0.7)]

for dc_id in sample_dcs:
    region = dc_region.get(dc_id, "Andina")
    base_orders = {"Andina": 220, "Caribe": 160, "Pacífica": 150, "Oriental": 120,
                   "Eje Cafetero": 110, "Centro": 90}.get(region, 120)
    base_unit_price = np.random.uniform(1800, 2600)  # precio promedio por unidad en COP

    for d in date_range:
        dow_mult = {0: 1.05, 1: 1.02, 2: 1.00, 3: 1.03, 4: 1.12, 5: 1.18, 6: 0.80}[d.dayofweek]
        # Bebidas: pico en meses calientes (Dic-Mar) y en diciembre.
        month_mult = {1: 1.15, 2: 1.10, 3: 1.08, 4: 0.98, 5: 0.95, 6: 0.92,
                      7: 0.94, 8: 0.96, 9: 0.97, 10: 1.00, 11: 1.05, 12: 1.28}[d.month]
        years_from_start = (d - pd.Timestamp("2024-01-01")).days / 365
        growth = 1 + 0.05 * years_from_start  # ~5% crecimiento YoY
        noise = np.random.normal(1.0, 0.10)

        orders_delivered = max(int(base_orders * dow_mult * month_mult * growth * noise), 1)
        orders_cancelled = int(orders_delivered * np.random.uniform(0, 0.06))
        units_per_order = int(np.random.uniform(60, 140))
        units_sold = orders_delivered * units_per_order

        unit_price = base_unit_price * month_mult * np.random.normal(1.0, 0.08)
        revenue_cop = round(units_sold * unit_price, 0)
        discount_cop = round(revenue_cop * np.random.uniform(0.02, 0.12), 0)
        cogs_cop = round(revenue_cop * np.random.uniform(0.55, 0.70), 0)
        fill_rate_pct = round(np.clip(np.random.normal(0.94, 0.04), 0.70, 1.0), 3)
        returns_units = int(units_sold * np.random.uniform(0, 0.02))

        all_sales.append({
            "sale_date": d.date(),
            "dc_id": dc_id,
            "orders_delivered": orders_delivered,
            "orders_cancelled": orders_cancelled,
            "units_sold": units_sold,
            "revenue_cop": revenue_cop,
            "discount_cop": discount_cop,
            "cogs_cop": cogs_cop,
            "fill_rate_pct": fill_rate_pct,
            "returns_units": returns_units,
        })

df_sales = pd.DataFrame(all_sales)

# ── Inject DQ issues ──
# 200 rows: revenue_cop = 0 pero units_sold > 0
zero_rev_idx = np.random.choice(len(df_sales), 200, replace=False)
df_sales.loc[zero_rev_idx, "revenue_cop"] = 0.0

# 50 rows: precio unitario negativo (revenue_cop negativo)
neg_price_idx = np.random.choice(len(df_sales), 50, replace=False)
df_sales.loc[neg_price_idx, "revenue_cop"] = -1000.0

# 30 rows: sale_date NULL
null_date_idx = np.random.choice(len(df_sales), 30, replace=False)
df_sales.loc[null_date_idx, "sale_date"] = None

# 40 rows: fill_rate_pct fuera de [0,1]
bad_fill_idx = np.random.choice(len(df_sales), 40, replace=False)
df_sales.loc[bad_fill_idx, "fill_rate_pct"] = df_sales.loc[bad_fill_idx, "fill_rate_pct"] + 0.25

print(f"fact_daily_sales: {len(df_sales)} rows")
print(f"  DQ issues: 200 zero revenue, 50 negative unit price, 30 null dates, 40 bad fill_rate_pct")

sdf_sales = spark.createDataFrame(df_sales)
sdf_sales.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_daily_sales")
print(f"✅ {CATALOG}.{SCHEMA}.fact_daily_sales written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 3: fact_pos_activity

# COMMAND ----------

pos_rows = []
n_pos = 8000
activity_dates = pd.date_range("2025-10-01", "2026-04-13", freq="D")

for _ in range(200000):
    dc_id = np.random.choice(sample_dcs)
    region = dc_region.get(dc_id, "Andina")
    pos_id = f"POS-{REGION_PREFIX[region]}-{np.random.randint(1, n_pos):07d}"
    ad = pd.Timestamp(np.random.choice(activity_dates)).date()
    channel = np.random.choice(CHANNELS, p=[0.45, 0.20, 0.15, 0.12, 0.08])
    brand = np.random.choice(BRANDS, p=[0.22, 0.14, 0.16, 0.10, 0.06, 0.08, 0.10, 0.08, 0.06])
    category = BRAND_CATEGORY[brand] if np.random.random() < 0.9 else np.random.choice(CATEGORIES)
    units_purchased = int(np.random.uniform(6, 240))
    ticket_value_cop = round(units_purchased * np.random.uniform(1800, 3200), 0)
    promo_applied = np.random.random() < 0.28
    sku_count = int(np.random.uniform(1, 12))

    pos_rows.append({
        "activity_date": ad,
        "pos_id": pos_id,
        "dc_id": dc_id,
        "region": region,
        "channel": channel,
        "brand": brand,
        "category": category,
        "units_purchased": units_purchased,
        "ticket_value_cop": ticket_value_cop,
        "promo_applied": promo_applied,
        "sku_count": sku_count,
    })

df_pos = pd.DataFrame(pos_rows)

# ── Inject DQ issues ──
# 100 rows: brand en minúsculas
brand_case_idx = np.random.choice(len(df_pos), 100, replace=False)
df_pos.loc[brand_case_idx, "brand"] = df_pos.loc[brand_case_idx, "brand"].str.lower()

# 40 rows: units_purchased negativo
neg_units_idx = np.random.choice(len(df_pos), 40, replace=False)
df_pos.loc[neg_units_idx, "units_purchased"] = -50

# 25 rows: región huérfana "XX" (+ pos_id POS-XX-0000000)
orphan_idx = np.random.choice(len(df_pos), 25, replace=False)
df_pos.loc[orphan_idx, "region"] = "XX"
df_pos.loc[orphan_idx, "pos_id"] = "POS-XX-0000000"

# 80 rows: dc_id que no existe en la dimensión (ruptura de integridad referencial)
fake_dcs = [f"DC-ZZ-9{i:03d}" for i in range(80)]
ref_break_idx = np.random.choice(len(df_pos), 80, replace=False)
for i, idx in enumerate(ref_break_idx):
    df_pos.loc[idx, "dc_id"] = fake_dcs[i]

print(f"fact_pos_activity: {len(df_pos)} rows")
print(f"  DQ issues: 100 lowercase brands, 40 negative units, 25 orphan XX, 80 ref integrity breaks")

sdf_pos = spark.createDataFrame(df_pos)
sdf_pos.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_pos_activity")
print(f"✅ {CATALOG}.{SCHEMA}.fact_pos_activity written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 4: fact_trade_promos

# COMMAND ----------

promo_rows = []
promo_names = [
    "Combo Verano Postobón", "2x1 Hit", "Descuento Tienda Colombiana", "Rebate Mayorista",
    "Pony Malta Escolar", "Black Friday Bebidas", "Manzana Fin de Semana", "Mr Tea Refréscate",
    "Agua Cristal Familiar", "Bretaña Aniversario", "H2O Deportivo", "Colombiana Nostalgia",
    "Hit Vitamina Escolar", "Energizante Nocturno", "Mega Combo Fiestas",
]
target_channels = ["Tienda de Barrio", "Supermercado", "Mayorista", "Autoservicio", "HORECA", "ALL"]

for year in [2024, 2025, 2026]:
    n_promos = 60 if year < 2026 else 30
    for i in range(n_promos):
        start = date(year, 1, 1) + timedelta(days=int(np.random.uniform(0, 330 if year < 2026 else 100)))
        duration = int(np.random.uniform(5, 40))
        end = start + timedelta(days=duration)
        region = np.random.choice(list(REGIONS.keys()) + ["ALL"], p=[0.11]*6 + [0.34])
        brand = np.random.choice(BRANDS)
        dtype = np.random.choice(DISCOUNT_TYPES, p=[0.45, 0.20, 0.20, 0.15])
        dval = {"percentage": np.random.choice([10, 15, 20, 25, 30, 50]),
                "fixed_amount": np.random.choice([500, 1000, 2000, 5000, 10000]),
                "bonus_units": np.random.choice([6, 12, 24, 48]),
                "rebate": np.random.choice([2, 3, 5, 8])}[dtype]
        target_channel = np.random.choice(target_channels, p=[0.20, 0.15, 0.15, 0.10, 0.05, 0.35])
        target_brand = brand if np.random.random() < 0.8 else "ALL"
        units_moved = int(np.random.uniform(5000, 800000))
        budget_cop = round(np.random.uniform(20000000, 900000000), 0) if np.random.random() < 0.8 else None

        promo_rows.append({
            "promo_id": f"PROMO-{year}-{i+1:03d}",
            "promo_name": np.random.choice(promo_names),
            "start_date": start,
            "end_date": end,
            "region": region,
            "brand": brand,
            "discount_type": dtype,
            "discount_value": float(dval),
            "target_channel": target_channel,
            "target_brand": target_brand,
            "units_moved": units_moved,
            "budget_cop": budget_cop,
        })

df_promos = pd.DataFrame(promo_rows)

# ── Inject DQ issues ──
# 3 promos: end_date ANTES de start_date
bad_date_idx = np.random.choice(len(df_promos), 3, replace=False)
for idx in bad_date_idx:
    df_promos.loc[idx, "end_date"] = df_promos.loc[idx, "start_date"] - timedelta(days=10)

# 5 promos: discount_value > 100 en tipo percentage
pct_idx = df_promos[df_promos["discount_type"] == "percentage"].index
if len(pct_idx) >= 5:
    bad_pct_idx = np.random.choice(pct_idx, 5, replace=False)
    df_promos.loc[bad_pct_idx, "discount_value"] = 150.0

# 10 promos: promo_name NULL
null_name_idx = np.random.choice(len(df_promos), 10, replace=False)
df_promos.loc[null_name_idx, "promo_name"] = None

print(f"fact_trade_promos: {len(df_promos)} rows")
print(f"  DQ issues: 3 inverted dates, 5 impossible discounts, 10 null names")

sdf_promos = spark.createDataFrame(df_promos)
sdf_promos.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_trade_promos")
print(f"✅ {CATALOG}.{SCHEMA}.fact_trade_promos written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 5: fact_daily_kpis

# COMMAND ----------

kpi_rows = []
regions_list = list(REGIONS.keys())

for region in regions_list:
    region_dcs = df_dcs[df_dcs["region"] == region]["dc_id"].tolist()
    n_dcs = len(region_dcs)

    for d in date_range:
        d_date = d.date()
        region_data = df_sales[(df_sales["dc_id"].isin(region_dcs)) & (df_sales["sale_date"] == d_date)]

        if len(region_data) == 0:
            total_orders = int(np.random.uniform(200, 900))
            total_units = int(total_orders * np.random.uniform(60, 140))
            total_revenue_cop = round(total_units * np.random.uniform(1800, 2600), 0)
        else:
            total_orders = int(region_data["orders_delivered"].sum())
            total_units = int(region_data["units_sold"].sum())
            total_revenue_cop = round(region_data["revenue_cop"].sum(), 0)

        avg_ticket_cop = round(total_revenue_cop / max(total_orders, 1), 0)
        fill_rate_pct = round(np.clip(np.random.normal(0.94, 0.04), 0.70, 1.0), 3)
        osa_pct = round(np.clip(np.random.normal(0.92, 0.05), 0.65, 1.0), 3)
        active_pos = int(np.random.uniform(3000, 45000))
        new_pos = int(np.random.uniform(20, 500))

        yoy_revenue_growth = None
        if d.year >= 2025:
            yoy_revenue_growth = round(np.random.normal(0.05, 0.06), 4)

        kpi_rows.append({
            "kpi_date": d_date,
            "region": region,
            "total_dcs": n_dcs,
            "total_orders": total_orders,
            "total_units": total_units,
            "total_revenue_cop": total_revenue_cop,
            "avg_ticket_cop": avg_ticket_cop,
            "fill_rate_pct": fill_rate_pct,
            "osa_pct": osa_pct,
            "active_pos": active_pos,
            "new_pos": new_pos,
            "yoy_revenue_growth": yoy_revenue_growth,
        })

df_kpis = pd.DataFrame(kpi_rows)

# ── Inject DQ issues ──
# 20 rows: total_units = 0 pero revenue > 0
zero_units_idx = np.random.choice(len(df_kpis), 20, replace=False)
df_kpis.loc[zero_units_idx, "total_units"] = 0

# 5 rows: region = "UNKNOWN"
unknown_idx = np.random.choice(len(df_kpis), 5, replace=False)
df_kpis.loc[unknown_idx, "region"] = "UNKNOWN"

# 3 rows: yoy_revenue_growth = 999.99 (sentinel outlier)
sentinel_idx = np.random.choice(df_kpis[df_kpis["yoy_revenue_growth"].notna()].index, 3, replace=False)
df_kpis.loc[sentinel_idx, "yoy_revenue_growth"] = 999.99

print(f"fact_daily_kpis: {len(df_kpis)} rows")
print(f"  DQ issues: 20 zero-units with revenue, 5 UNKNOWN region, 3 sentinel outliers")

sdf_kpis = spark.createDataFrame(df_kpis)
sdf_kpis.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_daily_kpis")
print(f"✅ {CATALOG}.{SCHEMA}.fact_daily_kpis written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------

print("=" * 60)
print("POSTOBÓN GENIE CODE WORKSHOP — DATA GENERATION SUMMARY")
print("=" * 60)

tables = ["dim_distribution_centers", "fact_daily_sales", "fact_pos_activity", "fact_trade_promos", "fact_daily_kpis"]
for t in tables:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.{t}").collect()[0]["cnt"]
    print(f"  {CATALOG}.{SCHEMA}.{t}: {count:,} rows")

print()
print("DQ Issues Summary:")
print("  dim_distribution_centers: 15 null region, 8 future dates, 12 zero coords, 5 dup IDs, 20 bad casing, 8 dock outliers")
print("  fact_daily_sales: 200 zero revenue, 50 negative unit price, 30 null dates, 40 bad fill_rate_pct")
print("  fact_pos_activity: 100 lowercase brands, 40 negative units, 25 orphan XX, 80 ref breaks")
print("  fact_trade_promos: 3 inverted dates, 5 impossible discounts, 10 null names")
print("  fact_daily_kpis: 20 zero-units with revenue, 5 UNKNOWN region, 3 sentinel outliers")
print()
print(f"Total DQ defects: ~360")
print("=" * 60)
print("✅ Data generation complete. Ready for workshop.")

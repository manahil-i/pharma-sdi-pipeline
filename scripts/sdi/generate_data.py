"""Data oversampling generator – creates NEW monthly data so the pipeline can be refreshed.

Refresh n creates 3 new months, continuing after the last real month (Dec 2025):
    refresh 1 -> Jan-Mar 2026, refresh 2 -> Apr-Jun 2026, refresh 3 -> Jul-Sep 2026 ...

How it works (explain this in your 1-2 page generator report):
1. Bootstrap sampling: for every new month we sample (with replacement) real sales lines of the
   SAME calendar month of 2025, so the seasonality (December peak, flu season) is kept.
   Bulk-posted days (> 10x the median daily count) are excluded from the sample pool.
2. Volume trend: the number of lines = 2025 count x (1 + VOLUME_GROWTH) with small random noise.
3. Quantities get a small random change (+/-1 pack on 25% of the lines that have 2+ packs).
4. Prices rise by PRICE_INFLATION; all money fields are re-computed so they stay consistent:
   gross = unit price x qty, discount keeps its original %, net = gross - discount,
   tax = 6% of net, COGS = unit cost x qty, margin = net - COGS.
5. New unique IDs continue after the highest real ID (SALE-, RX-, HDEM-).
6. Hospital demand: every hospital x medicine row of the same 2025 month is copied and all pack
   quantities are scaled by a random factor around (1 + VOLUME_GROWTH).
7. SCD demo: from refresh 2 onwards one Independent pharmacy becomes a Chain pharmacy (pharmacy_type
   is SCD Type 2, so the mart keeps both versions).
8. Data-quality test: a few broken rows are injected on purpose (missing GTIN, bad date, negative
   quantity, duplicate ID) so you can show the validation layer catching them.

Output (in data/landing, picked up automatically by the pipeline DAG):
    05_pharmacy_sales_data_refresh{n}_stream.csv   -> streaming ingestion
    06_hospital_demand_data_refresh{n}.csv         -> batch ingestion
and statistics for your report in data/generated/refresh{n}_stats.json

Run:  python -m sdi.generate_data --refresh 1        (or trigger the Airflow DAG sdi_generate_data)
"""
import argparse
import json
import datetime as dt
import calendar

import duckdb
import numpy as np
import pandas as pd

from . import config

VOLUME_GROWTH = 0.04        # +4% more lines / packs than the same month last year
PRICE_INFLATION = 0.03      # +3% retail price and unit cost
TAX_RATE = 0.06
MONTHS_PER_REFRESH = 3
FIRST_NEW_MONTH = dt.date(2026, 1, 1)
SALES_FILE = "05_pharmacy_sales_data.csv"
DEMAND_FILE = "06_hospital_demand_data.csv"


def refresh_months(n):
    months = []
    for k in range(MONTHS_PER_REFRESH):
        i = (n - 1) * MONTHS_PER_REFRESH + k
        y, m = FIRST_NEW_MONTH.year + i // 12, i % 12 + 1
        months.append(dt.date(y, m, 1))
    return months


def shift_date(text, year, month):
    """Keep the day of month, move to the new year/month (31 Jan -> 28 Feb if needed)."""
    d = int(text[8:10])
    d = min(d, calendar.monthrange(year, month)[1])
    return f"{year:04d}-{month:02d}-{d:02d}"


def money(x):
    return np.round(x.astype(float), 2)


# ------------------------------------------------------------------ pharmacy sales
def sample_pool(con):
    """Real 2025 sales lines, without bulk-posted days, as text."""
    src = (config.SOURCE / SALES_FILE).as_posix()
    # read the CSV straight from disk each time (DuckDB streams it, so memory stays low)
    con.execute(f"""CREATE OR REPLACE TEMP VIEW src AS
                    SELECT * FROM read_csv('{src}', all_varchar = true, header = true)""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE bulk AS
        WITH daily AS (SELECT SalesDate AS d, COUNT(*) AS n FROM src GROUP BY 1)
        SELECT d FROM daily WHERE n > {config.BULK_POSTING_FACTOR} * (SELECT median(n) FROM daily)""")
    max_sale, max_rx = con.execute("""SELECT MAX(CAST(substr(SalesTransactionID, 6) AS BIGINT)),
                                             MAX(CASE WHEN PrescriptionID LIKE 'RX-%'
                                                      THEN CAST(substr(PrescriptionID, 4) AS BIGINT) END)
                                      FROM src""").fetchone()
    pool = con.execute("""SELECT * FROM src WHERE SalesDate LIKE '2025-%'
                          AND SalesDate NOT IN (SELECT d FROM bulk)""").df().fillna("")   # empty cells stay ''
    return pool, max_sale, max_rx


def make_sales(pool, months, n, rng, max_sale, max_rx):
    parts, stats = [], []
    for month in months:
        same = pool[pool.SalesDate.str[5:7] == f"{month.month:02d}"]
        size = int(round(len(same) * (1 + VOLUME_GROWTH) * rng.normal(1, 0.02)))
        s = same.sample(n=size, replace=True, random_state=int(rng.integers(1e9))).reset_index(drop=True)
        s["SalesDate"] = [shift_date(t, month.year, month.month) for t in s.SalesDate]
        s["SalesTimestampUTC"] = s.SalesDate + s.SalesTimestampUTC.str[10:]
        parts.append(s)
        stats.append(dict(month=str(month), source_month=f"2025-{month.month:02d}", source_rows=len(same),
                          generated_rows=size))
    s = pd.concat(parts, ignore_index=True).sort_values("SalesTimestampUTC", ignore_index=True)

    # quantities: lines with 2+ packs get -1 or +1 pack 25% of the time (symmetric, so the average stays the same)
    qty0 = s.QuantitySoldPacks.astype(int)
    change = rng.choice([-1, 0, 1], size=len(s), p=[0.125, 0.75, 0.125])
    change[qty0.values == 1] = 0
    qty = np.maximum(qty0 + change, 1)
    # money fields (re-computed so they stay consistent)
    gross0 = s.GrossSalesAmountUSD.astype(float)
    disc_pct = np.where(gross0 > 0, s.DiscountAmountUSD.astype(float) / gross0, 0)
    unit_price = money(s.UnitRetailPriceUSD.astype(float) * (1 + PRICE_INFLATION))
    unit_cost = s.CostOfGoodsSoldUSD.astype(float) / qty0 * (1 + PRICE_INFLATION)
    gross = money(unit_price * qty)
    disc = money(gross * disc_pct)
    net = money(gross - disc)
    cogs = money(unit_cost * qty)
    s["QuantitySoldPacks"] = qty
    s["UnitRetailPriceUSD"] = unit_price
    s["GrossSalesAmountUSD"], s["DiscountAmountUSD"], s["NetSalesAmountUSD"] = gross, disc, net
    s["SalesTaxUSD"] = money(net * TAX_RATE)
    s["CostOfGoodsSoldUSD"] = cogs
    s["GrossMarginUSD"] = money(net - cogs)
    s["ReturnedQuantityPacks"] = np.minimum(s.ReturnedQuantityPacks.astype(int), qty)
    s["BatchExpiryDate"] = [f"{int(t[:4]) + 1}{t[4:]}" if t else t for t in s.BatchExpiryDate]

    # new unique IDs – refresh n starts at its own block so refreshes never collide
    start = max_sale + (n - 1) * 1_000_000 + 1
    s["SalesTransactionID"] = [f"SALE-{i:09d}" for i in range(start, start + len(s))]
    has_rx = s.PrescriptionID.str.startswith("RX-")
    rx_start = max_rx + (n - 1) * 1_000_000 + 1
    s.loc[has_rx, "PrescriptionID"] = [f"RX-{i:09d}" for i in range(rx_start, rx_start + has_rx.sum())]
    s["SourceSystem"] = "SDI_GENERATOR"
    return s, stats


def apply_scd_change(s, pool, n):
    """From refresh 2: one Independent pharmacy is re-branded as a Chain pharmacy."""
    indep = sorted(pool.loc[pool.PharmacyType == "Independent Pharmacy", "PharmacyID"].unique())
    if n < 2 or not indep:
        return None
    pid = indep[0]
    s.loc[s.PharmacyID == pid, "PharmacyType"] = "Chain Pharmacy"
    return dict(pharmacy_id=pid, old_type="Independent Pharmacy", new_type="Chain Pharmacy")


def inject_bad_sales(s, rng):
    """A few broken rows on purpose, to prove the data-quality checks work."""
    bad = s.sample(n=12, random_state=int(rng.integers(1e9))).copy()
    kinds = ["missing_gtin"] * 3 + ["bad_date"] * 3 + ["negative_qty"] * 3 + ["duplicate_id"] * 3
    for (idx, row), kind in zip(bad.iterrows(), kinds):
        if kind == "missing_gtin":
            bad.at[idx, "GTIN14"] = ""
        elif kind == "bad_date":
            bad.at[idx, "SalesDate"] = "2026-13-45"
        elif kind == "negative_qty":
            bad.at[idx, "QuantitySoldPacks"] = -2
    # rows that are NOT duplicates get a fresh ID so only the 'duplicate_id' rows clash
    fresh = [k != "duplicate_id" for k in kinds]
    bad.loc[fresh, "SalesTransactionID"] = [f"SALE-9{i:08d}" for i in rng.choice(10**8, sum(fresh), replace=False)]
    out = pd.concat([s, bad], ignore_index=True)
    return out.sample(frac=1, random_state=int(rng.integers(1e9))).sort_values("SalesTimestampUTC", kind="stable",
                                                                            ignore_index=True), \
        {k: kinds.count(k) for k in set(kinds)}


# ------------------------------------------------------------------ hospital demand
QTY_COLS = ["ForecastDemandPacks", "RequestedQuantityPacks", "ApprovedQuantityPacks", "ReceivedQuantityPacks",
            "EmergencyRequestQuantityPacks", "OpeningStockPacks", "DispensedQuantityPacks", "ConsumedQuantityPacks",
            "WastageQuantityPacks", "ClosingStockPacks", "UnfulfilledDemandPacks",
            "EstimatedDiseaseCases", "DiseaseRelatedAdmissions", "OutpatientEncounters"]


def make_demand(months, n, rng):
    src = pd.read_csv(config.SOURCE / DEMAND_FILE, dtype=str, keep_default_na=False)
    max_id = src.HospitalDemandRecordID.str[5:].astype(int).max()
    parts, stats = [], []
    for month in months:
        d = src[src.DemandMonth == f"2025-{month.month:02d}-01"].copy()
        factor = rng.normal(1 + VOLUME_GROWTH, 0.05, size=len(d))
        for c in QTY_COLS:
            d[c] = np.round(d[c].astype(float) * factor).astype(int)
        req = d.RequestedQuantityPacks.astype(int)
        d["ServiceLevelPct"] = np.where(req > 0, np.round(100 * (1 - d.UnfulfilledDemandPacks / req), 1), 100)
        d["DemandMonth"] = str(month)
        parts.append(d)
        stats.append(dict(month=str(month), source_rows=len(d), generated_rows=len(d),
                          consumed_packs=int(d.ConsumedQuantityPacks.sum())))
    d = pd.concat(parts, ignore_index=True)
    start = max_id + (n - 1) * 100_000 + 1
    d["HospitalDemandRecordID"] = [f"HDEM-{i:08d}" for i in range(start, start + len(d))]
    d["SourceSystem"] = "SDI_GENERATOR"
    # 2 broken rows on purpose
    bad = d.sample(n=2, random_state=int(rng.integers(1e9))).copy()
    bad.iloc[0, bad.columns.get_loc("ConsumedQuantityPacks")] = -5
    bad.iloc[1, bad.columns.get_loc("HospitalID")] = ""
    bad["HospitalDemandRecordID"] = [f"HDEM-9{i:07d}" for i in range(2)]
    return pd.concat([d, bad], ignore_index=True), stats


# ------------------------------------------------------------------ main entry point
def describe(df, cols):
    return {c: dict(mean=round(float(df[c].astype(float).mean()), 3), std=round(float(df[c].astype(float).std()), 3),
                    min=float(df[c].astype(float).min()), max=float(df[c].astype(float).max())) for c in cols}


def generate(refresh=1, seed=None):
    """Create refresh files in data/landing. Returns the statistics dictionary."""
    refresh = int(refresh)
    rng = np.random.default_rng(seed if seed is not None else 1000 + refresh)
    months = refresh_months(refresh)
    config.LANDING.mkdir(parents=True, exist_ok=True)
    config.GENERATED.mkdir(parents=True, exist_ok=True)

    with duckdb.connect() as con:
        pool, max_sale, max_rx = sample_pool(con)
    sales, sales_stats = make_sales(pool, months, refresh, rng, max_sale, max_rx)
    scd = apply_scd_change(sales, pool, refresh)
    real_same_months = pool[pool.SalesDate.str[5:7].isin([f"{m.month:02d}" for m in months])]
    sales_clean_stats = describe(sales, ["QuantitySoldPacks", "NetSalesAmountUSD"])
    clean_net = round(float(sales.NetSalesAmountUSD.astype(float).sum()), 2)
    sales, sales_bad = inject_bad_sales(sales, rng)
    demand, demand_stats = make_demand(months, refresh, rng)

    sales_path = config.LANDING / f"05_pharmacy_sales_data_refresh{refresh}{config.STREAM_MARKER}.csv"
    demand_path = config.LANDING / f"06_hospital_demand_data_refresh{refresh}.csv"
    sales.to_csv(sales_path, index=False)
    demand.to_csv(demand_path, index=False)

    stats = dict(
        refresh=refresh, months=[str(m) for m in months], created_at=dt.datetime.now().isoformat(timespec="seconds"),
        method="bootstrap sampling of the same calendar month of 2025 (bulk-posted days excluded)",
        parameters=dict(volume_growth=VOLUME_GROWTH, price_inflation=PRICE_INFLATION, tax_rate=TAX_RATE,
                        seed=seed if seed is not None else 1000 + refresh),
        sales=dict(file=sales_path.name, rows_written=len(sales), per_month=sales_stats,
                   injected_bad_rows=sales_bad,
                   real_2025_same_months=describe(real_same_months, ["QuantitySoldPacks", "NetSalesAmountUSD"]),
                   generated=sales_clean_stats,
                   net_sales_real_2025=round(float(real_same_months.NetSalesAmountUSD.astype(float).sum()), 2),
                   net_sales_generated=clean_net),
        demand=dict(file=demand_path.name, rows_written=len(demand), per_month=demand_stats,
                    injected_bad_rows={"negative_consumed": 1, "missing_hospital_id": 1}),
        scd_change=scd,
    )
    with open(config.GENERATED / f"refresh{refresh}_stats.json", "w") as f:
        json.dump(stats, f, indent=2)
    print(f"[generator] refresh {refresh}: {len(sales):,} sales lines and {len(demand):,} demand rows for "
          f"{stats['months'][0]} .. {stats['months'][-1]} written to {config.LANDING}")
    return stats


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate refresh data for the SDI pipeline")
    ap.add_argument("--refresh", type=int, default=1, help="refresh number (1 = Jan-Mar 2026, 2 = Apr-Jun 2026 ...)")
    ap.add_argument("--seed", type=int, default=None, help="random seed (default 1000 + refresh)")
    a = ap.parse_args()
    generate(a.refresh, a.seed)

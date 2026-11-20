"""Reference data and business rules used to build the dimensions.

Everything here comes from the Excel design file (Transform.OLTP / Dim.Tables sheets):
calendar flags, US state -> region map, city enrichment (US Census 2020), bands and abbreviations.
"""
import calendar
import datetime as dt

# =============================================================== calendar (DimDate)
def _nth_weekday(y, m, wd, n):
    d = dt.date(y, m, 1)
    d += dt.timedelta((wd - d.weekday()) % 7)
    return d + dt.timedelta(weeks=n - 1)


def _last_weekday(y, m, wd):
    d = dt.date(y, m, calendar.monthrange(y, m)[1])
    return d - dt.timedelta((d.weekday() - wd) % 7)


def _observed(d):
    if d.weekday() == 5:
        return d - dt.timedelta(1)
    if d.weekday() == 6:
        return d + dt.timedelta(1)
    return d


def us_federal_holidays(y):
    """The 11 US federal holidays: (name, actual date, observed date)."""
    h = [("New Year's Day", dt.date(y, 1, 1)), ("Martin Luther King Jr. Day", _nth_weekday(y, 1, 0, 3)),
         ("Presidents' Day", _nth_weekday(y, 2, 0, 3)), ("Memorial Day", _last_weekday(y, 5, 0)),
         ("Juneteenth", dt.date(y, 6, 19)), ("Independence Day", dt.date(y, 7, 4)),
         ("Labor Day", _nth_weekday(y, 9, 0, 1)), ("Columbus Day", _nth_weekday(y, 10, 0, 2)),
         ("Veterans Day", dt.date(y, 11, 11)), ("Thanksgiving", _nth_weekday(y, 11, 3, 4)),
         ("Christmas Day", dt.date(y, 12, 25))]
    return [(n, d, _observed(d)) for n, d in h]


SEASON = {12: "Winter", 1: "Winter", 2: "Winter", 3: "Spring", 4: "Spring", 5: "Spring",
          6: "Summer", 7: "Summer", 8: "Summer", 9: "Autumn", 10: "Autumn", 11: "Autumn"}

DATE_COLUMNS = ["date_key", "date", "day_of_month", "day_name", "day_short", "day_of_week", "is_weekend",
                "is_working_day", "is_holiday", "holiday_name", "iso_week", "week_start", "month_start",
                "year_month", "month_name", "month_short", "month_no", "month_seq", "quarter", "quarter_label",
                "half", "year", "is_month_end", "is_christmas_month", "is_thanksgiving_month",
                "is_holiday_season", "season", "flu_season_flag", "allergy_season_flag"]


def calendar_rows(start="2023-01-01", end="2027-12-31"):
    """One row per day with every DimDate attribute (same rules as the Excel design)."""
    start, end = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
    hol = {}
    for y in range(start.year - 1, end.year + 2):
        for name, d, o in us_federal_holidays(y):
            hol.setdefault(d, name)
            if o != d:
                hol.setdefault(o, name + " (observed)")
    yn = lambda b: "Y" if b else "N"
    rows, d = [], start
    while d <= end:
        m, wk, is_hol = d.month, d.weekday() >= 5, d in hol
        rows.append([int(d.strftime("%Y%m%d")), d, d.day, calendar.day_name[d.weekday()],
                     calendar.day_abbr[d.weekday()], d.weekday() + 1, yn(wk), yn(not wk and not is_hol), yn(is_hol),
                     hol.get(d, "None"), d.isocalendar()[1], d - dt.timedelta(d.weekday()), dt.date(d.year, m, 1),
                     f"{d.year}-{m:02d}", calendar.month_name[m], calendar.month_abbr[m], m,
                     (d.year - start.year) * 12 + m, f"Q{(m - 1) // 3 + 1}", f"{d.year}-Q{(m - 1) // 3 + 1}",
                     f"H{1 if m <= 6 else 2}", d.year, yn(d.day == calendar.monthrange(d.year, m)[1]),
                     yn(m == 12), yn(m == 11), yn(m in (11, 12)), SEASON[m], yn(m in (10, 11, 12, 1, 2, 3)),
                     yn(m in (3, 4, 5))])
        d += dt.timedelta(1)
    return rows


# =============================================================== geography (DimLocation)
# state code -> (state name, census region, census division)
STATES = {
    "CT": ("Connecticut", "Northeast", "New England"), "ME": ("Maine", "Northeast", "New England"),
    "MA": ("Massachusetts", "Northeast", "New England"), "NH": ("New Hampshire", "Northeast", "New England"),
    "RI": ("Rhode Island", "Northeast", "New England"), "VT": ("Vermont", "Northeast", "New England"),
    "NJ": ("New Jersey", "Northeast", "Middle Atlantic"), "NY": ("New York", "Northeast", "Middle Atlantic"),
    "PA": ("Pennsylvania", "Northeast", "Middle Atlantic"),
    "IL": ("Illinois", "Midwest", "East North Central"), "IN": ("Indiana", "Midwest", "East North Central"),
    "MI": ("Michigan", "Midwest", "East North Central"), "OH": ("Ohio", "Midwest", "East North Central"),
    "WI": ("Wisconsin", "Midwest", "East North Central"), "IA": ("Iowa", "Midwest", "West North Central"),
    "KS": ("Kansas", "Midwest", "West North Central"), "MN": ("Minnesota", "Midwest", "West North Central"),
    "MO": ("Missouri", "Midwest", "West North Central"), "NE": ("Nebraska", "Midwest", "West North Central"),
    "ND": ("North Dakota", "Midwest", "West North Central"), "SD": ("South Dakota", "Midwest", "West North Central"),
    "DE": ("Delaware", "South", "South Atlantic"), "DC": ("District of Columbia", "South", "South Atlantic"),
    "FL": ("Florida", "South", "South Atlantic"), "GA": ("Georgia", "South", "South Atlantic"),
    "MD": ("Maryland", "South", "South Atlantic"), "NC": ("North Carolina", "South", "South Atlantic"),
    "SC": ("South Carolina", "South", "South Atlantic"), "VA": ("Virginia", "South", "South Atlantic"),
    "WV": ("West Virginia", "South", "South Atlantic"), "AL": ("Alabama", "South", "East South Central"),
    "KY": ("Kentucky", "South", "East South Central"), "MS": ("Mississippi", "South", "East South Central"),
    "TN": ("Tennessee", "South", "East South Central"), "AR": ("Arkansas", "South", "West South Central"),
    "LA": ("Louisiana", "South", "West South Central"), "OK": ("Oklahoma", "South", "West South Central"),
    "TX": ("Texas", "South", "West South Central"), "AZ": ("Arizona", "West", "Mountain"),
    "CO": ("Colorado", "West", "Mountain"), "ID": ("Idaho", "West", "Mountain"), "MT": ("Montana", "West", "Mountain"),
    "NV": ("Nevada", "West", "Mountain"), "NM": ("New Mexico", "West", "Mountain"), "UT": ("Utah", "West", "Mountain"),
    "WY": ("Wyoming", "West", "Mountain"), "AK": ("Alaska", "West", "Pacific"), "CA": ("California", "West", "Pacific"),
    "HI": ("Hawaii", "West", "Pacific"), "OR": ("Oregon", "West", "Pacific"), "WA": ("Washington", "West", "Pacific"),
}

# City enrichment – US Census 2020 (population, land area sq mi); lat/long = city centre
CITIES = {
    ("New York", "NY"): (8804190, 300.5, "Eastern", 40.71, -74.01, "Cold winter"),
    ("Philadelphia", "PA"): (1603797, 134.4, "Eastern", 39.95, -75.17, "Cold winter"),
    ("Pittsburgh", "PA"): (302971, 55.4, "Eastern", 40.44, -80.00, "Cold winter"),
    ("Boston", "MA"): (675647, 48.3, "Eastern", 42.36, -71.06, "Cold winter"),
    ("Hartford", "CT"): (121054, 17.4, "Eastern", 41.76, -72.67, "Cold winter"),
    ("Chicago", "IL"): (2746388, 227.7, "Central", 41.88, -87.63, "Cold winter"),
    ("Detroit", "MI"): (639111, 138.7, "Eastern", 42.33, -83.05, "Cold winter"),
    ("Columbus", "OH"): (905748, 220.0, "Eastern", 39.96, -83.00, "Cold winter"),
    ("Houston", "TX"): (2304580, 640.4, "Central", 29.76, -95.37, "Warm humid"),
    ("Dallas", "TX"): (1304379, 339.6, "Central", 32.78, -96.80, "Warm humid"),
    ("Atlanta", "GA"): (498715, 135.3, "Eastern", 33.75, -84.39, "Warm humid"),
    ("Miami", "FL"): (442241, 36.0, "Eastern", 25.76, -80.19, "Warm humid"),
    ("Charlotte", "NC"): (874579, 308.3, "Eastern", 35.23, -80.84, "Warm humid"),
    ("Nashville", "TN"): (689447, 475.8, "Central", 36.16, -86.78, "Warm humid"),
    ("Los Angeles", "CA"): (3898747, 469.5, "Pacific", 34.05, -118.24, "Dry / mild"),
    ("San Diego", "CA"): (1386932, 325.9, "Pacific", 32.72, -117.16, "Dry / mild"),
    ("Las Vegas", "NV"): (641903, 141.8, "Pacific", 36.17, -115.14, "Dry / mild"),
    ("Denver", "CO"): (715522, 153.1, "Mountain", 39.74, -104.99, "Cold winter"),
}


def city_size_band(pop):
    if pop is None:
        return "Unknown"
    if pop >= 2_000_000:
        return "4-Mega (2M+)"
    if pop >= 1_000_000:
        return "3-Large (1–2M)"
    if pop >= 500_000:
        return "2-Mid (500k–1M)"
    return "1-Small (<500k)"


# =============================================================== medicines (DimMedicine)
THERAPY_TYPE = {"Diabetes Mellitus": "Chronic", "Cardiovascular Disease": "Chronic", "Respiratory Disease": "Chronic",
                "Neurological and Mental Health Conditions": "Chronic", "Infectious Disease": "Acute",
                "Gastrointestinal Disease": "Mixed", "Pain and Inflammatory Conditions": "Mixed"}


def price_tier(rrp):
    if rrp < 5:
        return "1-Low (<$5)"
    if rrp < 15:
        return "2-Mid ($5–15)"
    return "3-High ($15+)"


# =============================================================== abbreviations (OLTP.XRay rules)
PHARMACY_PREFIX = [("Clarity Medical Center Pharmacy", "CMC Pharm"), ("WellSpring Pharmacy", "WS Pharm"),
                   ("Community Care Pharmacy", "CC Pharm")]


def pharmacy_short(name):
    for full, short in PHARMACY_PREFIX:
        if name.startswith(full):
            return name.replace(full, short)
    return name[:15]


def hospital_short(name):
    """Initials of the capitalised words: 'Coastal Care Hospital' -> 'CCH'."""
    return "".join(w[0] for w in name.split() if w[:1].isupper())


WAREHOUSE_SHORT = {"Midwest Distribution Center": "MWDC", "New England Cold Chain Hub": "NECCH",
                   "Northeast Distribution Center": "NEDC", "Southeast Distribution Center": "SEDC",
                   "Southern Distribution Center": "SDC", "Western Distribution Center": "WDC"}


def warehouse_region(name):
    return "New England" if name.startswith("New England") else name.split()[0]


def band3(value, low, high, labels):
    if value is None:
        return "N/A"
    return labels[0] if value < low else labels[1] if value < high else labels[2]


BED_BANDS = (400, 700, ["1-Small (<400)", "2-Medium (400–699)", "3-Large (700+)"])
ICU_BANDS = (40, 80, ["1-Small (<40)", "2-Medium (40–79)", "3-Large (80+)"])
OCCUPANCY_TIERS = (70, 85, ["1-Moderate (<70%)", "2-Optimal (70–84%)", "3-Near Capacity (85%+)"])
CAPACITY_BANDS = (500_000, 800_000, ["1-Small (<500k)", "2-Medium (500k–799k)", "3-Large (800k+)"])

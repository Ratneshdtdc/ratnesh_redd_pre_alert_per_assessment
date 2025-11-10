# full_raw_xlsx_parser_with_analysis.py
import zipfile
import xml.etree.ElementTree as ET
import re
import pandas as pd
import numpy as np
from io import BytesIO
import streamlit as st

# ---------- CONFIG ----------
file_path = "input data.xlsx"    # change to your file path
exclude_sheet_name = "0411"
base_year = 2025                 # unused here, sheets used as-is
# ----------------------------

# Helpers
def col_letter_to_index(letter):
    """Convert excel column letters (e.g. 'A', 'AA') to 0-based index."""
    letter = letter.upper()
    idx = 0
    for ch in letter:
        idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return idx - 1

def parse_shared_strings(z):
    """Return list of shared strings (or empty list)."""
    try:
        ss_bytes = z.read("xl/sharedStrings.xml")
    except KeyError:
        return []
    root = ET.fromstring(ss_bytes)
    # Excel uses <si>(<t>text</t> | <r>...)</si>
    ns = {"a": root.tag.split('}')[0].strip('{')} if '}' in root.tag else {}
    strings = []
    for si in root.findall(".//{*}si"):
        # join all t elements inside si (handles rich text)
        text_parts = [t.text or "" for t in si.findall(".//{*}t")]
        s = "".join(text_parts)
        strings.append(s)
    return strings

def parse_workbook_rels(z):
    """Return mapping rId -> target (e.g. 'worksheets/sheet1.xml')"""
    try:
        rels_bytes = z.read("xl/_rels/workbook.xml.rels")
    except KeyError:
        return {}
    root = ET.fromstring(rels_bytes)
    rels = {}
    for rel in root.findall(".//{*}Relationship"):
        rId = rel.attrib.get("Id")
        target = rel.attrib.get("Target")
        rels[rId] = target
    return rels

def parse_workbook(z):
    """Return list of (sheetName, sheetTargetPath) in workbook order."""
    wb_bytes = z.read("xl/workbook.xml")
    root = ET.fromstring(wb_bytes)
    sheets = []
    # find all sheet elements
    for sh in root.findall(".//{*}sheet"):
        name = sh.attrib.get("name")
        rId = sh.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
        sheets.append((name, rId))
    return sheets

def parse_sheet(z, sheet_path, shared_strings):
    """Parse a single sheet xml into list-of-rows (each row is dict col_letter->value)."""
    sheet_bytes = z.read(f"xl/{sheet_path}" if not sheet_path.startswith("xl/") else sheet_path)
    root = ET.fromstring(sheet_bytes)
    rows = []
    for row in root.findall(".//{*}row"):
        row_dict = {}
        for c in row.findall(".//{*}c"):
            ref = c.attrib.get("r")  # e.g. 'A1'
            # extract column letters
            m = re.match(r"([A-Z]+)(\d+)", ref)
            if not m:
                continue
            col_letters = m.group(1)
            # detect type
            c_type = c.attrib.get("t")
            v = c.find("{*}v")
            is_val = None if v is None else v.text
            if c_type == "s":  # shared string
                try:
                    val = shared_strings[int(is_val)] if is_val is not None else ""
                except Exception:
                    val = is_val
            elif c_type == "b":  # boolean
                val = True if is_val == "1" else False
            else:
                # inlineStr handling
                if c_type == "inlineStr":
                    t = c.find(".//{*}t")
                    val = t.text if t is not None else ""
                else:
                    val = is_val
            row_dict[col_letters] = val
        rows.append(row_dict)
    return rows

def rows_to_dataframe(rows):
    """Convert list of row-dicts (colletter->val) to DataFrame with header as first row."""
    if not rows:
        return pd.DataFrame()
    # find all column letters used
    cols = set()
    for r in rows:
        cols.update(r.keys())
    # sort columns by excel order
    cols_sorted = sorted(list(cols), key=lambda x: col_letter_to_index(x))
    # build matrix
    matrix = []
    for r in rows:
        row_values = [r.get(c, None) for c in cols_sorted]
        matrix.append(row_values)
    df = pd.DataFrame(matrix, columns=cols_sorted)
    # first row is header
    header = df.iloc[0].fillna("").astype(str).tolist()
    df2 = df[1:].copy()
    # sanitize header strings and map to valid column names
    header = [h.strip().replace(" ", "_").replace("=", "").replace(">", "_gt_") for h in header]
    df2.columns = header
    # reset index
    df2 = df2.reset_index(drop=True)
    return df2

# ---------- Main: extract all sheets robustly ----------
with zipfile.ZipFile(file_path, 'r') as z:
    shared_strings = parse_shared_strings(z)
    rels = parse_workbook_rels(z)
    sheets = parse_workbook(z)  # list of (name, rId)
    extracted_dfs = []
    for name, rId in sheets:
        if name == exclude_sheet_name:
            print(f"Skipping sheet {name}")
            continue
        target = rels.get(rId)
        if not target:
            print(f"Warning: no rel for sheet {name} (rId={rId}), skipping")
            continue
        # normalize target path
        target_path = target if target.startswith("xl/") else f"xl/{target}"
        try:
            rows = parse_sheet(z, target_path, shared_strings)
            df = rows_to_dataframe(rows)
            df["SheetName"] = name
            df["__Sheet_OrigPath"] = target_path
            extracted_dfs.append(df)
            print(f"Loaded sheet: {name}, rows: {len(df)}")
        except KeyError as e:
            print(f"Error reading sheet {name}: {e}")
        except ET.ParseError as e:
            print(f"XML parse error in sheet {name}: {e}")

if not extracted_dfs:
    raise SystemExit("No sheets were successfully loaded. Check the file.")

# Concatenate all sheets (columns may differ; that's fine)
raw = pd.concat(extracted_dfs, ignore_index=True, sort=False)

# ---------- Normalize expected columns ----------
# Some header names might slightly differ. We'll map likely variants to canonical names.
col_map = {}
canon = {
    "Dlv_Zone": ["Dlv_Zone", "Dlv Zone", "DlvZone", "Zone"],
    "Dlv_Region": ["Dlv_Region", "Dlv Region", "Region", "DlvRegion"],
    "Older_than_7_days": ["Older_than_7_days", "Older than 7 days", "Older_than_7", "Older_than_7days"],
    "Last_7_days": ["Last_7_days", "Last 7 days", "Last_7days"],
    "REDD_Today": ["REDD_Today", "REDD = Today", "REDD_Today*", "REDDToday"],
    "Next_7_days": ["Next_7_days", "Next 7 days", "Next_7days"],
    "Future_>7_days": ["Future_>7_days", "Future >7 days", "Future_>7days"],
    "Total": ["Total", "TOTAL"]
}

# normalize raw column names
raw_cols = list(raw.columns)
for rc in raw_cols:
    matched = False
    for k, variants in canon.items():
        for v in variants:
            if rc.lower().replace(" ", "").replace("_", "") == v.lower().replace(" ", "").replace("_", ""):
                col_map[rc] = k
                matched = True
                break
        if matched:
            break
    if not matched:
        # keep as-is (may include date columns, etc.)
        col_map[rc] = rc

raw = raw.rename(columns=col_map)

# Ensure numeric conversion for relevant columns
num_cols = ["Older_than_7_days", "Last_7_days", "REDD_Today", "Next_7_days", "Future_>7_days", "Total"]
for c in num_cols:
    if c in raw.columns:
        raw[c] = pd.to_numeric(raw[c], errors="coerce").fillna(0)
    else:
        raw[c] = 0

# If there's a date column or sheet name as date, try to create a Date column from SheetName
# Here we assume sheet names are like '0111','0211' etc. We'll append a year to convert to real date if feasible
def sheetname_to_date(sn):
    # try parse formats like '0111' => Nov 01 (depending on pattern you earlier used; adjust if needed)
    s = str(sn).strip()
    if re.fullmatch(r"\d{4}", s):  # e.g. '0111'
        mm = s[2:4]
        dd = s[0:2]
        # choose a year (not strictly needed). We'll try 2025 as earlier mentioned.
        try:
            return pd.to_datetime(f"2025-{mm}-{dd}", format="%Y-%m-%d", errors="coerce")
        except Exception:
            return pd.NaT
    return pd.NaT

if "Date" not in raw.columns:
    raw["Date"] = raw["SheetName"].apply(sheetname_to_date)

# ---------- Now the corrected analysis pipeline ----------
data = raw.copy()

# data.to_csv("data_temp.csv")


st.title("Reached at Dest REDD Based Open Volume | Analysis & Targets")
st.divider()

# Define pending bands
data["Critical_Pending"] = data["Older_than_7_days"] + data["Last_7_days"] + data["REDD_Today"]
data["Upcoming_Pending"] = data["Next_7_days"] + data["Future_>7_days"]
data["Total_Pending"] = data["Critical_Pending"] + data["Upcoming_Pending"]

# Revised ratio: overdue share among all pending
data["Critical_Ratio"] = data["Critical_Pending"] / data["Total_Pending"].replace(0, np.nan)

# Per-date, per-region daily rank (lower ratio better)
# Ensure Date exists, else use SheetName as proxy (string)
if data["Date"].isna().all():
    data["_DateProxy"] = data["SheetName"]
    rank_group = "_DateProxy"
else:
    rank_group = "Date"

data["Daily_Rank"] = data.groupby(rank_group)["Critical_Ratio"].rank(method="dense", ascending=True)

# Aggregate region metrics (stats across provided sheets)
agg = data.groupby("Dlv_Region").agg(
    Avg_Critical_Ratio=("Critical_Ratio", "mean"),
    Avg_Rank=("Daily_Rank", "mean"),
    Std_Critical_Ratio=("Critical_Ratio", "std"),
    Mean_Critical_Ratio=("Critical_Ratio", "mean"),
    Days=("Date", lambda x: x.nunique() if rank_group == "Date" else x.count())
).reset_index()

agg["Stat_Consistency"] = 1 - (agg["Std_Critical_Ratio"] / agg["Mean_Critical_Cratio"] ) if "Mean_Critical_Cratio" in agg.columns else 1 - (agg["Std_Critical_Ratio"] / agg["Mean_Critical_Ratio"])

# Directional consistency: percent of days with improving Critical_Ratio vs previous day for each region
def directional_consistency(subdf):
    # use sorted by Date or SheetName proxy
    if rank_group == "Date":
        sub = subdf.sort_values("Date")
    else:
        sub = subdf.sort_values("SheetName")
    diffs = sub["Critical_Ratio"].diff().dropna()
    if len(diffs) == 0:
        return np.nan
    return (diffs < 0).sum() / len(diffs)

dir_cons = data.groupby("Dlv_Region").apply(directional_consistency).reset_index(name="Dir_Consistency")
agg = agg.merge(dir_cons, on="Dlv_Region", how="left")

# Composite consistency (weights)
agg["Composite_Consistency"] = 0.6 * agg["Stat_Consistency"].fillna(0) + 0.4 * agg["Dir_Consistency"].fillna(0)

data['Future__gt_7_days'] =  data['Future__gt_7_days'].astype(float)

region_avg = (
    data.groupby("Dlv_Region", as_index=False)
    .agg({
        "Older_than_7_days": "mean",
        "Last_7_days": "mean",
        "REDD_Today": "mean",
        "Next_7_days": "mean",
        "Future__gt_7_days": "mean",
        "Critical_Pending": "mean",
        "Upcoming_Pending": "mean",
        "Total_Pending": "mean",
        "Critical_Ratio": "mean"
    })
)

mean_ratio = region_avg["Critical_Ratio"].mean()
std_ratio = region_avg["Critical_Ratio"].std()

def assign_tier(x):
    if x < mean_ratio - 0.5 * std_ratio:
        return "Top"
    elif x > mean_ratio + 0.5 * std_ratio:
        return "Poor"
    else:
        return "Average"


region_avg["Performance_Tier"] = region_avg["Critical_Ratio"].apply(assign_tier)

# --- Step 3: Assign dynamic reduction rates ---
reduction_map = {"Top": 0.95, "Average": 0.90, "Poor": 0.95}
region_avg["Reduction_Factor"] = region_avg["Performance_Tier"].map(reduction_map)

region_avg["Target_Older_than_7_days"] = region_avg["Older_than_7_days"] * region_avg["Reduction_Factor"]
region_avg["Target_Last_7_days"] = region_avg["Last_7_days"] * region_avg["Reduction_Factor"]
region_avg["Target_REDD_Today"] = region_avg["REDD_Today"] * region_avg["Reduction_Factor"]

region_avg["Target_Critical_Pending"] = (
    region_avg["Target_Older_than_7_days"] +
    region_avg["Target_Last_7_days"] +
    region_avg["Target_REDD_Today"]
)

region_avg["Target_Critical_Ratio"] = region_avg["Target_Critical_Pending"] / (
    region_avg["Target_Critical_Pending"] + region_avg["Upcoming_Pending"]
).replace(0, np.nan)

# Merge agg with latest targets
final = agg.merge(region_avg, on="Dlv_Region", how="left")

# Categorization
q25 = agg["Avg_Critical_Ratio"].quantile(0.25)
def categorize(row):
    if pd.isna(row["Avg_Critical_Ratio"]):
        return "NoData"
    if row["Avg_Critical_Ratio"] < q25 and row["Composite_Consistency"] > 0.8:
        return "A: Stable Performer"
    elif row["Composite_Consistency"] > 0.7:
        return "B: Improving"
    elif row["Avg_Critical_Ratio"] < q25:
        return "C: Volatile"
    else:
        return "D: Underperformer"

final["Category"] = final.apply(categorize, axis=1)

# Reorder cols sensibly for output
out_cols = [
    "Dlv_Region", "Category", "Avg_Critical_Ratio", "Avg_Rank",
    "Composite_Consistency", "Critical_Pending", "Upcoming_Pending", "Total_Pending",
    "Target_Critical_Pending", "Target_Critical_Ratio"
]
# ensure columns exist
out_cols = [c for c in out_cols if c in final.columns]
output = final[out_cols].sort_values("Avg_Critical_Ratio")

# Save
output.to_excel("region_performance_summary.xlsx", index=False)
print("✅ Saved region_performance_summary.xlsx")
print(output.head(50))

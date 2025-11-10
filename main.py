# full_raw_xlsx_parser_with_analysis.py
import zipfile
import xml.etree.ElementTree as ET
import re
import pandas as pd
import numpy as np
from io import BytesIO
import streamlit as st
import plotly.express as px

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

st.set_page_config(layout="wide")
st.title("Reached at Dest REDD Based Open Volume | Analysis & Targets")
st.divider()
st.header("📊 1. REDD wise Pending Volume Trend")

st.write("Consists Data from 1st Nov to 10th Nov (Except 4th Nov).")
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



# --- Sample dataframe (replace this with your actual data) ---
# data = pd.read_csv('your_file.csv')

# Ensure Date column is properly parsed
#data['Date'] = data['SheetName'].str.extract(r'A0*(\d{2})(\d{2})')[0] + '-' + data['SheetName'].str.extract(r'A0*(\d{2})(\d{2})')[1]
#data['Date'] = pd.to_datetime('2025-' + data['SheetName'].str[3:5] + '-' + data['SheetName'].str[5:7], errors='coerce')
#A0111
#data['Date'] = pd.to_datetime(
#    '2025-' + data['_DateProxy'].str[-2:] + '-' + data['_DateProxy'].str[2:4],
#    errors='coerce'
#)

data['Date'] = pd.to_datetime(
    '2025-' + "11" + '-' + data['_DateProxy'].str[1:3],
    errors='coerce'
)
# --- Streamlit layout ---
st.set_page_config(layout="wide")
#st.title("📊 Critical Pending & Ratio Trend Dashboard")

# --- Filters ---
#zones = ["All"] + sorted(data["Dlv_Zone"].dropna().unique().tolist())
regions = ["All"] + sorted(data["Dlv_Region"].dropna().unique().tolist())

col1, col2 = st.columns(2)
#selected_zone = col1.selectbox("Select Zone", zones)
selected_region = col1.selectbox("Select Region", regions)

filtered_df = data.copy()
# if selected_zone != "All":
#     filtered_df = filtered_df[filtered_df["Dlv_Zone"] == selected_zone]
if selected_region != "All":
    filtered_df = filtered_df[filtered_df["Dlv_Region"] == selected_region]

# --- Aggregate per Date and Region ---
trend = (
    filtered_df.groupby(["Date", "Dlv_Region"], as_index=False)
    .agg({"Critical_Pending": "sum", "Critical_Ratio": "mean"})
    .sort_values("Date")
)

# --- Charts side by side ---
c1, c2 = st.columns(2)

with c1:
    fig_pending = px.line(
        trend,
        x="Date",
        y="Critical_Pending",
        color="Dlv_Region",
        markers=True,
        title="📦 Pending Vol: REDD Today or earlier Trend",
    )
    fig_pending.update_layout(xaxis_title="", yaxis_title="Pending Vol: REDD Today or earlier")
    st.plotly_chart(fig_pending, use_container_width=True)

with c2:
    fig_ratio = px.line(
        trend,
        x="Date",
        y="Critical_Ratio",
        color="Dlv_Region",
        markers=True,
        title="⚠️ REDD Today or earlier/Total Pending Ratio",
    )
    fig_ratio.update_layout(xaxis_title="", yaxis_title="REDD Today or earlier/Total Pending Ratio")
    st.plotly_chart(fig_ratio, use_container_width=True)

st.divider()
st.header("📊 2. Daily Trend of Ranks of Regions")

# --- Optional zone filter ---
# zones = ["All"] + sorted(data["Dlv_Region"].dropna().unique().tolist())
# selected_zone = st.selectbox("Select Region", zones)

# filtered_df = data.copy()
# if selected_zone != "All":
#    filtered_df = filtered_df[filtered_df["Dlv_Region"] == selected_zone]
    
# --- Plot line chart for daily ranks ---
fig = px.line(
    filtered_df,
    x="Date",
    y="Daily_Rank",
    color="Dlv_Region",
    markers=True,
    title="📅 Daily Rank Trend",
)

# --- Invert y-axis because Rank 1 is top ---
fig.update_yaxes(autorange="reversed", title="Daily Rank")
fig.update_layout(xaxis_title="", legend_title_text="Region")

st.plotly_chart(fig, use_container_width=True)


#st.write("Regions Ranks:")


# Aggregate region metrics (stats across provided sheets)
agg = data.groupby("Dlv_Region").agg(
    Avg_Critical_Ratio=("Critical_Ratio", "mean"),
    Avg_Rank=("Daily_Rank", "mean"),
    Std_Critical_Ratio=("Critical_Ratio", "std"),
    Mean_Critical_Ratio=("Critical_Ratio", "mean"),
    Days=("Date", lambda x: x.nunique() if rank_group == "Date" else x.count())
).reset_index()

st.divider()
st.header("🏅 3. Final Standings of Regions")

agg['Rank'] = agg['Avg_Critical_Ratio'].rank(ascending=True, method='dense').astype(int)
agg = agg.sort_values('Avg_Critical_Ratio', ascending=True)

st.dataframe(
    agg[['Rank', 'Dlv_Region', 'Avg_Critical_Ratio', 'Avg_Rank', 'Std_Critical_Ratio']]
        .style.format({'Avg_Critical_Ratio': '{:.2f}', 'Avg_Rank': '{:.2f}', 'Std_Critical_Ratio': '{:.2f}'}),
    column_config={
        "Avg_Critical_Ratio": st.column_config.NumberColumn(
            "Avg REDD Today or before / Total Pending Ratio",
            format="%.2f"
        ),
        "Std_Critical_Ratio": st.column_config.NumberColumn(
            "Std Dev of REDD Today or before / Total Pending Ratio",
            format="%.2f"
        ),
        "Avg_Rank": st.column_config.NumberColumn(
            "Average Rank (Across Days)",
            format="%.2f"
        ),
        "Rank": st.column_config.NumberColumn(
            "Final Rank",
            format="%d"
        ),
    },
    use_container_width=True,
    hide_index=True,
)


# fig = px.bar(
#     agg,
#     x='Dlv_Region',
#     y='Avg_Critical_Ratio',
#     color='Avg_Critical_Ratio',
#     color_continuous_scale='RdYlGn_r',
#     text='Rank',
#     title='📊 Overall Standings of Regions',
# )
# fig.update_traces(textposition='outside')
# fig.update_layout(
#     xaxis_title='Region',
#     yaxis_title='Avg Critical Ratio',
#     coloraxis_showscale=False,
#     uniformtext_minsize=8,
#     uniformtext_mode='hide',
# )

# st.plotly_chart(fig, use_container_width=True)

st.divider()
st.header("📈 4. Ranking as per Consistency")



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

agg["Rank"] = agg["Composite_Consistency"].rank(ascending=False, method='dense').astype(int)
agg = agg.sort_values("Composite_Consistency", ascending=False)

# --- Streamlit layout ---
#st.set_page_config(layout="wide")
#st.title(" Region Ranking by Composite Consistency")

st.markdown("""
**Composite Consistency** reflects how reliably a region performs across days -
it blends *statistical stability (60% weight)* and *directional improvement consistency (40% weight)*.
""")

# --- Styled dataframe ---
st.dataframe(
    agg[['Rank', 'Dlv_Region', 'Stat_Consistency', 'Dir_Consistency', 'Composite_Consistency']]
        .style.format({'Stat_Consistency': '{:.3f}', 'Dir_Consistency': '{:.3f}', 'Composite_Consistency': '{:.3f}'}),
    column_config={
        "Stat_Consistency": st.column_config.NumberColumn("Statistical Consistency Score", format="%.3f"),
        "Dir_Consistency": st.column_config.NumberColumn("Directional Consistency Score", format="%.3f"),
        "Composite_Consistency": st.column_config.NumberColumn("Composite Consistency Score (Weighted 0.6/0.4)", format="%.3f"),
        "Rank": st.column_config.NumberColumn("Final Rank", format="%d"),
    },
    hide_index=True,
    use_container_width=True,
)

# --- Bar chart ---
fig = px.bar(
    agg,
    x="Dlv_Region",
    y="Composite_Consistency",
    color="Composite_Consistency",
    color_continuous_scale="RdYlGn",
    text="Rank",
    title="🏅 Composite Consistency by Region",
)

fig.update_traces(textposition='outside')
fig.update_layout(xaxis_title="Dlv Region", yaxis_title="Composite Consistency Score", coloraxis_showscale=False)
st.plotly_chart(fig, use_container_width=True)

#st.dataframe(agg)

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

st.divider()
st.header("📈 5. Categorization of Regions Based on Performance & Consistency")

# Categorization
q25 = agg["Avg_Critical_Ratio"].quantile(0.25)
# def categorize(row):
#     if pd.isna(row["Avg_Critical_Ratio"]):
#         return "NoData"
#     if row["Avg_Critical_Ratio"] < q25 and row["Composite_Consistency"] > 0.7:
#         return "A: Stable Performer"
#     elif row["Composite_Consistency"] > 0.6:
#         return "B: Improving"
#     elif row["Avg_Critical_Ratio"] < q25:
#         return "C: Volatile"
#     else:
#         return "D: Underperformer"
# # Calculate 25th percentile of Avg_Critical_Ratio
# q25 = agg["Avg_Critical_Ratio"].quantile(0.25)

# thresholds (tweakable)
q25 = final["Avg_Critical_Ratio"].quantile(0.25)  # performance threshold (lower = better)
cons_high = 0.70   # high consistency
cons_low  = 0.60   # medium consistency cutoff

def categorize(row, q25=q25, cons_high=cons_high, cons_low=cons_low):
    """
    Quadrant-based categorization using BOTH Avg_Critical_Ratio and Composite_Consistency.

    - Avg_Critical_Ratio: lower is better (use q25 as "good" cutoff)
    - Composite_Consistency: higher is better

    Quadrants:
      A: Low ratio (good) & high consistency  -> "A: Stable Performer"
      B: Low ratio (good) & low/medium consistency -> "B: Good but Unstable"
      C: High ratio (poor) & high consistency -> "C: Consistent but Poor"
      D: High ratio (poor) & low/medium consistency -> "D: Underperformer"
    """
    ratio = row.get("Avg_Critical_Ratio", None)
    consistency = row.get("Composite_Consistency", None)

    if pd.isna(ratio) or pd.isna(consistency):
        return "NoData"

    low_ratio = ratio < q25
    high_cons = consistency >= cons_high
    med_cons = (consistency >= cons_low) and (consistency < cons_high)
    low_cons = consistency < cons_low

    # Quadrant logic (explicit, non-overlapping)
    if low_ratio and high_cons:
        return "A: Stable Performer"
    elif low_ratio and (med_cons or low_cons):
        return "B: Good but Unstable"
    elif (not low_ratio) and high_cons:
        return "C: Consistent but Poor"
    else:  # (not low_ratio) and (med_cons or low_cons)
        return "D: Underperformer"


# Apply categorization
final["Category"] = final.apply(categorize, axis=1)



st.markdown(f"""
### 🧭 **Categorization Logic**

| **Category** | **Criteria** | **Description** |
|:--------------|:-------------|:----------------|
| 🟢 **A: Stable Performer** | `Avg_Critical_Ratio < {q25:.3f}` **and** `Composite_Consistency ≥ 0.70` | Low REDD Pendency with strong consistency — ideal performance zone. |
| 🟡 **B: Good but Unstable** | `Avg_Critical_Ratio < {q25:.3f}` **and** `Composite_Consistency < 0.70` | Performs well at times but shows volatility — needs stability. |
| 🔵 **C: Consistent but Poor** | `Avg_Critical_Ratio ≥ {q25:.3f}` **and** `Composite_Consistency ≥ 0.70` | Reliable but with higher pendency — process improvement required. |
| 🔴 **D: Underperformer** | `Avg_Critical_Ratio ≥ {q25:.3f}` **and** `Composite_Consistency < 0.70` | Weak on both pendency and consistency — needs focused intervention. |
""")


# --- Color mapping ---
color_map = {
    "A: Stable Performer": "#2ecc71",   # green
    "B: Improving": "#f1c40f",          # yellow
    "C: Volatile": "#e67e22",           # orange
    "D: Underperformer": "#e74c3c",     # red
    "NoData": "#95a5a6"                 # gray
}

# --- Scatter Plot (Quadrant Visualization) ---
fig = px.scatter(
    final,
    x="Avg_Critical_Ratio",
    y="Composite_Consistency",
    color="Category",
    text="Dlv_Region",
    color_discrete_map=color_map,
    size_max=12,
    title="🧭 Region Performance Matrix: Avg REDD today or before pendency Ratio vs Consistency",
)

# Add quadrant reference lines
fig.add_vline(x=q25, line_dash="dash", line_color="gray")
fig.add_hline(y=0.7, line_dash="dot", line_color="gray")
#fig.add_hline(y=0.6, line_dash="dot", line_color="gray")

fig.update_traces(textposition="top center")
fig.update_layout(
    xaxis_title="Avg REDD Today or before/Total Pendency Ratio (Lower = Better)",
    yaxis_title="Composite Consistency (Higher = Better)",
    legend_title_text="Category",
    template="plotly_white",
)

st.plotly_chart(fig, use_container_width=True)

# --- Optional: Summary table ---
st.subheader("📋 Summary by Category")
summary = final.groupby("Category")["Dlv_Region"].count().reset_index().rename(columns={"Dlv_Region": "Count"})
st.dataframe(summary, hide_index=True, use_container_width=True)

st.divider()
st.header("📈 6. Targets")

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
import streamlit as st

# Initial defaults
st.write("### ⚙️ Set Reduction Factor as per Region Performance")

default_map = {"Top": 0.92, "Average": 0.89, "Poor": 0.89}
reduction_map = {}

# Create 3 columns horizontally
cols = st.columns(len(default_map))

for (key, val), col in zip(default_map.items(), cols):
    with col:
        pct = (1 - val) * 100  # convert to %
        new_pct = st.slider(
            f"{key}",
            min_value=0,
            max_value=20,
            value=int(pct),
            step=1,
            format="%d%%"
        )
        reduction_map[key] = 1 - new_pct / 100

#st.divider()
#st.write("### Final Reduction Multipliers:")
#st.json(reduction_map)


#reduction_map = {"Top": 0.95, "Average": 0.90, "Poor": 0.95}
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

col1, col2 = st.columns(2)
col1.metric("Current REDD Today or earlier Ratio", int(final['Critical_Pending'].sum()))
col2.metric("Targeted REDD Today or earlier Ratio", int(final['Target_Critical_Pending'].sum()),str(round(((final['Target_Critical_Pending'].sum()/final['Critical_Pending'].sum())-1)*100,2))+"%" )


st.dataframe(final)

# Reorder cols sensibly for output
out_cols = [
    "Dlv_Region", "Category", "Avg_Critical_Ratio", "Avg_Rank",
    "Composite_Consistency", "Critical_Pending", "Upcoming_Pending", "Total_Pending",
    "Target_Critical_Pending", "Target_Critical_Ratio"
]
# ensure columns exist
out_cols = [c for c in out_cols if c in final.columns]
output = final[out_cols].sort_values("Avg_Critical_Ratio")
st.dataframe(output)

# Save
#output.to_excel("region_performance_summary.xlsx", index=False)
#print("✅ Saved region_performance_summary.xlsx")
#print(output.head(50))

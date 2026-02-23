# app.py
# Streamlit Parts Catalog Mapper (Web App)
#
# Implements the exact logic you specified (no Tkinter).
#
# Run locally:
#   streamlit run app.py
#
# Deps:
#   pip install streamlit pandas openpyxl

import io
import zipfile
from datetime import datetime

import pandas as pd
import streamlit as st


STANDARD_HEADERS = [
    "Supplier",
    "ItemCode",
    "Description",
    "PurchasePrice",
    "SalesPrice",
    "SV_ManufacturerId",
    "ListCategory",
    "MarinaLocationId",
    "AdditionDatetime",
]

DROPDOWN_MAPPED_FIELDS = [
    "Supplier",
    "ItemCode",
    "Description",
    "PurchasePrice",
    "SalesPrice",
    "SV_ManufacturerId",
    "ListCategory",
]


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def is_excel(name: str) -> bool:
    name = (name or "").lower()
    return name.endswith(".xlsx") or name.endswith(".xlsm") or name.endswith(".xls")


def read_uploaded_file(uploaded_file) -> pd.DataFrame:
    """
    Reads Streamlit UploadedFile (CSV or Excel) into a DataFrame.
    Uses dtype=object to preserve strings and avoid inference surprises.
    """
    if uploaded_file is None:
        raise ValueError("No file provided.")

    filename = uploaded_file.name
    if is_excel(filename):
        df = pd.read_excel(uploaded_file, sheet_name=0, dtype=object)
    else:
        # keep_default_na=False keeps empty strings instead of NaN for CSVs
        df = pd.read_csv(uploaded_file, dtype=object, keep_default_na=False)

    df.columns = [str(c).strip() for c in df.columns]
    return df


def normalize_itemcode(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip()


def to_num(series: pd.Series) -> pd.Series:
    """
    Robust numeric conversion:
    - strips commas and whitespace
    - converts blanks/"nan"/"None" -> None
    - pd.to_numeric(..., errors="coerce")
    """
    s = series.astype(str).str.replace(",", "", regex=False).str.strip()
    s = s.replace({"": None, "nan": None, "None": None})
    return pd.to_numeric(s, errors="coerce")


def build_df_mapped(df_source: pd.DataFrame, mapping: dict, marina_location_id: str, addition_dt: str) -> pd.DataFrame:
    """
    Step 1 – Source File Mapping -> df_mapped with STANDARD_HEADERS.
    Dedupes safely by ItemCode (keep last), drops blank ItemCodes.
    """
    out = {}

    for field in STANDARD_HEADERS:
        if field in ("MarinaLocationId", "AdditionDatetime"):
            continue

        src_col = mapping.get(field, "(not mapped)")
        if not src_col or src_col == "(not mapped)":
            out[field] = pd.Series([""] * len(df_source), dtype=object)
        else:
            out[field] = df_source[src_col].astype(object)

    out["MarinaLocationId"] = pd.Series([marina_location_id] * len(df_source), dtype=object)
    out["AdditionDatetime"] = pd.Series([addition_dt] * len(df_source), dtype=object)

    df_mapped = pd.DataFrame(out, columns=STANDARD_HEADERS)

    df_mapped["ItemCode"] = normalize_itemcode(df_mapped["ItemCode"])
    df_mapped = df_mapped[df_mapped["ItemCode"].astype(str).str.strip() != ""].copy()

    # Safe dedupe: keep last occurrence for a given ItemCode
    df_mapped = df_mapped.drop_duplicates(subset=["ItemCode"], keep="last").reset_index(drop=True)

    return df_mapped


def compare_and_build_exports(df_mapped: pd.DataFrame, df_catalog: pd.DataFrame):
    """
    Step 2 – Comparison Logic:
    - Compare ItemCode between df_mapped and df_catalog
    - If exists in both: compare PurchasePrice and SalesPrice; update catalog if different
    - If exists only in catalog: do nothing; keep row as-is
    - If exists only in mapped: add to New Items with required extra columns

    Returns:
        df_catalog_updated (same column order as input df_catalog),
        new_items_df,
        updated_count,
        new_count
    """
    required_catalog_cols = ["ItemCode", "PurchasePrice", "SalesPrice"]
    missing = [c for c in required_catalog_cols if c not in df_catalog.columns]
    if missing:
        raise ValueError(
            "Marina catalog must contain columns: "
            f"{', '.join(required_catalog_cols)}. Missing: {', '.join(missing)}"
        )

    df_catalog_updated = df_catalog.copy()
    original_col_order = df_catalog_updated.columns.tolist()

    df_catalog_updated["ItemCode"] = normalize_itemcode(df_catalog_updated["ItemCode"])
    df_mapped_local = df_mapped.copy()
    df_mapped_local["ItemCode"] = normalize_itemcode(df_mapped_local["ItemCode"])

    # Lookups for safe matching (avoid ambiguous duplicates)
    catalog_lookup = df_catalog_updated.drop_duplicates(subset=["ItemCode"], keep="last").set_index("ItemCode")
    mapped_lookup = df_mapped_local.drop_duplicates(subset=["ItemCode"], keep="last").set_index("ItemCode")

    # Index operations scale better than set/list conversions on very large catalogs.
    both_codes = catalog_lookup.index.intersection(mapped_lookup.index, sort=False)
    new_codes = mapped_lookup.index.difference(catalog_lookup.index, sort=False)

    # Update prices where codes exist in both and prices differ
    updated_count = 0
    codes_to_update = []

    if len(both_codes) > 0:
        catalog_pp = to_num(catalog_lookup.loc[both_codes, "PurchasePrice"])
        catalog_sp = to_num(catalog_lookup.loc[both_codes, "SalesPrice"])
        mapped_pp = to_num(mapped_lookup.loc[both_codes, "PurchasePrice"])
        mapped_sp = to_num(mapped_lookup.loc[both_codes, "SalesPrice"])

        needs_update = (catalog_pp.ne(mapped_pp)) | (catalog_sp.ne(mapped_sp))
        codes_to_update = needs_update[needs_update].index.astype(str).tolist()

        if codes_to_update:
            # Index-aligned assignment avoids large dict/map overhead.
            catalog_indexed = df_catalog_updated.set_index("ItemCode", drop=False)
            catalog_indexed.loc[codes_to_update, "PurchasePrice"] = mapped_lookup.loc[codes_to_update, "PurchasePrice"]
            catalog_indexed.loc[codes_to_update, "SalesPrice"] = mapped_lookup.loc[codes_to_update, "SalesPrice"]
            df_catalog_updated = catalog_indexed.reset_index(drop=True)

            updated_count = len(codes_to_update)

    # Build New Items sheet
    new_items_df = pd.DataFrame()
    if len(new_codes) > 0:
        df_new = mapped_lookup.loc[new_codes].reset_index()

        new_items_df = pd.DataFrame(
            {
                "Id": ["*"] * len(df_new),
                "Supplier": df_new.get("Supplier", ""),
                "ItemCode": df_new.get("ItemCode", ""),
                "Description": df_new.get("Description", ""),
                "PurchasePrice": df_new.get("PurchasePrice", ""),
                "SalesPrice": df_new.get("SalesPrice", ""),
                "SV_ManufacturerId": df_new.get("SV_ManufacturerId", ""),
                "ListCategory": df_new.get("ListCategory", ""),
                # Insert RecordStatusId between ListCategory and MarinaLocationId
                "RecordStatusId": [1] * len(df_new),
                "MarinaLocationId": df_new.get("MarinaLocationId", ""),
                "AdditionDatetime": df_new.get("AdditionDatetime", ""),
                # Append after AdditionDatetime
                "ItemMaster_Id": ["NULL"] * len(df_new),
                "AspNetUser_Id": ["NULL"] * len(df_new),
                "SupersededItemCode": ["NULL"] * len(df_new),
            }
        )

    # Ensure column order is preserved in Catalog export
    df_catalog_updated = df_catalog_updated.reindex(columns=original_col_order)

    return df_catalog_updated, new_items_df, updated_count, len(new_codes)


def make_excel_bytes(df_export: pd.DataFrame, sheet_name: str) -> bytes:
    """
    Writes a single DataFrame to an in-memory Excel file using openpyxl.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_export.to_excel(writer, index=False, sheet_name=sheet_name)
    output.seek(0)
    return output.read()


# ---------------------------
# Streamlit UI
# ---------------------------

st.set_page_config(page_title="Parts Catalog Mapper", layout="wide")
st.title("Parts Catalog Mapper (Web)")

st.caption(
    "Upload a **Source** file, map columns into the standardized output, then upload the **Marina Catalog** and export an Excel with updates + a **New Items** sheet."
)

# Initialize session state
if "df_source" not in st.session_state:
    st.session_state.df_source = None
if "df_catalog" not in st.session_state:
    st.session_state.df_catalog = None
if "df_mapped" not in st.session_state:
    st.session_state.df_mapped = None
if "mapping" not in st.session_state:
    st.session_state.mapping = {f: "(not mapped)" for f in DROPDOWN_MAPPED_FIELDS}
if "addition_dt" not in st.session_state:
    st.session_state.addition_dt = now_stamp()
if "marina_location_id" not in st.session_state:
    st.session_state.marina_location_id = ""


with st.sidebar:
    st.header("Inputs")
    st.session_state.marina_location_id = st.text_input(
        "MarinaLocationId (manual)", value=st.session_state.marina_location_id, placeholder="e.g., 123"
    )

    colA, colB = st.columns([2, 1])
    with colA:
        st.text_input("AdditionDatetime (generated)", value=st.session_state.addition_dt, disabled=True)
    with colB:
        if st.button("Refresh", use_container_width=True):
            st.session_state.addition_dt = now_stamp()
            st.success("Timestamp refreshed.")

    st.divider()
    st.header("Step 1 — Upload Source")
    source_upload = st.file_uploader("Source file (CSV/XLSX)", type=["csv", "xlsx", "xlsm", "xls"])

    if source_upload is not None:
        try:
            df_source = read_uploaded_file(source_upload)
            st.session_state.df_source = df_source
            st.success(f"Loaded Source: {source_upload.name} | Rows: {len(df_source)} | Cols: {len(df_source.columns)}")
        except Exception as e:
            st.session_state.df_source = None
            st.error(f"Could not read Source file: {e}")

    st.divider()
    st.header("Step 2 — Upload Marina Catalog")
    catalog_upload = st.file_uploader("Marina catalog (CSV/XLSX)", type=["csv", "xlsx", "xlsm", "xls"], key="catalog")

    if catalog_upload is not None:
        try:
            df_catalog = read_uploaded_file(catalog_upload)
            st.session_state.df_catalog = df_catalog
            st.success(
                f"Loaded Marina Catalog: {catalog_upload.name} | Rows: {len(df_catalog)} | Cols: {len(df_catalog.columns)}"
            )
            st.info("Confirmation: Marina catalog loaded.")
        except Exception as e:
            st.session_state.df_catalog = None
            st.error(f"Could not read Marina Catalog file: {e}")


# Main layout
left, right = st.columns([1.1, 0.9], gap="large")

with left:
    st.subheader("Step 1 — Source Column Mapping")

    df_source = st.session_state.df_source
    if df_source is None:
        st.warning("Upload a Source file to configure mappings.")
    else:
        source_cols = ["(not mapped)"] + df_source.columns.tolist()

        # Mapping dropdowns
        for field in DROPDOWN_MAPPED_FIELDS:
            default = st.session_state.mapping.get(field, "(not mapped)")
            # ensure default exists in options
            if default not in source_cols:
                default = "(not mapped)"
                st.session_state.mapping[field] = default

            chosen = st.selectbox(
                f"{field}",
                options=source_cols,
                index=source_cols.index(default),
                key=f"map_{field}",
            )
            st.session_state.mapping[field] = chosen

        st.caption("MarinaLocationId is taken from the sidebar input. AdditionDatetime is generated (sidebar).")

        # Build df_mapped
        if st.button("Create Mapped Fields", type="primary", use_container_width=True):
            if st.session_state.mapping.get("ItemCode", "(not mapped)") == "(not mapped)":
                st.error("ItemCode must be mapped to proceed.")
            else:
                try:
                    mapping_payload = dict(st.session_state.mapping)
                    df_mapped = build_df_mapped(
                        df_source=df_source,
                        mapping=mapping_payload,
                        marina_location_id=(st.session_state.marina_location_id or "").strip(),
                        addition_dt=(st.session_state.addition_dt or "").strip() or now_stamp(),
                    )
                    st.session_state.df_mapped = df_mapped
                    st.success(f"df_mapped created. Rows: {len(df_mapped)} (deduped by ItemCode).")
                except Exception as e:
                    st.error(f"Failed to create df_mapped: {e}")

        # Preview df_source + df_mapped
        st.divider()
        st.write("**Source preview (first 50 rows)**")
        st.dataframe(df_source.head(50), use_container_width=True)

        if st.session_state.df_mapped is not None:
            st.write("**df_mapped preview (first 50 rows)**")
            st.dataframe(st.session_state.df_mapped.head(50), use_container_width=True)

with right:
    st.subheader("Step 2 — Compare + Export")

    df_catalog = st.session_state.df_catalog
    df_mapped = st.session_state.df_mapped

    if df_catalog is None:
        st.warning("Upload a Marina catalog file to enable export.")
    else:
        st.write("**Marina Catalog preview (first 50 rows)**")
        st.dataframe(df_catalog.head(50), use_container_width=True)

    st.divider()

    if df_mapped is None:
        st.warning("Create Mapped Fields first (Step 1).")
    elif df_catalog is None:
        st.warning("Upload Marina catalog first (Step 2).")
    else:
        # Export button
        if st.button("Export Updated Catalog + New Items", type="primary", use_container_width=True):
            try:
                df_catalog_updated, new_items_df, updated_count, new_count = compare_and_build_exports(
                    df_mapped=df_mapped,
                    df_catalog=df_catalog,
                )

                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                catalog_filename = f"updated_catalog_{ts}.xlsx"
                new_items_filename = f"new_items_{ts}.xlsx"
                zip_filename = f"catalog_exports_{ts}.zip"

                catalog_xlsx_bytes = make_excel_bytes(df_catalog_updated, "Catalog")
                new_items_export_df = new_items_df if new_items_df is not None else pd.DataFrame()
                new_items_xlsx_bytes = make_excel_bytes(new_items_export_df, "New Items")

                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.writestr(catalog_filename, catalog_xlsx_bytes)
                    zf.writestr(new_items_filename, new_items_xlsx_bytes)
                zip_buffer.seek(0)
                zip_bytes = zip_buffer.read()

                st.success(f"Export ready. Updated items: {updated_count} | New items: {new_count}")

                st.download_button(
                    label="Download Both Files (ZIP)",
                    data=zip_bytes,
                    file_name=zip_filename,
                    mime="application/zip",
                    use_container_width=True,
                )

                with st.expander("Details", expanded=False):
                    st.write("**Updated ItemCodes (price changes)**")
                    # We can recompute codes_to_update quickly for display, without changing export logic
                    # Display is optional; keep it safe and small
                    st.write(f"Updated count: {updated_count}")
                    st.write(f"New items count: {new_count}")

                    st.write("**New Items preview (first 50 rows)**")
                    st.dataframe((new_items_df if new_items_df is not None else pd.DataFrame()).head(50), use_container_width=True)

            except Exception as e:
                st.error(f"Export failed: {e}")

st.divider()
st.caption(
    "Notes: This webapp intentionally **does not** use Tkinter. It reads CSV/XLSX, maps to standardized headers, "
    "updates catalog prices without changing column order, and creates a 'New Items' sheet per your rules."
)

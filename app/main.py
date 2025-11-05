from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import io, os, base64

API_TOKEN = os.getenv("API_TOKEN", "supersecrettoken")
app = FastAPI(title="XLSX Aggregator API", docs_url=None, redoc_url=None)


@app.get("/")
def root():
    return {"message": "XLSX Aggregator API is running"}


@app.post("/aggregate")
async def aggregate_json(
    authorization: str = Header(...),
    x_group_by: str = Header(None),
    body: dict = None
):
    # --- Authorization ---
    if authorization != f"Bearer {API_TOKEN}":
        raise HTTPException(status_code=401, detail="Invalid API token")
    if body is None or "file_b64" not in body:
        raise HTTPException(status_code=400, detail="Missing file_b64 in body")

    # --- Decode ---
    try:
        decoded = base64.b64decode(body["file_b64"])
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid base64 content")

    # --- Detect XLS vs XLSX ---
    excel_bytes = io.BytesIO(decoded)
    start_bytes = excel_bytes.read(8)
    excel_bytes.seek(0)

    if start_bytes[:2] == b"PK":
        engine = "openpyxl"  # XLSX (zip)
    else:
        engine = "xlrd"      # Legacy XLS

    # --- Try reading with multiple header offsets ---
    df = None
    try:
        for h in [0, 1, 2, 3, 4, 5]:
            try:
                temp = pd.read_excel(excel_bytes, engine=engine, header=h)
                excel_bytes.seek(0)
                cols = [str(c).strip() for c in temp.columns]
                if any(k in " ".join(cols) for k in ["AdmNo", "URN", "CurrWardUnit", "Disch Unit"]):
                    df = temp
                    break
            except Exception:
                excel_bytes.seek(0)
                continue
        if df is None:
            raise ValueError("Could not locate header row")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading XLS/XLSX: {e}")

    # --- Clean up columns ---
    df.columns = [str(c).strip() for c in df.columns]
    df.dropna(how="all", inplace=True)

    # --- Identify Report Type ---
    if "URN" in df.columns:
        report_type = "Inpatients"
    elif "Disch Unit" in df.columns:
        report_type = "Deceased"
    elif "CurrWardUnit" in df.columns:
        report_type = "Transfers"
    else:
        report_type = "Unknown"

    # --- Normalise + Grouping ---
    if report_type == "Inpatients":
        df = df[df["Ward"].notna()]
        group_cols = ["Ward", "Unit"]
        count_col = "URN"

    elif report_type == "Deceased":
        df = df[df["AdmNo"].notna()]
        df["Unit"] = df["Disch Unit"].astype(str)
        df["Ward"] = None
        group_cols = ["Unit"]
        count_col = "AdmNo"

    elif report_type == "Transfers":
        df = df[df["CurrWardUnit"].notna()]
        ward_unit = df["CurrWardUnit"].astype(str).str.split(" ", n=1, expand=True)
        df["Ward"] = ward_unit[0].str.strip()
        df["Unit"] = ward_unit[1].str.strip()
        group_cols = ["Ward", "Unit"]
        count_col = "AdmNo"

    else:
        if not x_group_by:
            raise HTTPException(status_code=400, detail="Unknown format and no x-group-by provided")
        group_cols = [x.strip() for x in x_group_by.split(",")]
        count_col = df.columns[0]

    # --- Clean up count column ---
    df[count_col] = df[count_col].astype(str)
    df = df[df[count_col].str.strip() != ""]

    # --- Aggregate ---
    try:
        agg = (
            df.groupby(group_cols)[count_col]
            .nunique()
            .reset_index(name="Count")
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error aggregating data: {e}")

    # --- Normalize columns for output ---
    if "Ward" not in agg.columns:
        agg["Ward"] = None
    if "Unit" not in agg.columns:
        agg["Unit"] = None
    agg = agg[["Ward", "Unit", "Count"]]

    return JSONResponse(
        content={
            "report_type": report_type,
            "data": agg.to_dict(orient="records")
        }
    )

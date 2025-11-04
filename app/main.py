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
    x_group_by: str = Header(...),
    body: dict = None
):
    if authorization != f"Bearer {API_TOKEN}":
        raise HTTPException(status_code=401, detail="Invalid API token")
    if body is None or "file_b64" not in body:
        raise HTTPException(status_code=400, detail="Missing file_b64 in body")

    # Decode and inspect Excel
    try:
        decoded = base64.b64decode(body["file_b64"])
        # Try reading different possible header offsets
        candidates = [0, 1, 2, 3, 4, 5]
        df = None
        for h in candidates:
            temp = pd.read_excel(io.BytesIO(decoded), engine="openpyxl", header=h)
            if any("AdmNo" in str(c) or "Adm No" in str(c) for c in temp.columns):
                df = temp
                break
        if df is None:
            raise ValueError("Could not locate header row")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading XLSX: {e}")

    # Drop footer rows (where AdmNo is NaN or not numeric)
    if "AdmNo" in df.columns:
        df = df[df["AdmNo"].notna()]

    # --- Normalise known column variations ---
    colmap = {c.strip(): c.strip() for c in df.columns}
    df.rename(columns=colmap, inplace=True)

    # Handle specific formats
    if "Disch Unit" in df.columns:  # deceased patients
        df["Unit"] = df["Disch Unit"].astype(str)
        group_cols = ["Unit"]
        count_col = "AdmNo"

    elif "CurrWardUnit" in df.columns:  # transfer report
        # Split "CurrWardUnit" into Ward + Unit
        ward_unit = df["CurrWardUnit"].astype(str).str.split(" ", n=1, expand=True)
        df["Ward"] = ward_unit[0]
        df["Unit"] = ward_unit[1]
        group_cols = ["Ward", "Unit"]
        count_col = "AdmNo"

    else:
        # Fallback to caller's requested columns
        group_cols = [c.strip() for c in x_group_by.split(",")]
        count_col = "URN" if "URN" in df.columns else df.columns[0]

    # --- Aggregate ---
    try:
        agg = (
            df.groupby(group_cols)[count_col]
            .nunique()
            .reset_index(name="Count")
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error aggregating data: {e}")

    return JSONResponse(content={"data": agg.to_dict(orient="records")})

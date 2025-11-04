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
    # --- Auth & input validation ---
    if authorization != f"Bearer {API_TOKEN}":
        raise HTTPException(status_code=401, detail="Invalid API token")
    if body is None or "file_b64" not in body:
        raise HTTPException(status_code=400, detail="Missing file_b64 in body")

    # --- Decode & read Excel ---
    try:
        decoded = base64.b64decode(body["file_b64"])
        df = None

        # Try normal read first (Inpatients layout)
        try:
            df = pd.read_excel(io.BytesIO(decoded), engine="openpyxl", header=0)
        except Exception:
            df = None

        # If that fails or columns don't look right, scan alternative header rows
        if df is None or not any(
            key in " ".join([str(c) for c in df.columns])
            for key in ["Ward", "Unit", "CurrWardUnit", "Disch Unit", "AdmNo", "URN"]
        ):
            for h in range(1, 10):
                try:
                    temp = pd.read_excel(io.BytesIO(decoded), engine="openpyxl", header=h)
                    if any(
                        key in " ".join([str(c) for c in temp.columns])
                        for key in ["Ward", "Unit", "CurrWardUnit", "Disch Unit", "AdmNo", "URN"]
                    ):
                        df = temp
                        break
                except Exception:
                    continue

        if df is None:
            raise ValueError("Could not locate header row")

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading XLSX: {e}")

    # --- Clean & normalise ---
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]
    id_cols = [c for c in df.columns if "Adm" in c or "URN" in c]
    if id_cols:
        df = df[df[id_cols[0]].notna()]

    # --- Determine report type ---
    if "Disch Unit" in df.columns:
        # Deceased
        df["Unit"] = df["Disch Unit"].astype(str)
        group_cols = ["Unit"]
        count_col = "AdmNo" if "AdmNo" in df.columns else df.columns[0]

    elif "CurrWardUnit" in df.columns:
        # Transfers
        ward_unit = df["CurrWardUnit"].astype(str).str.split(" ", n=1, expand=True)
        df["Ward"] = ward_unit[0]
        df["Unit"] = ward_unit[1]
        group_cols = ["Ward", "Unit"]
        count_col = "AdmNo" if "AdmNo" in df.columns else df.columns[0]

    else:
        # Inpatients (headers at row 1)
        group_cols = [c.strip() for c in x_group_by.split(",") if c.strip()]
        count_col = (
            "URN"
            if "URN" in df.columns
            else "AdmNo"
            if "AdmNo" in df.columns
            else df.columns[0]
        )

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

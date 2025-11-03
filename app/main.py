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
    # --- Auth check ---
    if authorization != f"Bearer {API_TOKEN}":
        raise HTTPException(status_code=401, detail="Invalid API token")

    if body is None or "file_b64" not in body:
        raise HTTPException(status_code=400, detail="Missing file_b64 in body")

    # --- Decode and read Excel ---
    try:
        decoded = base64.b64decode(body["file_b64"])
        df = pd.read_excel(io.BytesIO(decoded), engine="openpyxl")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading XLSX: {e}")

    # --- Group and aggregate ---
    group_cols = [c.strip() for c in x_group_by.split(",")]
    for c in group_cols:
        if c not in df.columns:
            raise HTTPException(status_code=400, detail=f"Column '{c}' not found in XLSX")

    try:
        agg = df.groupby(group_cols).agg({"URN": "count"}).reset_index()
        agg.rename(columns={"URN": "Count"}, inplace=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error aggregating data: {e}")

    return JSONResponse(content={"data": agg.to_dict(orient="records")})

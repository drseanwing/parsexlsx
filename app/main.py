from fastapi import FastAPI, File, UploadFile, Header, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import io
import os
from app.utils import aggregate_xlsx

app = FastAPI(title="XLSX Aggregator API")

API_TOKEN = os.getenv("API_TOKEN", "supersecrettoken")

@app.get("/")
def root():
    return {"message": "XLSX Aggregator API is running"}

@app.put("/aggregate")
async def aggregate(
    file: UploadFile = File(...),
    authorization: str = Header(...),
    x_group_by: str = Header(...),
):
    if authorization != f"Bearer {API_TOKEN}":
        raise HTTPException(status_code=401, detail="Invalid API token")

    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file type")

    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents), engine="openpyxl")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading XLSX: {str(e)}")

    if df.empty:
        raise HTTPException(status_code=400, detail="The uploaded XLSX file is empty")

    try:
        group_cols = [col.strip() for col in x_group_by.split(",") if col.strip()]
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid X-Group-By header")

    missing_cols = [c for c in group_cols if c not in df.columns]
    if missing_cols:
        raise HTTPException(status_code=400, detail=f"Missing columns: {missing_cols}")

    try:
        result = aggregate_xlsx(df, group_cols)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Aggregation failed: {str(e)}")

    return JSONResponse(content={"data": result})

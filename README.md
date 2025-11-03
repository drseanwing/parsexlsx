# XLSX Aggregator API

A lightweight FastAPI service for aggregating Excel data by any set of columns.

## Build & Run

```bash
docker-compose up --build -d
```

The service will be available at http://localhost:8000.

## Authentication

All requests must include a header:

```
Authorization: Bearer supersecrettoken
```

## Example Request

```bash
curl -X PUT http://localhost:8000/aggregate   -H 'Authorization: Bearer supersecrettoken'   -H 'X-Group-By: Ward,Unit'   -F 'file=@sample.xlsx'
```

## Example Response

```json
{
  "data": [
    {"Ward": "3B", "Unit": "Cardiology", "Count": 12},
    {"Ward": "2A", "Unit": "Oncology", "Count": 8}
  ]
}
```

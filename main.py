import duckdb
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

app = FastAPI()

print("⏳ Initializing DuckDB for Render...")
db = duckdb.connect()
# Render free tier only has 512MB RAM. 256MB is the safe limit.
db.execute("SET memory_limit='256MB';")
db.execute("SET threads=1;") 
db.execute("INSTALL httpfs; LOAD httpfs;")

# Using the exact URL that worked for you
BASE_URL = "https://huggingface.co/buckets/CutehackX/icrm-hitek-full-db-mixed-bucket/resolve/"
phone_urls = [f"{BASE_URL}idx_phone.{i}.parquet" for i in range(7)]
phone_urls_str = str(phone_urls).replace("'", '"')

try:
    db.execute(f"CREATE VIEW people_phone AS SELECT * FROM read_parquet({phone_urls_str})")
    print("✅ View created successfully. Ready to query.")
except Exception as e:
    print(f"❌ Error creating view: {e}")

# Helper function to convert DuckDB cursor to list of dictionaries (Replaces pandas)
def fetch_dicts(cursor):
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

@app.get("/")
def health():
    return {"status": "alive"}

@app.get("/search")
def search(q: str = Query(...)):
    try:
        q = q.strip()
        results = []
        
        if q.isdigit():
            # Search by phone or aadhar (Using Parameterized Query for Security)
            query = """
                SELECT name, fathersName, phoneNumber, aadharNumber, otherNumber, address, district, pincode, state, town 
                FROM people_phone 
                WHERE phoneNumber = ? OR aadharNumber = ? 
                LIMIT 5
            """
            cur = db.execute(query, [q, q])
        else:
            # Search by name (Using Parameterized Query for Security)
            query = """
                SELECT name, fathersName, phoneNumber, aadharNumber, otherNumber, address, district, pincode, state, town 
                FROM people_phone 
                WHERE name ILIKE ? 
                LIMIT 5
            """
            cur = db.execute(query, [f"%{q}%"])

        results = fetch_dicts(cur)

        # DuckDB natively returns None for NULL/NaN values, which FastAPI serializes perfectly.
        # We only need to ensure empty strings or weird values are cleaned up.
        clean_results = []
        for row in results:
            clean_row = {k: (None if str(v).lower() == 'nan' else v) for k, v in row.items()}
            clean_results.append(clean_row)

        return {"query": q, "count": len(clean_results), "results": clean_results}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

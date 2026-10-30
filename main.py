import duckdb
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

app = FastAPI()

print("⏳ Initializing DuckDB for Render...")

# 🔴 FIX: Set the custom user-agent when creating the connection
db = duckdb.connect(config={
    'custom_user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
})

# Render memory limits
db.execute("SET memory_limit='256MB';")
db.execute("SET threads=1;")
db.execute("INSTALL httpfs; LOAD httpfs;")

BASE_URL = "https://huggingface.co/buckets/CutehackX/icrm-hitek-full-db-mixed-bucket/resolve/"
phone_urls = [f"{BASE_URL}idx_phone.{i}.parquet" for i in range(7)]
phone_urls_str = str(phone_urls).replace("'", '"')

try:
    db.execute(f"CREATE VIEW people_phone AS SELECT * FROM read_parquet({phone_urls_str})")
    print("✅ View created successfully. Ready to query.")
except Exception as e:
    print(f"❌ Error creating view: {e}")

# Helper to replace pandas and save memory
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
            # Parameterized query (Stops SQL injection)
            query = """
                SELECT name, fathersName, phoneNumber, aadharNumber, otherNumber, address, district, pincode, state, town 
                FROM people_phone 
                WHERE phoneNumber = ? OR aadharNumber = ? 
                LIMIT 5
            """
            results = fetch_dicts(db.execute(query, [q, q]))
        else:
            # Parameterized query (Stops SQL injection)
            query = """
                SELECT name, fathersName, phoneNumber, aadharNumber, otherNumber, address, district, pincode, state, town 
                FROM people_phone 
                WHERE name ILIKE ? 
                LIMIT 5
            """
            results = fetch_dicts(db.execute(query, [f"%{q}%"]))

        # Clean up NaN values for JSON serialization
        clean_results = []
        for row in results:
            clean_row = {k: (None if str(v) == 'nan' else v) for k, v in row.items()}
            clean_results.append(clean_row)

        return {"query": q, "count": len(clean_results), "results": clean_results}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

import struct
import pyodbc
import subprocess
import sys

server = "nkhahdl5to4ezo6p5bg76flepa-qapzded7q26enlrro3xmofm5oq.datawarehouse.fabric.microsoft.com"
database = "healthcare1_reporting_gold"

def get_access_token() -> str:
    cmd = ["az", "account", "get-access-token", "--resource", "https://database.windows.net", "--query", "accessToken", "-o", "tsv"]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise Exception(f"Failed to get database access token: {res.stderr}")
    return res.stdout.strip()

def main():
    print(f"Connecting to Database: {database} ...")
    try:
        token = get_access_token()
    except Exception as e:
        print(f"Token error: {e}")
        sys.exit(1)
        
    token_bytes = token.encode("UTF-16-LE")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Encrypt=Yes;"
            f"Connection Timeout=15;",
            attrs_before={1256: token_struct},
        )
        print("Connected successfully!")
    except Exception as e:
        print(f"Failed to connect: {e}")
        sys.exit(1)

    cursor = conn.cursor()
    
    # List all tables and row counts
    try:
        cursor.execute("""
            SELECT s.name, t.name 
            FROM sys.tables t 
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'dbo'
        """)
        tables = cursor.fetchall()
        print(f"Found {len(tables)} tables in Reporting Gold:")
        for s_name, t_name in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM [{s_name}].[{t_name}]")
                count = cursor.fetchone()[0]
                print(f"  Table: {s_name}.{t_name:40} | Row count: {count}")
            except Exception as e:
                print(f"  Table: {s_name}.{t_name:40} | Error: {e}")
    except Exception as e:
        print(f"Error querying tables: {e}")
        
    conn.close()

if __name__ == "__main__":
    main()

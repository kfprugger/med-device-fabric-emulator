import struct
import pyodbc
import subprocess
import sys

server = "nkhahdl5to4ezo6p5bg76flepa-qapzded7q26enlrro3xmofm5oq.datawarehouse.fabric.microsoft.com"
database = "healthcare1_msft_admin"

def get_access_token() -> str:
    cmd = ["az", "account", "get-access-token", "--resource", "https://database.windows.net", "--query", "accessToken", "-o", "tsv"]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise Exception(f"Failed to get database access token: {res.stderr}")
    return res.stdout.strip()

def main():
    token = get_access_token()
    token_bytes = token.encode("UTF-16-LE")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Encrypt=Yes;"
        f"Connection Timeout=15;",
        attrs_before={1256: token_struct},
    )
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM dbo.ExecutionSummary")
    cols = [column[0] for column in cursor.description]
    print("Columns:", cols)
    rows = cursor.fetchall()
    for row in rows:
        print("\n--- Row ---")
        for col, val in zip(cols, row):
            print(f"  {col}: {val}")
            
    conn.close()

if __name__ == "__main__":
    main()

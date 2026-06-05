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
    
    # Query BusinessEvents
    try:
        cursor.execute("SELECT TOP 20 * FROM [dbo].[BusinessEvents]")
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        print(f"\nFound {len(rows)} recent events in BusinessEvents:")
        for row in rows:
            row_dict = dict(zip(columns, row))
            print(f"\n- Event: {row_dict.get('eventName')} | Time: {row_dict.get('msftCreatedDatetime')}")
            print(f"  Description: {row_dict.get('eventDescription')}")
            print(f"  PipelineInfo: {row_dict.get('pipelineInfo')}")
    except Exception as e:
        print(f"Error querying BusinessEvents: {e}")
        
    conn.close()

if __name__ == "__main__":
    main()

import pyodbc

def get_connection():
    server = 'WIN-0M0TINRF89U'
    database = 'AdventureWorks2014'
    username = 'testsapysqlAdventure'
    password = ''

    conn_str = (
        "DRIVER={ODBC DRIVER 17 FOR SQL SERVER};"
        f"SERVER={server};" 
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )

    return pyodbc.connect(conn_str)

if __name__ == "__main__":
    try:
        conn = get_connection()
        print("Success - Connect!")
        conn.close()
    except Exception as e:
        print("Error - Not Connect!", e)
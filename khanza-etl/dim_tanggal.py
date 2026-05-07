import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_tanggal():
    source_conn = None
    target_conn = None
    
    try:
        config = load_config()
        source_conn = get_connection(config['source_config'])
        target_conn = get_connection(config['target_config'])
        
        source_cursor = source_conn.cursor(dictionary=True)
        target_cursor = target_conn.cursor(dictionary=True, buffered=True)

        source_cursor.execute("SELECT DISTINCT tgl_registrasi FROM reg_periksa")
        rows = source_cursor.fetchall()

        inserted_rows = []

        for row in rows:
            target_cursor.execute(
                "SELECT tanggal FROM dim_tanggal WHERE tanggal = %s",
                (row['tgl_registrasi'],)
            )
            existing = target_cursor.fetchone()

            if not existing:
                query = "INSERT INTO dim_tanggal (tanggal) VALUES (%s)"
                target_cursor.execute(query, (row['tgl_registrasi'],))
                inserted_rows.append(str(row['tgl_registrasi']))

        target_conn.commit()

        if inserted_rows:
            print(f"Update: {len(inserted_rows)} baris baru di dim_tanggal.")
        else:
            print("Status: Tidak ada data baru.")

    except (Error, FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Failure: {e}")

    finally:
        if source_conn and source_conn.is_connected():
            source_cursor.close()
            source_conn.close()
        if target_conn and target_conn.is_connected():
            target_cursor.close()
            target_conn.close()

if __name__ == "__main__":
    sync_tanggal()
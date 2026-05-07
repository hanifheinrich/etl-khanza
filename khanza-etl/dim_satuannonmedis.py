import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_satuan_non_medis():
    source_conn = None
    target_conn = None
    
    try:
        config = load_config()
        source_conn = get_connection(config['source_config'])
        target_conn = get_connection(config['target_config'])
        
        source_cursor = source_conn.cursor(dictionary=True)
        target_cursor = target_conn.cursor(dictionary=True)

        source_cursor.execute("SELECT kode_sat, satuan FROM kodesatuan")
        rows = source_cursor.fetchall()

        changed_rows = []

        for row in rows:
            target_cursor.execute(
                "SELECT satuan FROM dim_satuan_non_medis WHERE kode_sat = %s",
                (row['kode_sat'],)
            )
            existing = target_cursor.fetchone()

            if not existing or existing['satuan'] != row['satuan']:
                query = """
                    INSERT INTO dim_satuan_non_medis (kode_sat, satuan)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                        satuan = VALUES(satuan)
                """
                target_cursor.execute(query, (row['kode_sat'], row['satuan']))
                changed_rows.append(row['kode_sat'])

        target_conn.commit()

        if changed_rows:
            print(f"Update: {len(changed_rows)} baris di dim_satuan_non_medis.")
        else:
            print("Status: Tidak ada perubahan.")

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
    sync_satuan_non_medis()
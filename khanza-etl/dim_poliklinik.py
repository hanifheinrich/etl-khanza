import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_poliklinik():
    source_conn = None
    target_conn = None
    
    try:
        config = load_config()
        source_conn = get_connection(config['source_config'])
        target_conn = get_connection(config['target_config'])
        
        source_cursor = source_conn.cursor(dictionary=True)
        target_cursor = target_conn.cursor(dictionary=True)

        source_cursor.execute("""
            SELECT kd_poli, nm_poli, CAST(status AS CHAR) AS status 
            FROM poliklinik
        """)
        rows = source_cursor.fetchall()

        changed_rows = []

        for row in rows:
            target_cursor.execute("""
                SELECT nm_poli, status 
                FROM dim_poliklinik 
                WHERE kd_poli = %s
            """, (row['kd_poli'],))
            existing = target_cursor.fetchone()

            if (not existing or 
                existing['nm_poli'] != row['nm_poli'] or 
                existing['status'] != row['status']):

                query = """
                    INSERT INTO dim_poliklinik (kd_poli, nm_poli, status)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        nm_poli = VALUES(nm_poli),
                        status = VALUES(status)
                """
                target_cursor.execute(query, (row['kd_poli'], row['nm_poli'], row['status']))
                changed_rows.append(row['kd_poli'])

        target_conn.commit()

        if changed_rows:
            print(f"Update: {len(changed_rows)} baris di dim_poliklinik.")
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
    sync_poliklinik()
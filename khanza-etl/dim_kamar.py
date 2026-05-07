import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_kamar():
    source_conn = None
    target_conn = None
    
    try:
        config = load_config()
        source_conn = get_connection(config['source_config'])
        target_conn = get_connection(config['target_config'])
        
        source_cursor = source_conn.cursor(dictionary=True)
        target_cursor = target_conn.cursor(dictionary=True)

        source_cursor.execute("""
            SELECT kd_kamar, kd_bangsal, kelas, statusdata 
            FROM kamar
        """)
        rows = source_cursor.fetchall()

        changed_rows = []

        for row in rows:
            target_cursor.execute("""
                SELECT kd_bangsal, kelas, statusdata 
                FROM dim_kamar 
                WHERE kd_kamar = %s
            """, (row['kd_kamar'],))
            existing = target_cursor.fetchone()

            if (not existing or 
                existing['kd_bangsal'] != row['kd_bangsal'] or 
                existing['kelas'] != row['kelas'] or 
                existing['statusdata'] != row['statusdata']):

                query = """
                    INSERT INTO dim_kamar (kd_kamar, kd_bangsal, kelas, statusdata)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        kd_bangsal = VALUES(kd_bangsal),
                        kelas = VALUES(kelas),
                        statusdata = VALUES(statusdata)
                """
                target_cursor.execute(query, (
                    row['kd_kamar'], 
                    row['kd_bangsal'], 
                    row['kelas'], 
                    row['statusdata']
                ))
                changed_rows.append(row['kd_kamar'])

        target_conn.commit()

        if changed_rows:
            print(f"Update: {len(changed_rows)} baris di dim_kamar.")
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
    sync_kamar()
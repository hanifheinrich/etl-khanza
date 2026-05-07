import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_icd10():
    source_conn = None
    target_conn = None
    
    try:
        config = load_config()
        source_conn = get_connection(config['source_config'])
        target_conn = get_connection(config['target_config'])
        
        source_cursor = source_conn.cursor(dictionary=True)
        target_cursor = target_conn.cursor(dictionary=True)

        source_cursor.execute("""
            SELECT kd_penyakit, nm_penyakit, status 
            FROM penyakit
        """)
        rows = source_cursor.fetchall()

        changed_rows = []

        for row in rows:
            target_cursor.execute("""
                SELECT nm_penyakit, status 
                FROM dim_icd10 
                WHERE kd_penyakit = %s
            """, (row['kd_penyakit'],))
            existing = target_cursor.fetchone()

            if (not existing or 
                existing['nm_penyakit'] != row['nm_penyakit'] or 
                existing['status'] != row['status']):

                query = """
                    INSERT INTO dim_icd10 (
                        kd_penyakit, nm_penyakit, status, 
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, NOW(), NOW())
                    ON DUPLICATE KEY UPDATE
                        nm_penyakit = VALUES(nm_penyakit),
                        status = VALUES(status),
                        updated_at = NOW()
                """
                target_cursor.execute(query, (
                    row['kd_penyakit'], 
                    row['nm_penyakit'], 
                    row['status']
                ))
                changed_rows.append(row['kd_penyakit'])

        target_conn.commit()

        if changed_rows:
            print(f"Update: {len(changed_rows)} baris di dim_icd10.")
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
    sync_icd10()
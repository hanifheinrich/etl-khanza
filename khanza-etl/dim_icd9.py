import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_icd9():
    source_conn = None
    target_conn = None
    
    try:
        config = load_config()
        source_conn = get_connection(config['source_config'])
        target_conn = get_connection(config['target_config'])
        
        source_cursor = source_conn.cursor(dictionary=True)
        target_cursor = target_conn.cursor(dictionary=True)

        source_cursor.execute("""
            SELECT kode, deskripsi_panjang, deskripsi_pendek 
            FROM icd9
        """)
        rows = source_cursor.fetchall()

        changed_rows = []

        for row in rows:
            target_cursor.execute("""
                SELECT deskripsi_panjang, deskripsi_pendek 
                FROM dim_icd9 
                WHERE kode = %s
            """, (row['kode'],))
            existing = target_cursor.fetchone()

            if (not existing or 
                existing['deskripsi_panjang'] != row['deskripsi_panjang'] or 
                existing['deskripsi_pendek'] != row['deskripsi_pendek']):

                query = """
                    INSERT INTO dim_icd9 (
                        kode, deskripsi_panjang, deskripsi_pendek, 
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, NOW(), NOW())
                    ON DUPLICATE KEY UPDATE
                        deskripsi_panjang = VALUES(deskripsi_panjang),
                        deskripsi_pendek = VALUES(deskripsi_pendek),
                        updated_at = NOW()
                """
                target_cursor.execute(query, (
                    row['kode'], 
                    row['deskripsi_panjang'], 
                    row['deskripsi_pendek']
                ))
                changed_rows.append(row['kode'])

        target_conn.commit()

        if changed_rows:
            print(f"Update: {len(changed_rows)} baris di dim_icd9.")
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
    sync_icd9()
import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_ipsrs_barang():
    source_conn = None
    target_conn = None
    
    try:
        config = load_config()
        source_conn = get_connection(config['source_config'])
        target_conn = get_connection(config['target_config'])
        
        source_cursor = source_conn.cursor(dictionary=True)
        target_cursor = target_conn.cursor(dictionary=True)

        source_cursor.execute("""
            SELECT kode_brng, nama_brng, kode_sat, jenis, status 
            FROM ipsrsbarang
        """)
        rows = source_cursor.fetchall()

        changed_rows = []

        for row in rows:
            target_cursor.execute("""
                SELECT nama_brng, kode_sat, jenis, status 
                FROM dim_barang 
                WHERE kode_brng = %s
            """, (row['kode_brng'],))
            existing = target_cursor.fetchone()

            if (not existing or 
                existing['nama_brng'] != row['nama_brng'] or 
                existing['kode_sat'] != row['kode_sat'] or 
                existing['jenis'] != row['jenis'] or 
                existing['status'] != row['status']):

                query = """
                    INSERT INTO dim_barang (kode_brng, nama_brng, kode_sat, jenis, status)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        nama_brng = VALUES(nama_brng),
                        kode_sat = VALUES(kode_sat),
                        jenis = VALUES(jenis),
                        status = VALUES(status)
                """
                target_cursor.execute(query, (
                    row['kode_brng'], row['nama_brng'], 
                    row['kode_sat'], row['jenis'], row['status']
                ))
                changed_rows.append(row['kode_brng'])

        target_conn.commit()

        if changed_rows:
            print(f"Update: {len(changed_rows)} baris di dim_barang.")
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
    sync_ipsrs_barang()
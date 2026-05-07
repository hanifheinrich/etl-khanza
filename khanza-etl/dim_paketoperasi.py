import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_paket_operasi():
    source_conn = None
    target_conn = None
    
    try:
        config = load_config()
        source_conn = get_connection(config['source_config'])
        target_conn = get_connection(config['target_config'])
        
        source_cursor = source_conn.cursor(dictionary=True)
        target_cursor = target_conn.cursor(dictionary=True)

        source_cursor.execute("""
            SELECT kode_paket, nm_perawatan, kategori, status, kelas 
            FROM paket_operasi
        """)
        rows = source_cursor.fetchall()

        changed_rows = []

        for row in rows:
            target_cursor.execute("""
                SELECT nm_perawatan, kategori, status, kelas 
                FROM dim_paketoperasi 
                WHERE kode_paket = %s
            """, (row['kode_paket'],))
            existing = target_cursor.fetchone()

            if (not existing or 
                existing['nm_perawatan'] != row['nm_perawatan'] or 
                existing['kategori'] != row['kategori'] or 
                existing['status'] != row['status'] or 
                existing['kelas'] != row['kelas']):

                query = """
                    INSERT INTO dim_paketoperasi (
                        kode_paket, nm_perawatan, kategori, status, kelas
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        nm_perawatan = VALUES(nm_perawatan),
                        kategori = VALUES(kategori),
                        status = VALUES(status),
                        kelas = VALUES(kelas)
                """
                target_cursor.execute(query, (
                    row['kode_paket'], 
                    row['nm_perawatan'], 
                    row['kategori'], 
                    row['status'], 
                    row['kelas']
                ))
                changed_rows.append(row['kode_paket'])

        target_conn.commit()

        if changed_rows:
            print(f"Update: {len(changed_rows)} baris di dim_paketoperasi.")
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
    sync_paket_operasi()
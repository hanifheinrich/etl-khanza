import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_labor_services():
    source_conn = None
    target_conn = None
    
    try:
        config = load_config()
        source_conn = get_connection(config['source_config'])
        target_conn = get_connection(config['target_config'])
        
        source_cursor = source_conn.cursor(dictionary=True)
        target_cursor = target_conn.cursor(dictionary=True)

        source_cursor.execute("""
            SELECT 
                kd_jenis_prw, nm_perawatan, kd_pj, 
                status, kelas, kategori
            FROM jns_perawatan_lab
        """)
        rows = source_cursor.fetchall()

        changed_rows = []

        for row in rows:
            target_cursor.execute("""
                SELECT nm_perawatan, kd_pj, status, kelas, kategori
                FROM dim_labor
                WHERE kd_jenis_prw = %s
            """, (row['kd_jenis_prw'],))
            existing = target_cursor.fetchone()

            if (not existing or
                existing['nm_perawatan'] != row['nm_perawatan'] or
                existing['kd_pj'] != row['kd_pj'] or
                existing['status'] != row['status'] or
                existing['kelas'] != row['kelas'] or
                existing['kategori'] != row['kategori']):
                
                query = """
                    INSERT INTO dim_labor (
                        kd_jenis_prw, nm_perawatan, kd_pj, 
                        status, kelas, kategori
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        nm_perawatan = VALUES(nm_perawatan),
                        kd_pj = VALUES(kd_pj),
                        status = VALUES(status),
                        kelas = VALUES(kelas),
                        kategori = VALUES(kategori)
                """
                target_cursor.execute(query, (
                    row['kd_jenis_prw'], row['nm_perawatan'], row['kd_pj'],
                    row['status'], row['kelas'], row['kategori']
                ))
                changed_rows.append(row['kd_jenis_prw'])

        target_conn.commit()

        if changed_rows:
            print(f"Update: {len(changed_rows)} baris di dim_labor.")
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
    sync_labor_services()
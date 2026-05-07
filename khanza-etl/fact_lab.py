import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_pemeriksaan_lab():
    source_conn = None
    target_conn = None
    
    try:
        config = load_config()
        source_conn = get_connection(config['source_config'])
        target_conn = get_connection(config['target_config'])
        
        source_cursor = source_conn.cursor(dictionary=True)
        target_cursor = target_conn.cursor()

        select_query = """
            SELECT 
                no_rawat, nip, kd_jenis_prw, tgl_periksa, jam, 
                dokter_perujuk, biaya, kd_dokter, status, kategori
            FROM periksa_lab
            WHERE tgl_periksa >= CURDATE() - INTERVAL 7 DAY
        """
        source_cursor.execute(select_query)

        insert_query = """
            INSERT INTO fact_lab (
                no_rawat, nip, kd_jenis_prw, tgl_periksa, jam, 
                dokter_perujuk, biaya, kd_dokter, status, kategori,
                created_at, updated_at
            ) VALUES (
                %(no_rawat)s, %(nip)s, %(kd_jenis_prw)s, %(tgl_periksa)s, 
                %(jam)s, %(dokter_perujuk)s, %(biaya)s, %(kd_dokter)s, 
                %(status)s, %(kategori)s, NOW(), NOW()
            )
            ON DUPLICATE KEY UPDATE
                nip = VALUES(nip),
                dokter_perujuk = VALUES(dokter_perujuk),
                biaya = VALUES(biaya),
                kd_dokter = VALUES(kd_dokter),
                status = VALUES(status),
                kategori = VALUES(kategori),
                updated_at = NOW()
        """

        batch_size = 5000
        total_synced = 0

        while True:
            rows = source_cursor.fetchmany(batch_size)
            if not rows:
                break

            target_cursor.executemany(insert_query, rows)
            target_conn.commit()
            total_synced += len(rows)
            print(f"Batch Sync: {len(rows)} baris diproses.")

        print(f"Success: Total {total_synced} baris disinkronkan ke fact_lab.")

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
    sync_pemeriksaan_lab()
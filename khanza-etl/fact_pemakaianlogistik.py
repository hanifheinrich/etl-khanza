import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_pemakaian_logistik():
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
                p.no_permintaan, d.kode_brng, d.kode_sat, d.jumlah, 
                p.nip, p.ruang, p.tanggal, p.status
            FROM permintaan_non_medis p
            JOIN detail_permintaan_non_medis d 
                ON p.no_permintaan = d.no_permintaan
            WHERE p.tanggal >= CURDATE() - INTERVAL 7 DAY
        """
        source_cursor.execute(select_query)

        insert_query = """
            INSERT INTO fact_pemakaianlogistik (
                no_permintaan, kode_barang, kode_sat, jumlah, 
                nip, ruang, tanggal, status
            ) VALUES (
                %(no_permintaan)s, %(kode_brng)s, %(kode_sat)s, %(jumlah)s, 
                %(nip)s, %(ruang)s, %(tanggal)s, %(status)s
            )
            ON DUPLICATE KEY UPDATE 
                jumlah = VALUES(jumlah),
                nip = VALUES(nip),
                ruang = VALUES(ruang),
                tanggal = VALUES(tanggal),
                status = VALUES(status)
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

        print(f"Success: Total {total_synced} baris disinkronkan ke fact_pemakaianlogistik.")

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
    sync_pemakaian_logistik()
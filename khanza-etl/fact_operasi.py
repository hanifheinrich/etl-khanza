import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_operasi():
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
                no_rawat, kode_paket, tanggal, jam_mulai, 
                jam_selesai, status, kd_dokter, kd_ruang_ok
            FROM booking_operasi
            WHERE tanggal >= CURDATE() - INTERVAL 60 DAY
        """
        source_cursor.execute(select_query)

        insert_query = """
            INSERT INTO fact_operasi (
                no_rawat, kode_paket, tanggal, jam_mulai, 
                jam_selesai, status, kd_dokter, kd_ruang_ok
            ) VALUES (
                %(no_rawat)s, %(kode_paket)s, %(tanggal)s, %(jam_mulai)s, 
                %(jam_selesai)s, %(status)s, %(kd_dokter)s, %(kd_ruang_ok)s
            )
            ON DUPLICATE KEY UPDATE
                kode_paket = VALUES(kode_paket),
                tanggal = VALUES(tanggal),
                jam_mulai = VALUES(jam_mulai),
                jam_selesai = VALUES(jam_selesai),
                status = VALUES(status),
                kd_dokter = VALUES(kd_dokter),
                kd_ruang_ok = VALUES(kd_ruang_ok)
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

        print(f"Success: Total {total_synced} baris disinkronkan ke fact_operasi.")

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
    sync_operasi()
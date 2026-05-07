import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_prosedur_pasien():
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
                pp.no_rawat, pp.kode, pp.status, pp.prioritas
            FROM prosedur_pasien pp
            JOIN reg_periksa rp ON pp.no_rawat = rp.no_rawat
            WHERE rp.tgl_registrasi >= CURDATE() - INTERVAL 60 DAY
        """
        source_cursor.execute(select_query)

        insert_query = """
            INSERT INTO fact_prosedur_pasien (
                no_rawat, kode, status, prioritas
            ) VALUES (
                %(no_rawat)s, %(kode)s, %(status)s, %(prioritas)s
            )
            ON DUPLICATE KEY UPDATE
                status = VALUES(status),
                prioritas = VALUES(prioritas)
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

        print(f"Success: Total {total_synced} baris disinkronkan ke fact_prosedur_pasien.")

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
    sync_prosedur_pasien()
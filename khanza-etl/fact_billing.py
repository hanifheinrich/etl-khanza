import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_billing():
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
                no_rawat, tgl_byr, status, nm_perawatan, 
                biaya, jumlah, tambahan, totalbiaya
            FROM billing
            WHERE tgl_byr >= CURDATE() - INTERVAL 60 DAY
        """
        source_cursor.execute(select_query)

        insert_query = """
            INSERT INTO fact_billing (
                no_rawat, tgl_byr, status, nm_perawatan, 
                biaya, jumlah, tambahan, totalbiaya
            ) VALUES (
                %(no_rawat)s, %(tgl_byr)s, %(status)s, %(nm_perawatan)s, 
                %(biaya)s, %(jumlah)s, %(tambahan)s, %(totalbiaya)s
            )
            ON DUPLICATE KEY UPDATE
                tgl_byr = VALUES(tgl_byr),
                status = VALUES(status),
                nm_perawatan = VALUES(nm_perawatan),
                biaya = VALUES(biaya),
                jumlah = VALUES(jumlah),
                tambahan = VALUES(tambahan),
                totalbiaya = VALUES(totalbiaya)
        """

        batch_size = 5000
        total_synced = 0

        while True:
            rows = source_cursor.fetchmany(batch_size)
            if not rows:
                break

            clean_rows = []
            for row in rows:
                if not row['totalbiaya'] or row['totalbiaya'] == 0:
                    continue

                if row['status'] and row['status'].strip().lower() == 'registrasi':
                    row['nm_perawatan'] = 'Registrasi'

                clean_rows.append(row)

            if clean_rows:
                target_cursor.executemany(insert_query, clean_rows)
                target_conn.commit()
                total_synced += len(clean_rows)
                print(f"Batch Sync: {len(clean_rows)} baris diproses.")

        print(f"Success: Total {total_synced} baris disinkronkan ke fact_billing.")

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
    sync_billing()
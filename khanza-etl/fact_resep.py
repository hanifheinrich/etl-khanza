import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_resep():
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
                rd.no_resep, rd.tgl_perawatan, rd.no_rawat, 
                rd.kd_dokter, rd.status, ro.kode_brng, ro.jml
            FROM resep_dokter ro
            JOIN resep_obat rd ON rd.no_resep = ro.no_resep
            WHERE rd.tgl_perawatan >= (CURDATE() - INTERVAL 60 DAY)
        """
        source_cursor.execute(select_query)

        insert_query = """
            INSERT INTO fact_resep (
                no_resep, tgl_perawatan, no_rawat, kd_dokter, 
                status, kode_brng, jml
            ) VALUES (
                %(no_resep)s, %(tgl_perawatan)s, %(no_rawat)s, 
                %(kd_dokter)s, %(status)s, %(kode_brng)s, %(jml)s
            )
            ON DUPLICATE KEY UPDATE
                tgl_perawatan = VALUES(tgl_perawatan),
                no_rawat = VALUES(no_rawat),
                kd_dokter = VALUES(kd_dokter),
                status = VALUES(status),
                kode_brng = VALUES(kode_brng),
                jml = VALUES(jml)
        """

        batch_size = 5000
        total_synced = 0

        while True:
            rows = source_cursor.fetchmany(batch_size)
            if not rows:
                break

            clean_rows = [r for r in rows if r['tgl_perawatan'] and r['tgl_perawatan'] != '0000-00-00']

            if clean_rows:
                target_cursor.executemany(insert_query, clean_rows)
                target_conn.commit()
                total_synced += len(clean_rows)
                print(f"Batch Sync: {len(clean_rows)} baris diproses.")

        print(f"Success: Total {total_synced} baris dimigrasikan ke fact_resep.")

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
    sync_resep()
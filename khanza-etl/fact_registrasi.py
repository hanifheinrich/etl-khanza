import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_registrasi():
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
                no_rawat, tgl_registrasi, jam_reg, kd_dokter, no_rkm_medis, 
                kd_poli, biaya_reg, stts, stts_daftar, status_lanjut, 
                kd_pj, status_bayar, status_poli
            FROM reg_periksa
            WHERE tgl_registrasi >= CURDATE() - INTERVAL 60 DAY
        """
        source_cursor.execute(select_query)

        insert_query = """
            INSERT INTO fact_registrasi (
                no_rawat, tgl_registrasi, jam_reg, kd_dokter, no_rkm_medis,
                kd_poli, biaya_reg, stts, stts_daftar, status_lanjut,
                kd_pj, status_bayar, status_poli
            ) VALUES (
                %(no_rawat)s, %(tgl_registrasi)s, %(jam_reg)s, %(kd_dokter)s, 
                %(no_rkm_medis)s, %(kd_poli)s, %(biaya_reg)s, %(stts)s, 
                %(stts_daftar)s, %(status_lanjut)s, %(kd_pj)s, 
                %(status_bayar)s, %(status_poli)s
            )
            ON DUPLICATE KEY UPDATE
                tgl_registrasi = VALUES(tgl_registrasi),
                jam_reg = VALUES(jam_reg),
                kd_dokter = VALUES(kd_dokter),
                no_rkm_medis = VALUES(no_rkm_medis),
                kd_poli = VALUES(kd_poli),
                biaya_reg = VALUES(biaya_reg),
                stts = VALUES(stts),
                stts_daftar = VALUES(stts_daftar),
                status_lanjut = VALUES(status_lanjut),
                kd_pj = VALUES(kd_pj),
                status_bayar = VALUES(status_bayar),
                status_poli = VALUES(status_poli)
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

        print(f"Success: Total {total_synced} baris disinkronkan ke fact_registrasi.")

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
    sync_registrasi()
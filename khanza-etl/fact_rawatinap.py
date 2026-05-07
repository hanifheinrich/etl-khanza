import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_rawat_inap():
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
                ki.no_rawat, ki.kd_kamar, ki.diagnosa_awal, ki.diagnosa_akhir,
                ki.tgl_masuk, ki.jam_masuk, ki.tgl_keluar, ki.jam_keluar,
                ki.lama, CAST(ki.stts_pulang AS CHAR) AS stts_pulang, d.kd_dokter
            FROM kamar_inap ki
            LEFT JOIN (
                SELECT no_rawat, MAX(kd_dokter) AS kd_dokter
                FROM dpjp_ranap
                GROUP BY no_rawat
            ) d ON ki.no_rawat = d.no_rawat
            WHERE ki.tgl_masuk >= CURDATE() - INTERVAL 60 DAY
        """
        source_cursor.execute(select_query)

        insert_query = """
            INSERT INTO fact_rawatinap (
                no_rawat, kd_kamar, diagnosa_awal, diagnosa_akhir,
                tgl_masuk, jam_masuk, tgl_keluar, jam_keluar,
                lama, stts_pulang, kd_dokter
            ) VALUES (
                %(no_rawat)s, %(kd_kamar)s, %(diagnosa_awal)s, %(diagnosa_akhir)s,
                %(tgl_masuk)s, %(jam_masuk)s, %(tgl_keluar)s, %(jam_keluar)s,
                %(lama)s, %(stts_pulang)s, %(kd_dokter)s
            )
            ON DUPLICATE KEY UPDATE
                kd_kamar = VALUES(kd_kamar),
                diagnosa_awal = VALUES(diagnosa_awal),
                diagnosa_akhir = VALUES(diagnosa_akhir),
                tgl_masuk = VALUES(tgl_masuk),
                jam_masuk = VALUES(jam_masuk),
                tgl_keluar = VALUES(tgl_keluar),
                jam_keluar = VALUES(jam_keluar),
                lama = VALUES(lama),
                stts_pulang = VALUES(stts_pulang),
                kd_dokter = COALESCE(VALUES(kd_dokter), kd_dokter)
        """

        batch_size = 5000
        total_synced = 0

        while True:
            rows = source_cursor.fetchmany(batch_size)
            if not rows:
                break

            for r in rows:
                if r.get('kd_dokter'):
                    r['kd_dokter'] = r['kd_dokter'].strip()

            target_cursor.executemany(insert_query, rows)
            target_conn.commit()
            total_synced += len(rows)
            print(f"Batch Sync: {len(rows)} baris diproses.")

        print(f"Success: Total {total_synced} baris disinkronkan ke fact_rawatinap.")

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
    sync_rawat_inap()
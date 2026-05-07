import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_pasien():
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
                no_rkm_medis, nm_pasien, no_ktp, CAST(jk AS CHAR) AS jk, 
                tmp_lahir, tgl_lahir, nm_ibu, alamat, gol_darah, 
                pekerjaan, stts_nikah, agama, no_tlp, pnd, no_peserta
            FROM pasien
        """)
        rows = source_cursor.fetchall()

        changed_rows = []

        for row in rows:
            if row["jk"] == "P":
                row["jk"] = "Perempuan"
            elif row["jk"] == "L":
                row["jk"] = "Laki-laki"

            target_cursor.execute("""
                SELECT nm_pasien, no_ktp, jk, tmp_lahir, tgl_lahir,
                       nm_ibu, alamat, gol_darah, pekerjaan, stts_nikah,
                       agama, no_tlp, pnd, no_peserta
                FROM dim_pasien
                WHERE no_rkm_medis = %s
            """, (row['no_rkm_medis'],))
            existing = target_cursor.fetchone()

            if (not existing or
                existing['nm_pasien'] != row['nm_pasien'] or
                existing['no_ktp'] != row['no_ktp'] or
                existing['jk'] != row['jk'] or
                existing['tmp_lahir'] != row['tmp_lahir'] or
                existing['tgl_lahir'] != row['tgl_lahir'] or
                existing['nm_ibu'] != row['nm_ibu'] or
                existing['alamat'] != row['alamat'] or
                existing['gol_darah'] != row['gol_darah'] or
                existing['pekerjaan'] != row['pekerjaan'] or
                existing['stts_nikah'] != row['stts_nikah'] or
                existing['agama'] != row['agama'] or
                existing['no_tlp'] != row['no_tlp'] or
                existing['pnd'] != row['pnd'] or
                existing['no_peserta'] != row['no_peserta']):
                
                query = """
                    INSERT INTO dim_pasien (
                        no_rkm_medis, nm_pasien, no_ktp, jk, tmp_lahir, tgl_lahir,
                        nm_ibu, alamat, gol_darah, pekerjaan, stts_nikah,
                        agama, no_tlp, pnd, no_peserta
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        nm_pasien = VALUES(nm_pasien),
                        no_ktp = VALUES(no_ktp),
                        jk = VALUES(jk),
                        tmp_lahir = VALUES(tmp_lahir),
                        tgl_lahir = VALUES(tgl_lahir),
                        nm_ibu = VALUES(nm_ibu),
                        alamat = VALUES(alamat),
                        gol_darah = VALUES(gol_darah),
                        pekerjaan = VALUES(pekerjaan),
                        stts_nikah = VALUES(stts_nikah),
                        agama = VALUES(agama),
                        no_tlp = VALUES(no_tlp),
                        pnd = VALUES(pnd),
                        no_peserta = VALUES(no_peserta)
                """
                target_cursor.execute(query, (
                    row['no_rkm_medis'], row['nm_pasien'], row['no_ktp'], row['jk'],
                    row['tmp_lahir'], row['tgl_lahir'], row['nm_ibu'], row['alamat'],
                    row['gol_darah'], row['pekerjaan'], row['stts_nikah'], row['agama'],
                    row['no_tlp'], row['pnd'], row['no_peserta']
                ))
                changed_rows.append(row['no_rkm_medis'])

        target_conn.commit()

        if changed_rows:
            print(f"Update: {len(changed_rows)} baris di dim_pasien.")
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
    sync_pasien()
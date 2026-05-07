import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_pegawai():
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
                nik, nama, jk, jbtn, jnj_jabatan,
                departemen, bidang, stts_kerja, pendidikan,
                tmp_lahir, tgl_lahir, alamat, kota,
                mulai_kerja, stts_aktif, mulai_kontrak
            FROM pegawai
        """)
        rows = source_cursor.fetchall()

        changed_rows = []

        for row in rows:
            target_cursor.execute("""
                SELECT nama, jk, jbtn, jnj_jabatan,
                       departemen, bidang, stts_kerja, pendidikan,
                       tmp_lahir, tgl_lahir, alamat, kota,
                       mulai_kerja, stts_aktif, mulai_kontrak
                FROM dim_pegawai
                WHERE nik = %s
            """, (row['nik'],))
            existing = target_cursor.fetchone()

            if (not existing or
                existing['nama'] != row['nama'] or
                existing['jk'] != row['jk'] or
                existing['jbtn'] != row['jbtn'] or
                existing['jnj_jabatan'] != row['jnj_jabatan'] or
                existing['departemen'] != row['departemen'] or
                existing['bidang'] != row['bidang'] or
                existing['stts_kerja'] != row['stts_kerja'] or
                existing['pendidikan'] != row['pendidikan'] or
                existing['tmp_lahir'] != row['tmp_lahir'] or
                existing['tgl_lahir'] != row['tgl_lahir'] or
                existing['alamat'] != row['alamat'] or
                existing['kota'] != row['kota'] or
                existing['mulai_kerja'] != row['mulai_kerja'] or
                existing['stts_aktif'] != row['stts_aktif'] or
                existing['mulai_kontrak'] != row['mulai_kontrak']):
                
                query = """
                    INSERT INTO dim_pegawai (
                        nik, nama, jk, jbtn, jnj_jabatan,
                        departemen, bidang, stts_kerja, pendidikan,
                        tmp_lahir, tgl_lahir, alamat, kota,
                        mulai_kerja, stts_aktif, mulai_kontrak
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        nama = VALUES(nama),
                        jk = VALUES(jk),
                        jbtn = VALUES(jbtn),
                        jnj_jabatan = VALUES(jnj_jabatan),
                        departemen = VALUES(departemen),
                        bidang = VALUES(bidang),
                        stts_kerja = VALUES(stts_kerja),
                        pendidikan = VALUES(pendidikan),
                        tmp_lahir = VALUES(tmp_lahir),
                        tgl_lahir = VALUES(tgl_lahir),
                        alamat = VALUES(alamat),
                        kota = VALUES(kota),
                        mulai_kerja = VALUES(mulai_kerja),
                        stts_aktif = VALUES(stts_aktif),
                        mulai_kontrak = VALUES(mulai_kontrak)
                """
                target_cursor.execute(query, (
                    row['nik'], row['nama'], row['jk'], row['jbtn'], row['jnj_jabatan'],
                    row['departemen'], row['bidang'], row['stts_kerja'], row['pendidikan'],
                    row['tmp_lahir'], row['tgl_lahir'], row['alamat'], row['kota'],
                    row['mulai_kerja'], row['stts_aktif'], row['mulai_kontrak']
                ))
                changed_rows.append(row['nik'])

        target_conn.commit()

        if changed_rows:
            print(f"Update: {len(changed_rows)} baris di dim_pegawai.")
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
    sync_pegawai()
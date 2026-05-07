import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_dokter():
    sps_mapping = {
        "ANAK": "Anak",
        "ANAS": "Anastesi",
        "AND": "Andrologi",
        "FISIO": "Fisioterapi",
        "KULIT": "Kulit & Kelamin",
        "OBGYN": "Obstetri dan Ginekologi",
        "PAK": "Patologi Klinis",
        "PSKLG": "Psikologi",
        "SpPD": "Penyakit Dalam",
        "UMUM": "Umum"
    }

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
                kd_dokter, nm_dokter, CAST(jk AS CHAR) AS jk,
                tgl_lahir, kd_sps, no_telp, CAST(status AS CHAR) AS status
            FROM dokter
        """)
        rows = source_cursor.fetchall()

        changed_rows = []

        for row in rows:
            if row['jk'] == 'P':
                row['jk'] = 'Perempuan'
            elif row['jk'] == 'L':
                row['jk'] = 'Laki-laki'

            row['kd_sps'] = sps_mapping.get(row['kd_sps'], row['kd_sps'])

            target_cursor.execute("""
                SELECT nm_dokter, jk, tgl_lahir, kd_sps, no_telp, status
                FROM dim_dokter
                WHERE kd_dokter = %s
            """, (row['kd_dokter'],))
            existing = target_cursor.fetchone()

            if (not existing or
                existing['nm_dokter'] != row['nm_dokter'] or
                existing['jk'] != row['jk'] or
                existing['tgl_lahir'] != row['tgl_lahir'] or
                existing['kd_sps'] != row['kd_sps'] or
                existing['no_telp'] != row['no_telp'] or
                existing['status'] != row['status']):
                
                query = """
                    INSERT INTO dim_dokter (
                        kd_dokter, nm_dokter, jk, tgl_lahir, kd_sps, 
                        no_telp, status, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON DUPLICATE KEY UPDATE
                        nm_dokter = VALUES(nm_dokter),
                        jk = VALUES(jk),
                        tgl_lahir = VALUES(tgl_lahir),
                        kd_sps = VALUES(kd_sps),
                        no_telp = VALUES(no_telp),
                        status = VALUES(status),
                        updated_at = NOW()
                """
                target_cursor.execute(query, (
                    row['kd_dokter'], row['nm_dokter'], row['jk'],
                    row['tgl_lahir'], row['kd_sps'], row['no_telp'], row['status']
                ))
                changed_rows.append(row['kd_dokter'])

        target_conn.commit()

        if changed_rows:
            print(f"Update: {len(changed_rows)} baris di dim_dokter.")
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
    sync_dokter()
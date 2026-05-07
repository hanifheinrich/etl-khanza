import mysql.connector
from mysql.connector import Error
import json

def load_config(filename):
    with open(filename, 'r') as f:
        return json.load(f)

try:
    config = load_config('config.txt')
    source_config = config['source_config']
    target_config = config['target_config']

    source_conn = mysql.connector.connect(**source_config)
    source_cursor = source_conn.cursor(dictionary=True)

    target_conn = mysql.connector.connect(**target_config)
    target_cursor = target_conn.cursor(dictionary=True)

    source_cursor.execute("""
        SELECT 
            kode_brng, nama_brng, kode_satbesar, kode_sat, 
            kdjns, expire, status, kode_kategori
        FROM databarang
    """)
    rows = source_cursor.fetchall()

    changed_rows = []

    for row in rows:
        target_cursor.execute("""
            SELECT 
                nama_brng, kode_satbesar, kode_sat, kdjns, expire, status, kode_kategori
            FROM dim_alkes
            WHERE kode_brng = %s
        """, (row['kode_brng'],))
        existing = target_cursor.fetchone()

        if (not existing or
            existing['nama_brng'] != row['nama_brng'] or
            existing['kode_satbesar'] != row['kode_satbesar'] or
            existing['kode_sat'] != row['kode_sat'] or
            existing['kdjns'] != row['kdjns'] or
            existing['expire'] != row['expire'] or
            existing['status'] != row['status'] or
            existing['kode_kategori'] != row['kode_kategori']):
            
            target_cursor.execute("""
                INSERT INTO dim_alkes (
                    kode_brng, nama_brng, kode_satbesar, kode_sat, 
                    kdjns, expire, status, kode_kategori
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    nama_brng = VALUES(nama_brng),
                    kode_satbesar = VALUES(kode_satbesar),
                    kode_sat = VALUES(kode_sat),
                    kdjns = VALUES(kdjns),
                    expire = VALUES(expire),
                    status = VALUES(status),
                    kode_kategori = VALUES(kode_kategori)
            """, (
                row['kode_brng'], row['nama_brng'], row['kode_satbesar'],
                row['kode_sat'], row['kdjns'], row['expire'],
                row['status'], row['kode_kategori']
            ))
            changed_rows.append(row['kode_brng'])

    target_conn.commit()

    if changed_rows:
        print(f"{len(changed_rows)} baris diperbarui di dim_alkes.")
    else:
        print("Tidak ada perubahan.")

except FileNotFoundError:
    print("Error: File config.txt tidak ditemukan.")
except Error as e:
    print(f"Database Error: {e}")
except Exception as e:
    print(f"Error: {e}")

finally:
    if 'source_conn' in locals() and source_conn.is_connected():
        source_cursor.close()
        source_conn.close()
    if 'target_conn' in locals() and target_conn.is_connected():
        target_cursor.close()
        target_conn.close()
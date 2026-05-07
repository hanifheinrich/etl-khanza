import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_sipstr():
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
                id, nama_karyawan, unit, no_sip, 
                masa_berlaku_sip, no_str, masa_berlaku_str
            FROM data_sip_str
        """)
        rows = source_cursor.fetchall()

        changed_rows = []

        for row in rows:
            target_cursor.execute("""
                SELECT nama_karyawan, unit, no_sip, masa_berlaku_sip, no_str, masa_berlaku_str
                FROM dim_sipstr
                WHERE id = %s
            """, (row['id'],))
            existing = target_cursor.fetchone()

            if (not existing or
                existing['nama_karyawan'] != row['nama_karyawan'] or
                existing['unit'] != row['unit'] or
                existing['no_sip'] != row['no_sip'] or
                existing['masa_berlaku_sip'] != row['masa_berlaku_sip'] or
                existing['no_str'] != row['no_str'] or
                existing['masa_berlaku_str'] != row['masa_berlaku_str']):
                
                query = """
                    INSERT INTO dim_sipstr (
                        id, nama_karyawan, unit, no_sip, masa_berlaku_sip, 
                        no_str, masa_berlaku_str, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON DUPLICATE KEY UPDATE
                        nama_karyawan = VALUES(nama_karyawan),
                        unit = VALUES(unit),
                        no_sip = VALUES(no_sip),
                        masa_berlaku_sip = VALUES(masa_berlaku_sip),
                        no_str = VALUES(no_str),
                        masa_berlaku_str = VALUES(masa_berlaku_str),
                        updated_at = NOW()
                """
                target_cursor.execute(query, (
                    row['id'], row['nama_karyawan'], row['unit'], 
                    row['no_sip'], row['masa_berlaku_sip'], 
                    row['no_str'], row['masa_berlaku_str']
                ))
                changed_rows.append(row['id'])

        target_conn.commit()

        if changed_rows:
            print(f"Update: {len(changed_rows)} baris di dim_sipstr.")
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
    sync_sipstr()
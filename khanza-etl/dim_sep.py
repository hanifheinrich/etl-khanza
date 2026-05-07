import mysql.connector
from mysql.connector import Error
import json
from datetime import datetime, timedelta

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_sep_data():
    source_conn = None
    target_conn = None
    
    try:
        config = load_config()
        source_conn = get_connection(config['source_config'])
        target_conn = get_connection(config['target_config'])
        
        source_cursor = source_conn.cursor(dictionary=True)
        target_cursor = target_conn.cursor(dictionary=True)

        today = datetime.today()
        start_date = (today - timedelta(days=60)).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')

        source_cursor.execute("""
            SELECT no_sep, no_rawat, tglsep, nama_pasien, nmdpdjp
            FROM bridging_sep
            WHERE tglsep BETWEEN %s AND %s
        """, (start_date, end_date))
        rows = source_cursor.fetchall()

        changed_rows = []

        for row in rows:
            target_cursor.execute("""
                SELECT no_rawat, tglsep, nama_pasien, nmdpdjp
                FROM dim_sep
                WHERE no_sep = %s
            """, (row['no_sep'],))
            existing = target_cursor.fetchone()

            if (not existing or
                existing['no_rawat'] != row['no_rawat'] or
                existing['tglsep'] != row['tglsep'] or
                existing['nama_pasien'] != row['nama_pasien'] or
                existing['nmdpdjp'] != row['nmdpdjp']):
                
                query = """
                    INSERT INTO dim_sep (
                        no_sep, no_rawat, tglsep, nama_pasien, nmdpdjp, 
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                    ON DUPLICATE KEY UPDATE
                        no_rawat = VALUES(no_rawat),
                        tglsep = VALUES(tglsep),
                        nama_pasien = VALUES(nama_pasien),
                        nmdpdjp = VALUES(nmdpdjp),
                        updated_at = NOW()
                """
                target_cursor.execute(query, (
                    row['no_sep'], row['no_rawat'], row['tglsep'], 
                    row['nama_pasien'], row['nmdpdjp']
                ))
                changed_rows.append(row['no_sep'])

        target_conn.commit()

        if changed_rows:
            print(f"Update: {len(changed_rows)} baris di dim_sep.")
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
    sync_sep_data()
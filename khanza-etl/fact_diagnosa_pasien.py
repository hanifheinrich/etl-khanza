import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_diagnosa_pasien():
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
                no_rawat, kd_penyakit, status, 
                prioritas, status_penyakit
            FROM diagnosa_pasien
        """
        source_cursor.execute(select_query)
        rows = source_cursor.fetchall()

        insert_query = """
            INSERT INTO fact_diagnosa_pasien (
                no_rawat, kd_penyakit, status, 
                prioritas, status_penyakit
            ) VALUES (
                %(no_rawat)s, %(kd_penyakit)s, %(status)s, 
                %(prioritas)s, %(status_penyakit)s
            )
            ON DUPLICATE KEY UPDATE
                prioritas = VALUES(prioritas),
                status_penyakit = VALUES(status_penyakit)
        """

        if rows:
            target_cursor.executemany(insert_query, rows)
            target_conn.commit()
            print(f"Success: {len(rows)} baris diproses ke fact_diagnosa_pasien.")
        else:
            print("Status: Tidak ada data untuk disinkronkan.")

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
    sync_diagnosa_pasien()
import mysql.connector
from mysql.connector import Error
import json
from datetime import datetime

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_stok_obat():
    source_conn = None
    target_conn = None
    
    try:
        config = load_config()
        source_conn = get_connection(config['source_config'])
        target_conn = get_connection(config['target_config'])
        
        source_cursor = source_conn.cursor(dictionary=True)
        target_cursor = target_conn.cursor()

        source_cursor.execute("""
            SELECT kode_brng, kd_bangsal, stok, no_batch, no_faktur 
            FROM gudangbarang
        """)
        rows = source_cursor.fetchall()

        query = """
            INSERT INTO fact_stokobat (
                kode_brng, kd_bangsal, stok, no_batch, no_faktur, tgl_insert
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                stok = VALUES(stok),
                tgl_insert = VALUES(tgl_insert)
        """

        current_time = datetime.now()
        data_to_sync = [
            (r['kode_brng'], r['kd_bangsal'], r['stok'], r['no_batch'], r['no_faktur'], current_time)
            for r in rows
        ]

        if data_to_sync:
            target_cursor.executemany(query, data_to_sync)
            target_conn.commit()
            print(f"Update: {len(data_to_sync)} baris di fact_stokobat.")
        else:
            print("Status: Tidak ada data untuk diproses.")

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
    sync_stok_obat()
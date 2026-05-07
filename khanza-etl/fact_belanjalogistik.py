import mysql.connector
from mysql.connector import Error
import pandas as pd
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_belanja_logistik():
    source_conn = None
    target_conn = None
    
    try:
        config = load_config()
        source_conn = get_connection(config['source_config'])
        target_conn = get_connection(config['target_config'])
        
        target_cursor = target_conn.cursor()

        query_header = """
            SELECT no_pemesanan, tanggal, kode_suplier, nip, ppn, status
            FROM surat_pemesanan_non_medis
        """
        df_header = pd.read_sql(query_header, source_conn)

        query_detail = """
            SELECT no_pemesanan, kode_brng AS kode_barang, kode_sat, 
                   jumlah, h_pesan AS harga_satuan, subtotal, total
            FROM detail_surat_pemesanan_non_medis
        """
        df_detail = pd.read_sql(query_detail, source_conn)

        df = pd.merge(df_detail, df_header, on="no_pemesanan", how="left")
        
        df["total_pemesanan"] = df.groupby("no_pemesanan")["total"].transform("sum")
        df["ppn_alokasi"] = ((df["total"] / df["total_pemesanan"]) * df["ppn"]).round()

        df_fakta = df[
            [
                "no_pemesanan", "tanggal", "kode_suplier", "nip", "kode_barang",
                "kode_sat", "jumlah", "harga_satuan", "subtotal", "total",
                "ppn_alokasi", "status"
            ]
        ]

        insert_query = """
            INSERT INTO fact_belanjalogistik (
                no_pemesanan, tanggal, kode_suplier, nip, kode_barang,
                kode_sat, jumlah, harga_satuan, subtotal, total, ppn, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                tanggal = VALUES(tanggal),
                kode_suplier = VALUES(kode_suplier),
                nip = VALUES(nip),
                jumlah = VALUES(jumlah),
                harga_satuan = VALUES(harga_satuan),
                subtotal = VALUES(subtotal),
                total = VALUES(total),
                ppn = VALUES(ppn),
                status = VALUES(status)
        """

        data_to_sync = df_fakta.values.tolist()
        
        if data_to_sync:
            target_cursor.executemany(insert_query, data_to_sync)
            target_conn.commit()
            print(f"Success: {len(data_to_sync)} baris diproses ke fact_belanjalogistik.")
        else:
            print("Status: Tidak ada data untuk disinkronkan.")

    except (Error, FileNotFoundError, json.JSONDecodeError, Exception) as e:
        print(f"Failure: {e}")

    finally:
        if source_conn and source_conn.is_connected():
            source_conn.close()
        if target_conn and target_conn.is_connected():
            target_cursor.close()
            target_conn.close()

if __name__ == "__main__":
    sync_belanja_logistik()
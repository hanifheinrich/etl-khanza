import requests
import mysql.connector
from mysql.connector import Error

API_URL = ""
API_USERNAME = ""
API_PASSWORD = ""

target_config = {
    'host': '',
    'user': '',
    'password': '',
    'database': ''
}

try:
    auth_url = f"{API_URL}/token"
    auth_payload = {
        "username": API_USERNAME,
        "password": API_PASSWORD
    }
    auth_response = requests.post(auth_url, json=auth_payload)
    auth_response.raise_for_status()
    token = auth_response.json().get("token")

    if not token:
        raise Exception("Token tidak ditemukan di response login API.")

    headers = {"Authorization": f"Bearer {token}"}

    spm_url = f"{API_URL}/surat_pemesanan_medis"
    response = requests.get(spm_url, headers=headers)
    response.raise_for_status()
    rows = response.json()

    target_conn = mysql.connector.connect(**target_config)
    target_cursor = target_conn.cursor()

    insert_query = """
        INSERT INTO surat_pemesanan_medis (
            no_pemesanan,
            kode_suplier,
            nip,
            tanggal,
            total1,
            potongan,
            total2,
            ppn,
            meterai,
            tagihan,
            status
        ) VALUES (
            %(no_pemesanan)s,
            %(kode_suplier)s,
            %(nip)s,
            %(tanggal)s,
            %(total1)s,
            %(potongan)s,
            %(total2)s,
            %(ppn)s,
            %(meterai)s,
            %(tagihan)s,
            %(status)s
        )
        ON DUPLICATE KEY UPDATE
            kode_suplier = VALUES(kode_suplier),
            nip = VALUES(nip),
            tanggal = VALUES(tanggal),
            total1 = VALUES(total1),
            potongan = VALUES(potongan),
            total2 = VALUES(total2),
            ppn = VALUES(ppn),
            meterai = VALUES(meterai),
            tagihan = VALUES(tagihan),
            status = VALUES(status)
    """

    target_cursor.executemany(insert_query, rows)
    target_conn.commit()

    print(f"{target_cursor.rowcount} baris berhasil dimigrasikan ke surat_pemesanan_medis dari API.")

except Error as e:
    print(f"Database Error: {e}")
except requests.exceptions.RequestException as e:
    print(f"API Error: {e}")
except Exception as e:
    print(f"Error umum: {e}")
finally:
    if 'target_cursor' in locals() and target_cursor:
        target_cursor.close()
    if 'target_conn' in locals() and target_conn:
        target_conn.close()
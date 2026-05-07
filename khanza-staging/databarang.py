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
    auth_payload = {"username": API_USERNAME, "password": API_PASSWORD}
    auth_response = requests.post(auth_url, json=auth_payload)
    auth_response.raise_for_status()
    token = auth_response.json().get("token")

    if not token:
        raise Exception("Token tidak ditemukan di response login API.")

    headers = {"Authorization": f"Bearer {token}"}

    api_url = f"{API_URL}/databarang"
    response = requests.get(api_url, headers=headers)
    response.raise_for_status()
    rows = response.json()

    target_conn = mysql.connector.connect(**target_config)
    target_cursor = target_conn.cursor()

    insert_query = """
        INSERT INTO databarang (
            kode_brng, nama_brng, kode_satbesar, kode_sat, letak_barang,
            dasar, h_beli, ralan, kelas1, kelas2, kelas3,
            utama, vip, vvip, beliluar, jualbebas, karyawan,
            stokminimal, kdjns, isi, kapasitas, expire,
            status, kode_industri, kode_kategori, kode_golongan
        ) VALUES (
            %(kode_brng)s, %(nama_brng)s, %(kode_satbesar)s, %(kode_sat)s, %(letak_barang)s,
            %(dasar)s, %(h_beli)s, %(ralan)s, %(kelas1)s, %(kelas2)s, %(kelas3)s,
            %(utama)s, %(vip)s, %(vvip)s, %(beliluar)s, %(jualbebas)s, %(karyawan)s,
            %(stokminimal)s, %(kdjns)s, %(isi)s, %(kapasitas)s, %(expire)s,
            %(status)s, %(kode_industri)s, %(kode_kategori)s, %(kode_golongan)s
        )
        ON DUPLICATE KEY UPDATE
            nama_brng = VALUES(nama_brng),
            kode_satbesar = VALUES(kode_satbesar),
            kode_sat = VALUES(kode_sat),
            letak_barang = VALUES(letak_barang),
            dasar = VALUES(dasar),
            h_beli = VALUES(h_beli),
            ralan = VALUES(ralan),
            kelas1 = VALUES(kelas1),
            kelas2 = VALUES(kelas2),
            kelas3 = VALUES(kelas3),
            utama = VALUES(utama),
            vip = VALUES(vip),
            vvip = VALUES(vvip),
            beliluar = VALUES(beliluar),
            jualbebas = VALUES(jualbebas),
            karyawan = VALUES(karyawan),
            stokminimal = VALUES(stokminimal),
            kdjns = VALUES(kdjns),
            isi = VALUES(isi),
            kapasitas = VALUES(kapasitas),
            expire = VALUES(expire),
            status = VALUES(status),
            kode_industri = VALUES(kode_industri),
            kode_kategori = VALUES(kode_kategori),
            kode_golongan = VALUES(kode_golongan)
    """

    target_cursor.executemany(insert_query, rows)
    target_conn.commit()

    print(f"{target_cursor.rowcount} baris berhasil dimigrasikan ke databarang dari API.")

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
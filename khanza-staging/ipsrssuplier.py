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

    api_url = f"{API_URL}/ipsrssuplier"
    response = requests.get(api_url, headers=headers)
    response.raise_for_status()
    rows = response.json()

    target_conn = mysql.connector.connect(**target_config)
    target_cursor = target_conn.cursor()

    insert_query = """
        INSERT INTO ipsrssuplier (
            kode_suplier, nama_suplier, alamat,
            kota, no_telp, nama_bank, rekening
        ) VALUES (
            %(kode_suplier)s, %(nama_suplier)s, %(alamat)s,
            %(kota)s, %(no_telp)s, %(nama_bank)s, %(rekening)s
        )
        ON DUPLICATE KEY UPDATE
            nama_suplier = VALUES(nama_suplier),
            alamat = VALUES(alamat),
            kota = VALUES(kota),
            no_telp = VALUES(no_telp),
            nama_bank = VALUES(nama_bank),
            rekening = VALUES(rekening)
    """

    target_cursor.executemany(insert_query, rows)
    target_conn.commit()

    print(f"{target_cursor.rowcount} baris berhasil dimigrasikan ke ipsrssuplier dari API.")

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
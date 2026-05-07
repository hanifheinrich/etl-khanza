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

    kamar_inap_url = f"{API_URL}/kamar_inap"
    response = requests.get(kamar_inap_url, headers=headers)
    response.raise_for_status()
    rows = response.json()

    for row in rows:
        tgl_keluar = row.get("tgl_keluar")
        if tgl_keluar in (None, "", "None", "0000-00-00"):
            row["tgl_keluar"] = None

    target_conn = mysql.connector.connect(**target_config)
    target_cursor = target_conn.cursor()

    insert_query = """
        INSERT INTO kamar_inap (
            no_rawat,
            kd_kamar,
            trf_kamar,
            diagnosa_awal,
            diagnosa_akhir,
            tgl_masuk,
            jam_masuk,
            tgl_keluar,
            jam_keluar,
            lama,
            ttl_biaya,
            stts_pulang
        ) VALUES (
            %(no_rawat)s,
            %(kd_kamar)s,
            %(trf_kamar)s,
            %(diagnosa_awal)s,
            %(diagnosa_akhir)s,
            %(tgl_masuk)s,
            %(jam_masuk)s,
            %(tgl_keluar)s,
            %(jam_keluar)s,
            %(lama)s,
            %(ttl_biaya)s,
            %(stts_pulang)s
        )
        ON DUPLICATE KEY UPDATE
            kd_kamar = VALUES(kd_kamar),
            trf_kamar = VALUES(trf_kamar),
            diagnosa_awal = VALUES(diagnosa_awal),
            diagnosa_akhir = VALUES(diagnosa_akhir),
            tgl_keluar = VALUES(tgl_keluar),
            jam_keluar = VALUES(jam_keluar),
            lama = VALUES(lama),
            ttl_biaya = VALUES(ttl_biaya),
            stts_pulang = VALUES(stts_pulang)
    """

    target_cursor.executemany(insert_query, rows)
    target_conn.commit()

    print(f"{target_cursor.rowcount} baris berhasil dimigrasikan ke kamar_inap dari API.")

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
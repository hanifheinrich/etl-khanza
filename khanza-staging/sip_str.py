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

    sipstr_url = f"{API_URL}/sipstr"
    response = requests.get(sipstr_url, headers=headers)
    response.raise_for_status()
    rows = response.json()

    for row in rows:
        if row.get("masa_berlaku_sip") in (None, "", "None", "0000-00-00"):
            row["masa_berlaku_sip"] = None
        if row.get("masa_berlaku_str") in (None, "", "None", "0000-00-00"):
            row["masa_berlaku_str"] = None

    target_conn = mysql.connector.connect(**target_config)
    target_cursor = target_conn.cursor()

    insert_query = """
        INSERT INTO data_sip_str (
            nama_karyawan, unit, no_sip, masa_berlaku_sip, no_str, masa_berlaku_str
        ) VALUES (
            %(nama_karyawan)s, %(unit)s, %(no_sip)s, %(masa_berlaku_sip)s,
            %(no_str)s, %(masa_berlaku_str)s
        )
        ON DUPLICATE KEY UPDATE
            nama_karyawan = VALUES(nama_karyawan),
            unit = VALUES(unit),
            no_sip = VALUES(no_sip),
            masa_berlaku_sip = VALUES(masa_berlaku_sip),
            no_str = VALUES(no_str),
            masa_berlaku_str = VALUES(masa_berlaku_str)
    """

    target_cursor.executemany(insert_query, rows)
    target_conn.commit()
    print(f"{target_cursor.rowcount} baris berhasil dimigrasikan ke data_sip_str dari API.")

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
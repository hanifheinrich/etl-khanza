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

    api_url = f"{API_URL}/paket_operasi"
    response = requests.get(api_url, headers=headers)
    response.raise_for_status()
    rows = response.json()

    target_conn = mysql.connector.connect(**target_config)
    target_cursor = target_conn.cursor()

    insert_query = """
        INSERT INTO paket_operasi (
            kode_paket, nm_perawatan, kategori,
            operator1, operator2, operator3,
            asisten_operator1, asisten_operator2, asisten_operator3,
            instrumen, dokter_anak, perawaat_resusitas,
            dokter_anestesi, asisten_anestesi, asisten_anestesi2,
            bidan, bidan2, bidan3,
            perawat_luar, sewa_ok, alat,
            akomodasi, bagian_rs,
            omloop, omloop2, omloop3, omloop4, omloop5,
            sarpras, dokter_pjanak, dokter_umum,
            kd_pj, status, kelas
        ) VALUES (
            %(kode_paket)s, %(nm_perawatan)s, %(kategori)s,
            %(operator1)s, %(operator2)s, %(operator3)s,
            %(asisten_operator1)s, %(asisten_operator2)s, %(asisten_operator3)s,
            %(instrumen)s, %(dokter_anak)s, %(perawaat_resusitas)s,
            %(dokter_anestesi)s, %(asisten_anestesi)s, %(asisten_anestesi2)s,
            %(bidan)s, %(bidan2)s, %(bidan3)s,
            %(perawat_luar)s, %(sewa_ok)s, %(alat)s,
            %(akomodasi)s, %(bagian_rs)s,
            %(omloop)s, %(omloop2)s, %(omloop3)s, %(omloop4)s, %(omloop5)s,
            %(sarpras)s, %(dokter_pjanak)s, %(dokter_umum)s,
            %(kd_pj)s, %(status)s, %(kelas)s
        )
        ON DUPLICATE KEY UPDATE
            nm_perawatan = VALUES(nm_perawatan),
            kategori = VALUES(kategori),
            operator1 = VALUES(operator1),
            operator2 = VALUES(operator2),
            operator3 = VALUES(operator3),
            asisten_operator1 = VALUES(asisten_operator1),
            asisten_operator2 = VALUES(asisten_operator2),
            asisten_operator3 = VALUES(asisten_operator3),
            instrumen = VALUES(instrumen),
            dokter_anak = VALUES(dokter_anak),
            perawaat_resusitas = VALUES(perawaat_resusitas),
            dokter_anestesi = VALUES(dokter_anestesi),
            asisten_anestesi = VALUES(asisten_anestesi),
            asisten_anestesi2 = VALUES(asisten_anestesi2),
            bidan = VALUES(bidan),
            bidan2 = VALUES(bidan2),
            bidan3 = VALUES(bidan3),
            perawat_luar = VALUES(perawat_luar),
            sewa_ok = VALUES(sewa_ok),
            alat = VALUES(alat),
            akomodasi = VALUES(akomodasi),
            bagian_rs = VALUES(bagian_rs),
            omloop = VALUES(omloop),
            omloop2 = VALUES(omloop2),
            omloop3 = VALUES(omloop3),
            omloop4 = VALUES(omloop4),
            omloop5 = VALUES(omloop5),
            sarpras = VALUES(sarpras),
            dokter_pjanak = VALUES(dokter_pjanak),
            dokter_umum = VALUES(dokter_umum),
            kd_pj = VALUES(kd_pj),
            status = VALUES(status),
            kelas = VALUES(kelas)
    """

    target_cursor.executemany(insert_query, rows)
    target_conn.commit()

    print(f"{target_cursor.rowcount} baris berhasil dimigrasikan ke paket_operasi dari API.")

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
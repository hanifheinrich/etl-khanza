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

BATCH_SIZE = 10000

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

    reg_url = f"{API_URL}/registrasi"
    response = requests.get(reg_url, headers=headers)
    response.raise_for_status()
    rows = response.json()

    for r in rows:
        if r.get("umurdaftar") in (None, "", "None"):
            r["umurdaftar"] = None
        else:
            try:
                r["umurdaftar"] = int(float(r["umurdaftar"]))
            except (ValueError, TypeError):
                r["umurdaftar"] = None    

    target_conn = mysql.connector.connect(**target_config)
    target_cursor = target_conn.cursor()
    target_conn.ping(reconnect=True, attempts=3, delay=5)

    insert_query = """
        INSERT INTO reg_periksa (
            no_reg, no_rawat, tgl_registrasi, jam_reg, kd_dokter,
            no_rkm_medis, kd_poli, p_jawab, almt_pj, hubunganpj,
            biaya_reg, stts, stts_daftar, status_lanjut, kd_pj,
            umurdaftar, sttsumur, status_bayar, status_poli
        ) VALUES (
            %(no_reg)s, %(no_rawat)s, %(tgl_registrasi)s, %(jam_reg)s, %(kd_dokter)s,
            %(no_rkm_medis)s, %(kd_poli)s, %(p_jawab)s, %(almt_pj)s, %(hubunganpj)s,
            %(biaya_reg)s, %(stts)s, %(stts_daftar)s, %(status_lanjut)s, %(kd_pj)s,
            %(umurdaftar)s, %(sttsumur)s, %(status_bayar)s, %(status_poli)s
        )
        ON DUPLICATE KEY UPDATE
            no_reg = VALUES(no_reg),
            tgl_registrasi = VALUES(tgl_registrasi),
            jam_reg = VALUES(jam_reg),
            kd_dokter = VALUES(kd_dokter),
            no_rkm_medis = VALUES(no_rkm_medis),
            kd_poli = VALUES(kd_poli),
            p_jawab = VALUES(p_jawab),
            almt_pj = VALUES(almt_pj),
            hubunganpj = VALUES(hubunganpj),
            biaya_reg = VALUES(biaya_reg),
            stts = VALUES(stts),
            stts_daftar = VALUES(stts_daftar),
            status_lanjut = VALUES(status_lanjut),
            kd_pj = VALUES(kd_pj),
            umurdaftar = VALUES(umurdaftar),
            sttsumur = VALUES(sttsumur),
            status_bayar = VALUES(status_bayar),
            status_poli = VALUES(status_poli)
    """

    total_rows = len(rows)
    print(f"Mulai migrasi {total_rows} data...")

    for i in range(0, total_rows, BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        try:
            target_conn.ping(reconnect=True, attempts=3, delay=5)
            target_cursor.executemany(insert_query, batch)
            target_conn.commit()
            print(f"Batch {i // BATCH_SIZE + 1}: {len(batch)} baris berhasil.")
        except Error as e:
            print(f"Batch {i // BATCH_SIZE + 1} gagal: {e}")
            target_conn.rollback()

    print("Migrasi selesai.")

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
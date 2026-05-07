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

    pegawai_url = f"{API_URL}/pegawai"
    response = requests.get(pegawai_url, headers=headers)
    response.raise_for_status()
    rows = response.json()

    target_conn = mysql.connector.connect(**target_config)
    target_cursor = target_conn.cursor()

    insert_query = """
        INSERT INTO pegawai (
            id, nik, nama, jk, jbtn, jnj_jabatan, kode_kelompok,
            kode_resiko, kode_emergency, departemen, bidang,
            stts_wp, stts_kerja, npwp, pendidikan, gapok,
            tmp_lahir, tgl_lahir, alamat, kota, mulai_kerja,
            ms_kerja, indexins, bpd, rekening, stts_aktif,
            wajibmasuk, pengurang, indek, mulai_kontrak,
            cuti_diambil, dankes, photo, no_ktp
        ) VALUES (
            %(id)s, %(nik)s, %(nama)s, %(jk)s, %(jbtn)s, %(jnj_jabatan)s, %(kode_kelompok)s,
            %(kode_resiko)s, %(kode_emergency)s, %(departemen)s, %(bidang)s,
            %(stts_wp)s, %(stts_kerja)s, %(npwp)s, %(pendidikan)s, %(gapok)s,
            %(tmp_lahir)s, %(tgl_lahir)s, %(alamat)s, %(kota)s, %(mulai_kerja)s,
            %(ms_kerja)s, %(indexins)s, %(bpd)s, %(rekening)s, %(stts_aktif)s,
            %(wajibmasuk)s, %(pengurang)s, %(indek)s, %(mulai_kontrak)s,
            %(cuti_diambil)s, %(dankes)s, %(photo)s, %(no_ktp)s
        )
        ON DUPLICATE KEY UPDATE
            nik = VALUES(nik),
            nama = VALUES(nama),
            jk = VALUES(jk),
            jbtn = VALUES(jbtn),
            jnj_jabatan = VALUES(jnj_jabatan),
            kode_kelompok = VALUES(kode_kelompok),
            kode_resiko = VALUES(kode_resiko),
            kode_emergency = VALUES(kode_emergency),
            departemen = VALUES(departemen),
            bidang = VALUES(bidang),
            stts_wp = VALUES(stts_wp),
            stts_kerja = VALUES(stts_kerja),
            npwp = VALUES(npwp),
            pendidikan = VALUES(pendidikan),
            gapok = VALUES(gapok),
            tmp_lahir = VALUES(tmp_lahir),
            tgl_lahir = VALUES(tgl_lahir),
            alamat = VALUES(alamat),
            kota = VALUES(kota),
            mulai_kerja = VALUES(mulai_kerja),
            ms_kerja = VALUES(ms_kerja),
            indexins = VALUES(indexins),
            bpd = VALUES(bpd),
            rekening = VALUES(rekening),
            stts_aktif = VALUES(stts_aktif),
            wajibmasuk = VALUES(wajibmasuk),
            pengurang = VALUES(pengurang),
            indek = VALUES(indek),
            mulai_kontrak = VALUES(mulai_kontrak),
            cuti_diambil = VALUES(cuti_diambil),
            dankes = VALUES(dankes),
            photo = VALUES(photo),
            no_ktp = VALUES(no_ktp)
    """

    target_cursor.executemany(insert_query, rows)
    target_conn.commit()

    print(f"{target_cursor.rowcount} baris berhasil dimigrasikan ke pegawai dari API.")

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
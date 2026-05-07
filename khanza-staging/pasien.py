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

    pasien_url = f"{API_URL}/pasien"
    response = requests.get(pasien_url, headers=headers)
    response.raise_for_status()
    rows = response.json()

    for row in rows:
        for key in ["tgl_lahir", "tgl_daftar"]:
            if row.get(key) in (None, "", "None", "0000-00-00"):
                row[key] = None

    target_conn = mysql.connector.connect(**target_config)
    target_cursor = target_conn.cursor()

    insert_query = """
        INSERT INTO pasien (
            no_rkm_medis, nm_pasien, no_ktp, jk, tmp_lahir, tgl_lahir,
            nm_ibu, alamat, gol_darah, pekerjaan, stts_nikah, agama,
            tgl_daftar, no_tlp, umur, pnd, keluarga, namakeluarga,
            kd_pj, no_peserta, kd_kel, kd_kec, kd_kab, pekerjaanpj,
            alamatpj, kelurahanpj, kecamatanpj, kabupatenpj,
            perusahaan_pasien, suku_bangsa, bahasa_pasien, cacat_fisik,
            email, nip, kd_prop, propinsipj
        ) VALUES (
            %(no_rkm_medis)s, %(nm_pasien)s, %(no_ktp)s, %(jk)s, %(tmp_lahir)s, %(tgl_lahir)s,
            %(nm_ibu)s, %(alamat)s, %(gol_darah)s, %(pekerjaan)s, %(stts_nikah)s, %(agama)s,
            %(tgl_daftar)s, %(no_tlp)s, %(umur)s, %(pnd)s, %(keluarga)s, %(namakeluarga)s,
            %(kd_pj)s, %(no_peserta)s, %(kd_kel)s, %(kd_kec)s, %(kd_kab)s, %(pekerjaanpj)s,
            %(alamatpj)s, %(kelurahanpj)s, %(kecamatanpj)s, %(kabupatenpj)s,
            %(perusahaan_pasien)s, %(suku_bangsa)s, %(bahasa_pasien)s, %(cacat_fisik)s,
            %(email)s, %(nip)s, %(kd_prop)s, %(propinsipj)s
        )
        ON DUPLICATE KEY UPDATE
            nm_pasien = VALUES(nm_pasien),
            no_ktp = VALUES(no_ktp),
            jk = VALUES(jk),
            tmp_lahir = VALUES(tmp_lahir),
            tgl_lahir = VALUES(tgl_lahir),
            nm_ibu = VALUES(nm_ibu),
            alamat = VALUES(alamat),
            gol_darah = VALUES(gol_darah),
            pekerjaan = VALUES(pekerjaan),
            stts_nikah = VALUES(stts_nikah),
            agama = VALUES(agama),
            tgl_daftar = VALUES(tgl_daftar),
            no_tlp = VALUES(no_tlp),
            umur = VALUES(umur),
            pnd = VALUES(pnd),
            keluarga = VALUES(keluarga),
            namakeluarga = VALUES(namakeluarga),
            kd_pj = VALUES(kd_pj),
            no_peserta = VALUES(no_peserta),
            kd_kel = VALUES(kd_kel),
            kd_kec = VALUES(kd_kec),
            kd_kab = VALUES(kd_kab),
            pekerjaanpj = VALUES(pekerjaanpj),
            alamatpj = VALUES(alamatpj),
            kelurahanpj = VALUES(kelurahanpj),
            kecamatanpj = VALUES(kecamatanpj),
            kabupatenpj = VALUES(kabupatenpj),
            perusahaan_pasien = VALUES(perusahaan_pasien),
            suku_bangsa = VALUES(suku_bangsa),
            bahasa_pasien = VALUES(bahasa_pasien),
            cacat_fisik = VALUES(cacat_fisik),
            email = VALUES(email),
            nip = VALUES(nip),
            kd_prop = VALUES(kd_prop),
            propinsipj = VALUES(propinsipj)
    """

    target_cursor.executemany(insert_query, rows)
    target_conn.commit()
    print(f"{target_cursor.rowcount} baris berhasil dimigrasikan ke pasien dari API.")

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
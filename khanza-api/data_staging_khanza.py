from flask import Flask, jsonify, request
import mysql.connector
from datetime import date, datetime, timedelta
import jwt
from functools import wraps

API_USERNAME = "adminapi"
API_PASSWORD = "supersecret"
JWT_SECRET = "rahasia123"
JWT_EXP = 3600

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get("Authorization")
        if not auth_header:
            return jsonify({"error": "Token tidak ditemukan"}), 401
        try:
            token = auth_header.split(" ")[1]
            jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        except Exception as e:
            return jsonify({"error": f"Token tidak valid: {str(e)}"}), 401
        return f(*args, **kwargs)
    return decorated

app = Flask(__name__)

db_config = {
    "host": "",
    "user": "",
    "password": "",
    "database": ""
}

@app.route('/token', methods=['POST'])
def generate_token():
    data = request.json
    if not data or "username" not in data or "password" not in data:
        return jsonify({"error": "Username dan password wajib diisi"}), 400

    if data["username"] == API_USERNAME and data["password"] == API_PASSWORD:
        payload = {
            "user": API_USERNAME,
            "exp": datetime.utcnow() + timedelta(seconds=JWT_EXP)
        }
        token = jwt.encode(payload, JWT_SECRET, algorithm="HS256")
        return jsonify({"token": token})
    else:
        return jsonify({"error": "Username atau password salah"}), 401


def serialize(obj):
    """Konversi tipe data agar bisa diubah ke JSON"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, timedelta):
        total_seconds = int(obj.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return str(obj)

@app.route('/registrasi', methods=['GET'])
@token_required
def get_registrasi():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM reg_periksa")
        rows = cursor.fetchall()

        result = []
        for row in rows:
            converted = {k: serialize(v) for k, v in row.items()}
            result.append(converted)

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/bangsal', methods=['GET'])
@token_required
def get_bangsal():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM bangsal")
        rows = cursor.fetchall()

        result = []
        for row in rows:
            converted = {k: serialize(v) for k, v in row.items()}
            result.append(converted)

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/databarang', methods=['GET'])
@token_required
def get_databarang():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        status_filter = request.args.get("status")
        if status_filter:
            cursor.execute("SELECT * FROM databarang WHERE status=%s", (status_filter,))
        else:
            cursor.execute("SELECT * FROM databarang")

        rows = cursor.fetchall()
        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@app.route('/ipsrsbarang', methods=['GET'])
@token_required
def get_ipsrsbarang():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        status_filter = request.args.get("status")
        if status_filter:
            cursor.execute("SELECT * FROM ipsrsbarang WHERE status=%s", (status_filter,))
        else:
            cursor.execute("SELECT * FROM ipsrsbarang")

        rows = cursor.fetchall()
        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@app.route('/penjab', methods=['GET'])
@token_required
def get_penjab():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        status_filter = request.args.get("status")
        if status_filter:
            cursor.execute("SELECT * FROM penjab WHERE status=%s", (status_filter,))
        else:
            cursor.execute("SELECT * FROM penjab")

        rows = cursor.fetchall()
        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@app.route('/departemen', methods=['GET'])
@token_required
def get_departemen():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM departemen")
        rows = cursor.fetchall()

        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@app.route('/dokter', methods=['GET'])
@token_required
def get_dokter():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM dokter")
        rows = cursor.fetchall()

        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@app.route('/jnj_jabatan', methods=['GET'])
@token_required
def get_jnj_jabatan():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM jnj_jabatan")
        rows = cursor.fetchall()

        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()



@app.route('/kamar', methods=['GET'])
@token_required
def get_kamar():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT k.kd_kamar, k.kd_bangsal, b.nm_bangsal, k.trf_kamar, 
                   k.status, k.kelas, k.statusdata
            FROM kamar k
            LEFT JOIN bangsal b ON k.kd_bangsal = b.kd_bangsal
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@app.route('/kategori_barang', methods=['GET'])
@token_required
def get_kategori_barang():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        query = "SELECT kode, nama FROM kategori_barang"
        cursor.execute(query)
        rows = cursor.fetchall()

        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@app.route('/stts_kerja', methods=['GET'])
@token_required
def get_stts_kerja():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        query = "SELECT stts, ktg, indek FROM stts_kerja"
        cursor.execute(query)
        rows = cursor.fetchall()

        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@app.route('/poliklinik', methods=['GET'])
@token_required
def get_poliklinik():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        query = "SELECT kd_poli, nm_poli, registrasi, registrasilama, status FROM poliklinik"
        cursor.execute(query)
        rows = cursor.fetchall()

        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@app.route('/kodesatuan', methods=['GET'])
@token_required
def get_kodesatuan():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        query = "SELECT kode_sat, satuan FROM kodesatuan"
        cursor.execute(query)
        rows = cursor.fetchall()

        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@app.route('/datasuplier', methods=['GET'])
@token_required
def get_datasuplier():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT kode_suplier, nama_suplier, alamat, kota, no_telp, nama_bank, rekening
            FROM datasuplier
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@app.route('/ipsrssuplier', methods=['GET'])
@token_required
def get_ipsrssuplier():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT kode_suplier, nama_suplier, alamat, kota, no_telp, nama_bank, rekening
            FROM ipsrssuplier
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


@app.route('/billing', methods=['GET'])
@token_required
def get_billing():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT noindex, no_rawat, tgl_byr, no, nm_perawatan, pemisah, 
                   biaya, jumlah, tambahan, totalbiaya, status
            FROM billing
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


@app.route('/booking_operasi', methods=['GET'])
@token_required
def get_booking_operasi():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT no_rawat, kode_paket, tanggal, jam_mulai, jam_selesai,
                   status, kd_dokter, kd_ruang_ok
            FROM booking_operasi
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


@app.route('/surat_pemesanan_medis', methods=['GET'])
@token_required
def get_surat_pemesanan_medis():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT no_pemesanan, kode_suplier, nip, tanggal, total1, potongan,
                   total2, ppn, meterai, tagihan, status
            FROM surat_pemesanan_medis
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


@app.route('/detail_surat_pemesanan_medis', methods=['GET'])
@token_required
def get_detail_surat_pemesanan_medis():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT no_pemesanan, kode_brng, kode_sat, jumlah, h_pesan, subtotal,
                   dis, besardis, total, jumlah2
            FROM detail_surat_pemesanan_medis
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


@app.route('/surat_pemesanan_non_medis', methods=['GET'])
@token_required
def get_surat_pemesanan_non_medis():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT no_pemesanan, kode_suplier, nip, tanggal, subtotal, potongan,
                   total, ppn, meterai, tagihan, status
            FROM surat_pemesanan_non_medis
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


@app.route('/detail_surat_pemesanan_non_medis', methods=['GET'])
@token_required
def get_detail_surat_pemesanan_non_medis():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT no_pemesanan, kode_brng, kode_sat, jumlah, h_pesan,
                   subtotal, dis, besardis, total
            FROM detail_surat_pemesanan_non_medis
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


@app.route('/pegawai', methods=['GET'])
@token_required
def get_pegawai():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT id, nik, nama, jk, jbtn, jnj_jabatan, kode_kelompok,
                   kode_resiko, kode_emergency, departemen, bidang,
                   stts_wp, stts_kerja, npwp, pendidikan, gapok,
                   tmp_lahir, tgl_lahir, alamat, kota, mulai_kerja, ms_kerja,
                   indexins, bpd, rekening, stts_aktif, wajibmasuk, pengurang,
                   indek, mulai_kontrak, cuti_diambil, dankes, photo, no_ktp
            FROM pegawai
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


@app.route('/pasien', methods=['GET'])
@token_required
def get_pasien():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT no_rkm_medis, nm_pasien, no_ktp, jk, tmp_lahir, tgl_lahir,
                   nm_ibu, alamat, gol_darah, pekerjaan, stts_nikah, agama,
                   tgl_daftar, no_tlp, umur, pnd, keluarga, namakeluarga,
                   kd_pj, no_peserta, kd_kel, kd_kec, kd_kab, pekerjaanpj,
                   alamatpj, kelurahanpj, kecamatanpj, kabupatenpj,
                   perusahaan_pasien, suku_bangsa, bahasa_pasien, cacat_fisik,
                   email, nip, kd_prop, propinsipj
            FROM pasien
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


@app.route('/paket_operasi', methods=['GET'])
@token_required
def get_paket_operasi():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT kode_paket, nm_perawatan, kategori, operator1, operator2, operator3,
                   asisten_operator1, asisten_operator2, asisten_operator3, instrumen,
                   dokter_anak, perawaat_resusitas, dokter_anestesi, asisten_anestesi,
                   asisten_anestesi2, bidan, bidan2, bidan3, perawat_luar,
                   sewa_ok, alat, akomodasi, bagian_rs, omloop, omloop2, omloop3,
                   omloop4, omloop5, sarpras, dokter_pjanak, dokter_umum, kd_pj,
                   status, kelas
            FROM paket_operasi
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        result = [{k: serialize(v) for k, v in row.items()} for row in rows]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5000)

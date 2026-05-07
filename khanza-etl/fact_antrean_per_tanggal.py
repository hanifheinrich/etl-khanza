import requests
import hashlib
import hmac
import base64
import time
from datetime import datetime, timedelta
import lzstring
from Crypto.Cipher import AES
import json
import mysql.connector

CONS_ID = ''
SECRET_KEY = ''
USER_KEY = ''

db = mysql.connector.connect(
    host="",
    user="",
    password="",
    database=""
)
cursor = db.cursor()

def get_headers():
    timestamp = str(int(time.time()))
    message = CONS_ID + "&" + timestamp
    signature = base64.b64encode(
        hmac.new(
            SECRET_KEY.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).digest()
    ).decode()
    headers = {
        'X-cons-id': CONS_ID,
        'X-timestamp': timestamp,
        'X-signature': signature,
        'user_key': USER_KEY,
        'Content-Type': 'application/json'
    }
    return headers, timestamp

def decrypt(key, txt_enc):
    x = lzstring.LZString()
    key_hash = hashlib.sha256(key.encode('utf-8')).digest()
    mode = AES.MODE_CBC
    decryptor = AES.new(key_hash[0:32], mode, IV=key_hash[0:16])
    plain = decryptor.decrypt(base64.b64decode(txt_enc))
    return x.decompressFromEncodedURIComponent(plain.decode('utf-8'))

def insert_to_db(data, tanggal):
    for row in data:
        try:
            sql = """
            INSERT INTO fact_antrol
            (kodebooking, tanggal, kodepoli, kodedokter, jampraktek, nik, nokapst, nohp, norekammedis, jeniskunjungan, 
            nomorreferensi, sumberdata, ispeserta, noantrean, estimasidilayani, createdtime, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                kodepoli = VALUES(kodepoli),
                kodedokter = VALUES(kodedokter),
                jampraktek = VALUES(jampraktek),
                nik = VALUES(nik),
                nokapst = VALUES(nokapst),
                nohp = VALUES(nohp),
                norekammedis = VALUES(norekammedis),
                jeniskunjungan = VALUES(jeniskunjungan),
                nomorreferensi = VALUES(nomorreferensi),
                sumberdata = VALUES(sumberdata),
                ispeserta = VALUES(ispeserta),
                noantrean = VALUES(noantrean),
                estimasidilayani = VALUES(estimasidilayani),
                createdtime = VALUES(createdtime),
                status = VALUES(status)
            """
            values = (
                row.get("kodebooking"),
                tanggal,
                row.get("kodepoli"),
                row.get("kodedokter"),
                row.get("jampraktek"),
                row.get("nik"),
                row.get("nokapst"),
                row.get("nohp"),
                row.get("norekammedis"),
                row.get("jeniskunjungan"),
                row.get("nomorreferensi"),
                row.get("sumberdata"),
                row.get("ispeserta"),
                row.get("noantrean"),
                row.get("estimasidilayani"),
                row.get("createdtime"),
                row.get("status")
            )
            cursor.execute(sql, values)
        except Exception as e:
            print(f"Gagal upsert data ke DB: {e}")
    db.commit()

today = datetime.today()
start_date = today - timedelta(days=60)
end_date = today
current_date = start_date

while current_date <= end_date:
    date_str = current_date.strftime("%Y-%m-%d")
    url = f"https://apijkn.bpjs-kesehatan.go.id/antreanrs/antrean/pendaftaran/tanggal/{date_str}"

    try:
        headers, timestamp = get_headers()
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict) and 'response' in data:
                encrypted = data['response']
                try:
                    if isinstance(encrypted, str):
                        key = CONS_ID + SECRET_KEY + timestamp
                        decrypted = decrypt(key, encrypted)
                    elif isinstance(encrypted, list):
                        for obj in encrypted:
                            ts_obj = obj.get('timestamp')
                            enc_text = obj.get('encrypted_data')
                            if ts_obj and enc_text:
                                key = CONS_ID + SECRET_KEY + ts_obj
                                decrypted = decrypt(key, enc_text)
                    else:
                        print(f"[{date_str}] Format tidak dikenali.")
                        current_date += timedelta(days=1)
                        continue

                    json_data = json.loads(decrypted)
                    if isinstance(json_data, list):
                        insert_to_db(json_data, date_str)
                        print(f"[{date_str}] Data berhasil di-upsert.")
                    else:
                        print(f"[{date_str}] Format JSON tidak sesuai: {json_data}")
                except Exception as e:
                    print(f"[{date_str}] Gagal dekripsi/parsing: {e}")
            else:
                print(f"[{date_str}] Tidak ada field 'response'")
        else:
            print(f"[{date_str}] Gagal HTTP {response.status_code}: {response.text}")
    except Exception as e:
        print(f"[{date_str}] Error: {e}")
    
    current_date += timedelta(days=1)
    time.sleep(1)

cursor.close()
db.close()
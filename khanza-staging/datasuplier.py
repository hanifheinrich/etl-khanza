import requests
import mysql.connector
from datetime import datetime, timedelta

API_URL = ""
API_USERNAME = ""
API_PASSWORD = ""

target_config = {
    'host': '',
    'user': '',
    'password': '',
    'database': ''
}

auth_url = f"{API_URL}/token"
auth_payload = {"username": API_USERNAME, "password": API_PASSWORD}
auth_response = requests.post(auth_url, json=auth_payload)
auth_response.raise_for_status()
token = auth_response.json().get("token")
headers = {"Authorization": f"Bearer {token}"}

conn = mysql.connector.connect(**target_config)
cursor = conn.cursor()

insert_query = """
    INSERT INTO billing (
        noindex, no_rawat, tgl_byr, no, nm_perawatan,
        pemisah, biaya, jumlah, tambahan, totalbiaya, status
    ) VALUES (
        %(noindex)s, %(no_rawat)s, %(tgl_byr)s, %(no)s, %(nm_perawatan)s,
        %(pemisah)s, %(biaya)s, %(jumlah)s, %(tambahan)s, %(totalbiaya)s, %(status)s
    )
    ON DUPLICATE KEY UPDATE
        nm_perawatan = VALUES(nm_perawatan),
        biaya = VALUES(biaya),
        jumlah = VALUES(jumlah),
        totalbiaya = VALUES(totalbiaya),
        status = VALUES(status)
"""

def daterange(start_date, end_date):
    current = start_date.replace(day=1)
    while current <= end_date:
        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        yield current, min(next_month - timedelta(days=1), end_date)
        current = next_month

start = datetime(2019, 10, 14)
end = datetime(2019, 12, 31)

for start_date, end_date in daterange(start, end):
    print(f"Ambil data: {start_date.date()} s.d. {end_date.date()}")
    params = {"start": start_date.strftime("%Y-%m-%d"), "end": end_date.strftime("%Y-%m-%d")}
    
    try:
        resp = requests.get(API_URL, headers=headers, params=params, timeout=120)
        resp.raise_for_status()
        rows = resp.json()
        
        if not rows:
            print("➡️ Tidak ada data untuk periode ini")
            continue
        
        cursor.executemany(insert_query, rows)
        conn.commit()
        print(f"✅ {len(rows)} baris berhasil diinsert untuk {start_date.strftime('%Y-%m')}")
    
    except Exception as e:
        print(f"❌ Error periode {start_date.date()} - {end_date.date()}: {e}")

cursor.close()
conn.close()
print("Selesai.")
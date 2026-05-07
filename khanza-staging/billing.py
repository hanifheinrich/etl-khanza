import requests
import mysql.connector
from datetime import datetime, timedelta

LOGIN_URL = ""
API_URL = ""
API_USERNAME = ""
API_PASSWORD = ""

DB_CONFIG = {
    'host': '',
    'user': '',
    'password': '',
    'database': ''
}

TABLE_NAME = "billing"
BATCH_SIZE = 500

def insert_to_db(data_batch):
    if not data_batch:
        return
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    sql = f"""INSERT INTO {TABLE_NAME} 
    (noindex, no_rawat, tgl_byr, no, nm_perawatan, pemisah, biaya, jumlah, tambahan, totalbiaya, status)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    
    values = []
    for d in data_batch:
        values.append((
            d.get('noindex'),
            d.get('no_rawat'),
            d.get('tgl_byr'),
            d.get('no'),
            d.get('nm_perawatan'),
            d.get('pemisah'),
            d.get('biaya'),
            d.get('jumlah'),
            d.get('tambahan'),
            d.get('totalbiaya'),
            d.get('status')
        ))
    
    cursor.executemany(sql, values)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"{len(values)} record inserted.")

def get_token():
    resp = requests.post(LOGIN_URL, json={"username": API_USERNAME, "password": API_PASSWORD})
    resp.raise_for_status()
    return resp.json().get("token")

START_DATE = datetime(2023, 1, 8)
END_DATE = datetime(2025, 9, 10)
delta = timedelta(days=1)

token = get_token()
current_start = START_DATE

while current_start <= END_DATE:
    current_end = current_start + delta - timedelta(days=1)
    if current_end > END_DATE:
        current_end = END_DATE
    
    params = {
        "start": current_start.strftime("%Y-%m-%d"),
        "end": current_end.strftime("%Y-%m-%d")
    }
    
    headers = {"Authorization": f"Bearer {token}"}
    print(f"Fetching data {params['start']} to {params['end']}...")
    
    try:
        response = requests.get(API_URL, params=params, headers=headers)
        
        if response.status_code == 401:
            print("Token expired, fetching new token...")
            token = get_token()
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(API_URL, params=params, headers=headers)
        
        response.raise_for_status()
        data = response.json()
        
        for i in range(0, len(data), BATCH_SIZE):
            insert_to_db(data[i:i+BATCH_SIZE])
            
    except Exception as e:
        print(f"Error fetching or inserting data: {e}")
    
    current_start = current_end + timedelta(days=1)

print("All data fetched and inserted.")
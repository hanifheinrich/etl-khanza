import mysql.connector
from mysql.connector import Error
import json

def load_config(filename='config.txt'):
    with open(filename, 'r') as f:
        return json.load(f)

def get_connection(config):
    return mysql.connector.connect(**config)

def sync_bangsal():
    try:
        config = load_config()
        source_conn = get_connection(config['source_config'])
        target_conn = get_connection(config['target_config'])
        
        source_cursor = source_conn.cursor(dictionary=True)
        target_cursor = target_conn.cursor(dictionary=True)

        source_cursor.execute("SELECT kd_bangsal, nm_bangsal, status FROM bangsal")
        rows = source_cursor.fetchall()

        changed_rows = []

        for row in rows:
            target_cursor.execute(
                "SELECT nm_bangsal, status FROM dim_bangsal WHERE kd_bangsal = %s",
                (row['kd_bangsal'],)
            )
            existing = target_cursor.fetchone()

            if not existing or existing['nm_bangsal'] != row['nm_bangsal'] or existing['status'] != row['status']:
                query = """
                    INSERT INTO dim_bangsal (kd_bangsal, nm_bangsal, status)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        nm_bangsal = VALUES(nm_bangsal),
                        status = VALUES(status)
                """
                target_cursor.execute(query, (row['kd_bangsal'], row['nm_bangsal'], row['status']))
                changed_rows.append(row['kd_bangsal'])

        target_conn.commit()

        if changed_rows:
            print(f"Update: {len(changed_rows)} baris di dim_bangsal.")
        else:
            print("Status: Tidak ada perubahan.")

    except (Error, FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Failure: {e}")

    finally:
        if 'source_conn' in locals() and source_conn.is_connected():
            source_cursor.close()
            source_conn.close()
        if 'target_conn' in locals() and target_conn.is_connected():
            target_cursor.close()
            target_conn.close()

if __name__ == "__main__":
    sync_bangsal()
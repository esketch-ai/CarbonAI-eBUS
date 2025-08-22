import sys
import psycopg2

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

def connect_to_db(db_params):
    """
    PostgreSQL 데이터베이스에 연결하는 함수.
    :param db_params: host, dbname, user, password, port를 포함하는 딕셔너리
    :return: connection 객체 또는 연결 실패 시 None
    """
    conn = None
    try:
        print("⏳ 데이터베이스에 연결을 시도합니다...")
        conn = psycopg2.connect(**db_params)
        print("✅ 데이터베이스 연결에 성공했습니다!")
    except psycopg2.OperationalError as e:
        print(f"❌ 데이터베이스 연결 오류: {e}")
    return conn

def close_db_connection(conn):
    """
    데이터베이스 연결을 닫는 함수.
    """
    if conn:
        conn.close()
        print("\n✅ 데이터베이스 연결을 닫았습니다.")
        
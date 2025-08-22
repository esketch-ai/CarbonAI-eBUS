import psycopg2
from psycopg2 import sql
from db_config import db_connection_params
from db_utils import connect_to_db, close_db_connection

def create_bus_monthly_fuel_data_table(conn):
    """
    bus_monthly_fuel_data 테이블을 생성하는 함수.
    """
    if not conn: return

    create_table_query = """
    CREATE TABLE IF NOT EXISTS bus_monthly_fuel_data (
        vehicle_plate_no VARCHAR(255) NOT NULL,
        record_year_month VARCHAR(6) NOT NULL,
        fuel_consumption_l DOUBLE PRECISION,
        distance_km DOUBLE PRECISION,
        PRIMARY KEY (vehicle_plate_no, record_year_month)
    );
    """
    with conn.cursor() as cur:
        try:
            print("⏳ 'bus_monthly_fuel_data' 테이블을 생성합니다...")
            cur.execute(create_table_query)
            conn.commit()
            print("✅ 'bus_monthly_fuel_data' 테이블이 성공적으로 생성되었거나 이미 존재합니다.")
        except psycopg2.Error as e:
            print(f"❌ 'bus_monthly_fuel_data' 테이블 생성 오류: {e}")
            conn.rollback()

def main():
    """
    메인 실행 함수.
    """
    print("--- [DB 테이블 생성 스크립트 시작] ---")
    
    db_params = db_connection_params
    conn = connect_to_db(db_params)
    
    if conn:
        create_bus_monthly_fuel_data_table(conn)
        close_db_connection(conn)
    
    print("--- [DB 테이블 생성 스크립트 완료] ---")

if __name__ == '__main__':
    main()

import psycopg2
from psycopg2 import sql
from db_config import db_connection_params
from db_utils import connect_to_db, close_db_connection

def execute_query(conn, query, message="쿼리 실행"):
    """주어진 쿼리를 실행하는 함수."""
    if not conn: return
    with conn.cursor() as cur:
        try:
            cur.execute(query)
            conn.commit()
            print(f"✅ {message} 성공적으로 실행되었습니다.")
        except psycopg2.Error as e:
            print(f"❌ {message} 오류: {e}")
            conn.rollback()

def create_tables(conn):
    """모든 테이블을 삭제하고 새로 생성하는 함수."""
    print("\n--- 기존 테이블 삭제 및 새 테이블 생성 시작 ---")

    # 기존 테이블 삭제 (외래 키 제약 조건 역순으로 삭제)
    drop_queries = [
        "DROP TABLE IF EXISTS bus_emission_reductions CASCADE;",
        "DROP TABLE IF EXISTS bus_baseline_parameters CASCADE;",
        "DROP TABLE IF EXISTS bus_driving_records CASCADE;",
        "DROP TABLE IF EXISTS bus_monthly_fuel_data CASCADE;",
        "DROP TABLE IF EXISTS bus_vehicle_master CASCADE;"
    ]
    for query in drop_queries:
        execute_query(conn, query, message=f"테이블 삭제: {query.split()[3]}")

    # 1. bus_vehicle_master 테이블 생성
    create_vehicle_master_query = """
    CREATE TABLE bus_vehicle_master (
        vehicle_plate_no VARCHAR(20) PRIMARY KEY,
        company_name VARCHAR(50) NOT NULL,
        sequence_no INT,
        business_type VARCHAR(20) NOT NULL,
        model_year INT,
        ev_registration_date DATE,
        original_fuel_type VARCHAR(20),
        chassis_number VARCHAR(50) UNIQUE,
        replaced_by_ev_plate_no VARCHAR(20),
        original_ice_plate_no VARCHAR(20),

        FOREIGN KEY (replaced_by_ev_plate_no) REFERENCES bus_vehicle_master(vehicle_plate_no),
        FOREIGN KEY (original_ice_plate_no) REFERENCES bus_vehicle_master(vehicle_plate_no)
    );
    """
    execute_query(conn, create_vehicle_master_query, message="'bus_vehicle_master' 테이블 생성")

    # 2. bus_driving_records 테이블 생성 (변경된 스키마)
    create_driving_records_query = """
    CREATE TABLE bus_driving_records (
        id SERIAL PRIMARY KEY,
        vehicle_plate_no VARCHAR(20) NOT NULL,
        year_month VARCHAR(7) NOT NULL,
        operating_days INT,
        driving_distance_km FLOAT,
        fuel_quantity_l FLOAT,
        charging_amount_kwh FLOAT,

        UNIQUE (vehicle_plate_no, year_month),
        FOREIGN KEY (vehicle_plate_no) REFERENCES bus_vehicle_master(vehicle_plate_no)
    );
    """
    execute_query(conn, create_driving_records_query, message="'bus_driving_records' 테이블 생성")

    # 3. bus_monthly_fuel_data 테이블 생성
    create_monthly_fuel_data_query = """
    CREATE TABLE bus_monthly_fuel_data (
        vehicle_plate_no VARCHAR(20) NOT NULL,
        record_year_month VARCHAR(6) NOT NULL,
        fuel_consumption_l DOUBLE PRECISION,
        distance_km DOUBLE PRECISION,
        PRIMARY KEY (vehicle_plate_no, record_year_month),
        FOREIGN KEY (vehicle_plate_no) REFERENCES bus_vehicle_master(vehicle_plate_no)
    );
    """
    execute_query(conn, create_monthly_fuel_data_query, message="'bus_monthly_fuel_data' 테이블 생성")

    # 3. bus_baseline_parameters 테이블 생성 (변경된 스키마)
    create_baseline_parameters_query = """
    CREATE TABLE bus_baseline_parameters (
        vehicle_plate_no VARCHAR(20) PRIMARY KEY,
        baseline_start_ym VARCHAR(7),
        baseline_end_ym VARCHAR(7),
        months_of_operation INT,
        avg_annual_distance_km FLOAT,
        avg_annual_fuel_l FLOAT,
        fuel_per_km FLOAT,
        baseline_co2_emission_kg DOUBLE PRECISION NOT NULL,
        baseline_emission_factor DOUBLE PRECISION NOT NULL,

        FOREIGN KEY (vehicle_plate_no) REFERENCES bus_vehicle_master(vehicle_plate_no)
    );
    """
    execute_query(conn, create_baseline_parameters_query, message="'bus_baseline_parameters' 테이블 생성")

    # 4. bus_emission_reductions 테이블 생성 (변경된 스키마)
    create_emission_reductions_query = """
    CREATE TABLE bus_emission_reductions (
        vehicle_plate_no VARCHAR(20) PRIMARY KEY,
        calculated_year INT NOT NULL,
        baseline_annual_fuel_l FLOAT,
        baseline_emission_factor FLOAT,
        baseline_co2_emission_kg FLOAT,
        ev_actual_co2_emission_kg FLOAT,
        co2_reduction_kg FLOAT,
        reduction_category VARCHAR(50) NOT NULL,

        FOREIGN KEY (vehicle_plate_no) REFERENCES bus_vehicle_master(vehicle_plate_no)
    );
    """
    execute_query(conn, create_emission_reductions_query, message="'bus_emission_reductions' 테이블 생성")

    print("--- 기존 테이블 삭제 및 새 테이블 생성 완료 ---")

def main():
    """메인 실행 함수."""
    print("\n--- [파일 00] 데이터베이스 스키마 관리 시작 ---")
    
    db_params = db_connection_params  # db_config.py에서 가져온 DB 연결 정보
    conn = connect_to_db(db_params)
    
    if conn:
        create_tables(conn)
        close_db_connection(conn)

if __name__ == '__main__':
    main()

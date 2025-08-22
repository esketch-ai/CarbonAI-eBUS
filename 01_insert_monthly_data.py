import pandas as pd
import numpy as np
import random
from datetime import datetime
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from io import StringIO
import os
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

def insert_vehicle_master_data(conn, df):
    """
    bus_vehicle_master 테이블에 차량 마스터 데이터를 저장하거나 업데이트하는 함수.
    :param conn: psycopg2 connection 객체
    :param df: 저장할 차량 마스터 데이터프레임
    """
    if not conn or df.empty: return

    # ev_registration_date의 NaT 값을 None으로 변환하여 DB의 DATE 타입에 맞춤
    df_copy = df.copy()
    if 'ev_registration_date' in df_copy.columns:
        df_copy['ev_registration_date'] = df_copy['ev_registration_date'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)

    cols = df_copy.columns.tolist()
    values = [tuple(row) for row in df_copy.to_numpy()]

    update_cols = [col for col in cols if col != 'vehicle_plate_no']
    update_statement = ", ".join([f"{col}=EXCLUDED.{col}" for col in update_cols])

    insert_query = sql.SQL("""
        INSERT INTO bus_vehicle_master ({}) 
        VALUES %s
        ON CONFLICT (vehicle_plate_no) DO UPDATE SET {}
    """).format(
        sql.SQL(', ').join(map(sql.Identifier, cols)),
        sql.SQL(update_statement)
    )

    with conn.cursor() as cur:
        try:
            print("⏳ 'bus_vehicle_master' 테이블에 차량 마스터 데이터를 저장/업데이트합니다...")
            execute_values(cur, insert_query, values)
            conn.commit()
            print(f"✅ {cur.rowcount}개의 차량 마스터 레코드가 성공적으로 저장/업데이트되었습니다.")
        except psycopg2.Error as e:
            print(f"❌ 차량 마스터 데이터 저장 오류: {e}")
            conn.rollback()

def insert_driving_records_data(conn, df):
    """
    DataFrame을 PostgreSQL의 bus_driving_records에 저장하거나 업데이트하는 함수 (execute_values 사용).
    :param conn: psycopg2 connection 객체
    :param df: 저장할 데이터프레임 (vehicle_plate_no, year_month, operating_days, driving_distance_km, fuel_quantity_l, charging_amount_kwh)
    """
    if not conn or df.empty: return
    
    cols = [
        'vehicle_plate_no', 'year_month', 'operating_days',
        'driving_distance_km', 'fuel_quantity_l', 'charging_amount_kwh'
    ]
    values = [tuple(row) for row in df[cols].to_numpy()]

    update_cols = [col for col in cols if col not in ['vehicle_plate_no', 'year_month']]
    update_statement = ", ".join([f"{col}=EXCLUDED.{col}" for col in update_cols])

    insert_query = sql.SQL("""
        INSERT INTO bus_driving_records ({}) 
        VALUES %s
        ON CONFLICT (vehicle_plate_no, year_month) DO UPDATE SET {}
    """).format(
        sql.SQL(', ').join(map(sql.Identifier, cols)),
        sql.SQL(update_statement)
    )

    with conn.cursor() as cur:
        try:
            print("⏳ 'bus_driving_records' 테이블에 월별 데이터를 저장/업데이트합니다...")
            execute_values(cur, insert_query, values)
            conn.commit()
            print(f"✅ {cur.rowcount}개의 월별 운행 기록 레코드가 성공적으로 저장/업데이트되었습니다.")
        except psycopg2.Error as e:
            print(f"❌ 월별 운행 기록 데이터 저장 오류: {e}")
            conn.rollback()

def insert_monthly_fuel_data(conn, df):
    """
    DataFrame을 PostgreSQL의 bus_monthly_fuel_data에 저장하거나 업데이트하는 함수 (execute_values 사용).
    :param conn: psycopg2 connection 객체
    :param df: 저장할 데이터프레임 (vehicle_plate_no, record_year_month, fuel_consumption_l, distance_km)
    """
    if not conn or df.empty: return
    
    cols = [
        'vehicle_plate_no', 'record_year_month', 'fuel_consumption_l', 'distance_km'
    ]

    # NaN 값을 None으로 변환하여 DB의 DOUBLE PRECISION 타입에 맞춤
    df_copy = df[cols].copy()
    for col in ['fuel_consumption_l', 'distance_km']:
        df_copy[col] = df_copy[col].replace({np.nan: None})

    values = [tuple(row) for row in df_copy.to_numpy()]

    update_cols = [col for col in cols if col not in ['vehicle_plate_no', 'record_year_month']]
    update_statement = ", ".join([f"{col}=EXCLUDED.{col}" for col in update_cols])

    insert_query = sql.SQL("""
        INSERT INTO bus_monthly_fuel_data ({}) 
        VALUES %s
        ON CONFLICT (vehicle_plate_no, record_year_month) DO UPDATE SET {}
    """).format(
        sql.SQL(', ').join(map(sql.Identifier, cols)),
        sql.SQL(update_statement)
    )
    
    with conn.cursor() as cur:
        try:
            print("⏳ 'bus_monthly_fuel_data' 테이블에 월별 연료 데이터를 저장/업데이트합니다...")
            execute_values(cur, insert_query, values)
            conn.commit()
            print(f"✅ {cur.rowcount}개의 월별 연료 기록 레코드가 성공적으로 삽입/업데이트되었습니다.")
        except psycopg2.Error as e:
            print(f"❌ 월별 연료 기록 데이터 저장 오류: {e}")
            conn.rollback()

def main():
    """메인 실행 함수"""
    print("--- [파일 1] 월별 운행 기록 데이터 생성 및 DB 적재 시작 ---")

    # --- 가상 데이터 생성 ---
    num_total_vehicles = 30 # 전체 차량 수 (EV + ICE)
    num_replacement_evs = 10 # 대체도입 전기버스 수 (이 수만큼 베이스라인 대상 내연기관 차량이 필요)
    start_year, end_year = 2019, 2023 # Generate 5 years of data (2019-2023)

    company_names = ['가상교통', '미래운수', '희망버스', '데이터교통']
    fuel_types = ['CNG', '경유']

    all_vehicle_plate_nos = [f'서울74사{random.randint(1000, 9999)}' for _ in range(num_total_vehicles * 2)] # 충분히 많은 차량번호 생성
    random.shuffle(all_vehicle_plate_nos)

    vehicle_master_data = []
    monthly_records_data = []
    used_plate_nos = set()
    
    # 베이스라인 계산에 필요한 내연기관 차량 번호 풀 생성
    # 이 차량들은 충분한 월별 데이터를 가지도록 보장
    baseline_eligible_ice_plate_nos = []
    for _ in range(num_replacement_evs):
        ice_plate_no = all_vehicle_plate_nos.pop()
        while ice_plate_no in used_plate_nos:
            ice_plate_no = all_vehicle_plate_nos.pop()
        used_plate_nos.add(ice_plate_no)
        baseline_eligible_ice_plate_nos.append(ice_plate_no)

    # 대체도입 전기버스 생성
    replacement_ev_data = []
    for i in range(num_replacement_evs):
        ev_plate_no = all_vehicle_plate_nos.pop()
        while ev_plate_no in used_plate_nos:
            ev_plate_no = all_vehicle_plate_nos.pop()
        used_plate_nos.add(ev_plate_no)

        original_ice_plate_no = baseline_eligible_ice_plate_nos[i] # 베이스라인 대상 ICE 차량과 연결
        ice_fuel_type = random.choice(fuel_types) # 대체된 ICE의 연료 타입

        # 대체된 내연기관 버스 마스터 데이터 추가
        vehicle_master_data.append({
            'vehicle_plate_no': original_ice_plate_no,
            'company_name': random.choice(company_names), 
            'sequence_no': i + 1, 
            'business_type': '내연기관', 
            'model_year': random.randint(2015, 2018), # 베이스라인 기간을 위해 좀 더 오래된 연식
            'ev_registration_date': None,
            'original_fuel_type': ice_fuel_type, 
            'chassis_number': f'ICE_CHASSIS{random.randint(100000, 999999)}',
            'replaced_by_ev_plate_no': ev_plate_no, 
            'original_ice_plate_no': None
        })

        # 전기버스 마스터 데이터 추가
        replacement_ev_data.append({
            'vehicle_plate_no': ev_plate_no,
            'company_name': random.choice(company_names),
            'sequence_no': i + 1,
            'business_type': '대체도입',
            'model_year': random.randint(2024, 2025),
            'ev_registration_date': f'{random.randint(2024, 2025)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}',
            'original_fuel_type': ice_fuel_type, # 대체 EV의 original_fuel_type은 대체된 ICE의 연료 타입
            'chassis_number': f'EV_CHASSIS{random.randint(100000, 999999)}',
            'replaced_by_ev_plate_no': None, 
            'original_ice_plate_no': original_ice_plate_no
        })
    
    # 나머지 차량 (신규도입 EV 또는 일반 내연기관) 생성
    num_other_vehicles = num_total_vehicles - num_replacement_evs
    for i in range(num_other_vehicles):
        vehicle_plate_no = all_vehicle_plate_nos.pop()
        while vehicle_plate_no in used_plate_nos:
            vehicle_plate_no = all_vehicle_plate_nos.pop()
        used_plate_nos.add(vehicle_plate_no)

        company_name = random.choice(company_names)
        sequence_no = num_replacement_evs + i + 1
        business_type = random.choice(['신규도입', '내연기관']) # 신규도입은 EV, 내연기관은 ICE
        model_year = random.randint(2015, 2025)
        chassis_number = f'CHASSIS{random.randint(100000, 999999)}'

        ev_registration_date = None
        original_fuel_type = None
        replaced_by_ev_plate_no = None
        original_ice_plate_no = None

        if business_type == '신규도입': # 신규도입 EV
            ev_registration_date = f'{random.randint(2024, 2025)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}'
            original_fuel_type = None # 신규 EV는 기존 연료 타입이 없음
        else: # 일반 내연기관 차량
            original_fuel_type = random.choice(fuel_types)
            model_year = random.randint(2015, 2020) # 일반 ICE는 좀 더 오래된 연식

        vehicle_master_data.append({
            'vehicle_plate_no': vehicle_plate_no,
            'company_name': company_name,
            'sequence_no': sequence_no,
            'business_type': business_type,
            'model_year': model_year,
            'ev_registration_date': ev_registration_date,
            'original_fuel_type': original_fuel_type,
            'chassis_number': chassis_number,
            'replaced_by_ev_plate_no': replaced_by_ev_plate_no,
            'original_ice_plate_no': original_ice_plate_no
        })
    
    # 모든 차량 마스터 데이터 합치기
    vehicle_master_df = pd.DataFrame(vehicle_master_data + replacement_ev_data)

    # 월별 운행 기록 데이터 생성
    for _, vehicle_row in vehicle_master_df.iterrows():
        vehicle_plate_no = vehicle_row['vehicle_plate_no']
        is_ev = vehicle_row['ev_registration_date'] is not None
        original_fuel_type = vehicle_row['original_fuel_type']
        model_year = vehicle_row['model_year']

        # 차량 연식과 월별 계절성을 반영한 운행/연료 데이터 생성
        model_year_efficiency_factor = 1 + (datetime.now().year - model_year) * 0.005
        seasonal_driving_factor = {
            1: 0.95, 2: 0.95, # 겨울철 운행 소폭 감소
            7: 1.05, 8: 0.9,  # 여름 휴가철 패턴
            12: 1.05 # 연말 특수
        }

        # 베이스라인 대상 ICE 차량은 5년치 데이터 보장
        if vehicle_plate_no in baseline_eligible_ice_plate_nos:
            date_range = pd.date_range(start=f'{start_year}-01-01', end=f'{end_year}-12-31', freq='MS')
            skip_chance = 0.0 # 베이스라인 대상은 결측치 거의 없음
        elif is_ev:
            # 전기차는 등록월 이후 데이터만 생성, 최소 1개월치 보장
            ev_reg_date = pd.to_datetime(vehicle_row['ev_registration_date'])
            # 등록월부터 현재까지의 데이터만 생성
            date_range = pd.date_range(start=ev_reg_date.strftime('%Y-%m-%d'), end=datetime.now().strftime('%Y-%m-%d'), freq='MS')
            if date_range.empty: # 최소 1개월치 데이터 보장
                date_range = pd.date_range(start=ev_reg_date.strftime('%Y-%m-%d'), periods=1, freq='MS')
            skip_chance = 0.05 # 전기차도 결측치 적게
        else: # 일반 내연기관 차량
            date_range = pd.date_range(start=f'{start_year}-01-01', end=f'{end_year}-12-31', freq='MS')
            skip_chance = 0.1 # 일반 내연기관은 기존처럼 10% 결측치

        for date in date_range:
            if random.random() < skip_chance:
                continue

            operating_days = random.randint(20, date.days_in_month)
            base_daily_distance = random.uniform(180, 250)
            distance = operating_days * base_daily_distance * seasonal_driving_factor.get(date.month, 1.0)
            
            fuel = np.nan
            charge = np.nan

            if is_ev:
                base_charge_efficiency = random.uniform(2.0, 3.0) 
                charge = distance / base_charge_efficiency if base_charge_efficiency > 0 else 0
            else: # 내연기관차 (original_fuel_type이 있는 경우)
                base_fuel_efficiency = random.uniform(0.4, 0.6) 
                fuel = distance * base_fuel_efficiency * model_year_efficiency_factor
            
            monthly_records_data.append({
                'vehicle_plate_no': vehicle_plate_no,
                'year_month': date.strftime('%Y%m'),
                'operating_days': operating_days,
                'driving_distance_km': distance,
                'fuel_quantity_l': fuel,
                'charging_amount_kwh': charge
            })
    
    monthly_records_df = pd.DataFrame(monthly_records_data)

    # 데이터 타입 변환 및 결측치 처리
    vehicle_master_df['model_year'] = pd.to_numeric(vehicle_master_df['model_year'], errors='coerce').fillna(0).astype(int)
    # ev_registration_date를 datetime 타입으로 변환
    vehicle_master_df['ev_registration_date'] = pd.to_datetime(vehicle_master_df['ev_registration_date'], errors='coerce')

    monthly_records_df['operating_days'] = pd.to_numeric(monthly_records_df['operating_days'], errors='coerce').fillna(0).astype(int)
    monthly_records_df['driving_distance_km'] = pd.to_numeric(monthly_records_df['driving_distance_km'], errors='coerce').fillna(0).astype(float)
    monthly_records_df['fuel_quantity_l'] = pd.to_numeric(monthly_records_df['fuel_quantity_l'], errors='coerce').fillna(0).astype(float)
    monthly_records_df['charging_amount_kwh'] = pd.to_numeric(monthly_records_df['charging_amount_kwh'], errors='coerce').fillna(0).astype(float)

    print("✅ 가상 데이터 생성 및 정제를 완료했습니다.")

    # --- 생성된 데이터를 엑셀 파일로 저장 ---
    try:
        # openpyxl 라이브러리가 있는지 확인
        import openpyxl
        output_dir = 'generated_data'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        excel_filename = f'generated_bus_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        excel_path = os.path.join(output_dir, excel_filename)

        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            vehicle_master_df.to_excel(writer, sheet_name='Vehicle_Master', index=False)
            monthly_records_df.to_excel(writer, sheet_name='Monthly_Records', index=False)
        print(f"✅ 생성된 데이터를 엑셀 파일로 저장했습니다: {excel_path}")

    except ImportError:
        print("\n⚠️ 'openpyxl' 라이브러리가 설치되지 않아 엑셀 파일로 저장할 수 없습니다.")
        print("   (엑셀 출력을 원하시면 'pip install openpyxl' 실행 후 다시 시도해주세요)\n")
    except Exception as e:
        print(f"❌ 엑셀 파일 저장 중 오류 발생: {e}")

    # --- DB 연결 및 작업 수행 ---
    db_params = db_connection_params  # db_config.py에서 가져온 DB 연결 정보
    conn = connect_to_db(db_params)
    
    if conn:
        # 1. bus_vehicle_master 테이블에 차량 마스터 데이터 적재
        insert_vehicle_master_data(conn, vehicle_master_df)

        # 2. bus_driving_records 테이블에 월별 운행 기록 데이터 적재
        insert_driving_records_data(conn, monthly_records_df)

        # 3. bus_monthly_fuel_data 테이블에 월별 연료 데이터 적재
        # fuel_quantity_l과 driving_distance_km만 선택하여 새로운 DataFrame 생성
        monthly_fuel_df = monthly_records_df[
            ['vehicle_plate_no', 'year_month', 'fuel_quantity_l', 'driving_distance_km']
        ].rename(columns={'year_month': 'record_year_month', 
                          'fuel_quantity_l': 'fuel_consumption_l',
                          'driving_distance_km': 'distance_km'})
        insert_monthly_fuel_data(conn, monthly_fuel_df)
        
        close_db_connection(conn)

if __name__ == '__main__':
    main()

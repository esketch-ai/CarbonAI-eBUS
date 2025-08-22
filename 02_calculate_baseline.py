import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from datetime import datetime
from db_config import db_connection_params
from db_utils import connect_to_db, close_db_connection
from constants import NET_CALORIFIC_VALUE, CO2_EMISSION_FACTOR, CNG_DENSITY_KG_PER_M3

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

def load_data_from_db(conn, table_name):
    """DB에서 지정된 테이블의 데이터를 불러와 DataFrame으로 반환하는 함수."""
    if not conn: return pd.DataFrame()
    print(f"⏳ '{table_name}' 테이블에서 데이터를 로드합니다...")
    try:
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql_query(query, conn)
        print(f"✅ {len(df)}개의 '{table_name}' 데이터를 성공적으로 로드했습니다.")
        return df
    except Exception as e:
        print(f"❌ '{table_name}' 데이터 로드 중 오류 발생: {e}")
        return pd.DataFrame()

def insert_or_update_baseline_data(conn, df):
    """
    베이스라인 데이터를 DB에 저장하거나 업데이트하는 함수 (ON CONFLICT ... DO UPDATE).
    :param conn: psycopg2 connection 객체
    :param df: 저장할 베이스라인 데이터프레임 (vehicle_plate_no, months_of_operation, avg_annual_distance_km, avg_annual_fuel_l, fuel_per_km)
    """
    if not conn or df.empty: return
    
    # bus_baseline_parameters 테이블의 컬럼명에 맞게 DataFrame 컬럼명 변경 (이미 영문으로 가정)
    cols = df.columns.tolist()
    values = [tuple(row) for row in df.to_numpy()]
    
    # ON CONFLICT ... DO UPDATE 쿼리 작성
    update_cols = [col for col in cols if col != 'vehicle_plate_no']
    update_statement = ", ".join([f"{col}=EXCLUDED.{col}" for col in update_cols])
    
    insert_query = sql.SQL("""
        INSERT INTO bus_baseline_parameters ({}) 
        VALUES %s
        ON CONFLICT (vehicle_plate_no) DO UPDATE SET {}
    """).format(
        sql.SQL(', ').join(map(sql.Identifier, cols)),
        sql.SQL(update_statement)
    )
    
    with conn.cursor() as cur:
        try:
            print("⏳ 'bus_baseline_parameters' 테이블에 데이터를 저장/업데이트합니다...")
            execute_values(cur, insert_query, values)
            conn.commit()
            print(f"✅ {cur.rowcount}개의 베이스라인 레코드가 성공적으로 저장/업데이트되었습니다.")
        except psycopg2.Error as e:
            print(f"❌ 베이스라인 데이터 저장 오류: {e}")
            conn.rollback()

def main():
    """메인 실행 함수."""
    print("\n--- [파일 2] 베이스라인 인자 계산 및 DB 적재 시작 ---")
    
    db_params = db_connection_params  # db_config.py에서 가져온 DB 연결 정보
    conn = connect_to_db(db_params)
    
    if conn:
        # 1. DB에서 월별 연료 데이터 및 차량 마스터 데이터 로드
        monthly_fuel_df = load_data_from_db(conn, 'bus_monthly_fuel_data')
        vehicle_master_df = load_data_from_db(conn, 'bus_vehicle_master')
        
        if monthly_fuel_df.empty or vehicle_master_df.empty:
            print("⚠️ 필요한 데이터(월별 연료 기록 또는 차량 마스터)가 없습니다. 01번 스크립트를 먼저 실행해주세요.")
            conn.close()
            return

        # 월별 연료 기록과 차량 마스터 정보를 조인
        # 베이스라인은 내연기관 차량에 대해서만 산정 (대체도입된 전기버스의 기존 내연기관 차량 포함)
        merged_df = pd.merge(monthly_fuel_df, vehicle_master_df, on='vehicle_plate_no', how='inner')
        
        # 내연기관 차량 또는 대체도입된 전기버스의 기존 내연기관 차량만 필터링
        ice_vehicles_for_baseline = merged_df[
            (merged_df['original_fuel_type'].isin(['CNG', '경유'])) | 
            (merged_df['business_type'] == '대체도입')
        ].copy()

        if ice_vehicles_for_baseline.empty:
            print("⚠️ 베이스라인을 계산할 내연기관 차량 데이터가 없습니다.")
            close_db_connection(conn)
            return

        print(f"✅ 베이스라인 계산 대상 내연기관 차량 {len(ice_vehicles_for_baseline['vehicle_plate_no'].unique())}대에 대한 데이터 {len(ice_vehicles_for_baseline)}개를 로드했습니다.")

        # 2. 베이스라인 계산을 위한 데이터 정제 및 계산
        # 'record_year_month'를 datetime으로 변환하여 정렬 및 기간 필터링 용이하게 함
        ice_vehicles_for_baseline['record_year_month_dt'] = pd.to_datetime(ice_vehicles_for_baseline['record_year_month'], format='%Y%m')
        ice_vehicles_for_baseline = ice_vehicles_for_baseline.sort_values(by=['vehicle_plate_no', 'record_year_month_dt'])

        baseline_data = []
        for vehicle_plate_no, group in ice_vehicles_for_baseline.groupby('vehicle_plate_no'):
            # 유효한 연료 소비량과 주행 거리가 있는 데이터만 필터링
            valid_monthly_data = group[
                (group['fuel_consumption_l'].notna()) & (group['fuel_consumption_l'] > 0) &
                (group['distance_km'].notna()) & (group['distance_km'] > 0)
            ].copy()

            if valid_monthly_data.empty:
                print(f"⚠️ 차량 {vehicle_plate_no}: 유효한 월별 연료/거리 데이터가 없어 베이스라인을 계산할 수 없습니다.")
                continue

            # 최근 5년치 (60개월) 데이터 중 최소 3년치 (36개월) 이상이 존재하는지 확인
            # 현재 날짜 기준으로 5년 전까지의 데이터만 고려
            current_date = pd.to_datetime(datetime.now().strftime('%Y%m'), format='%Y%m')
            five_years_ago = current_date - pd.DateOffset(years=5)

            recent_data = valid_monthly_data[
                (valid_monthly_data['record_year_month_dt'] >= five_years_ago)
            ].copy()

            if len(recent_data) < 36:
                print(f"⚠️ 차량 {vehicle_plate_no}: 베이스라인 계산에 필요한 최소 3년(36개월)치 데이터가 부족합니다 ({len(recent_data)}개월). 베이스라인을 계산하지 않습니다.")
                continue
            
            # 실제 베이스라인 계산에 사용될 데이터 (최대 5년치)
            baseline_period_data = recent_data.tail(60) # 최근 60개월 (5년) 데이터 사용

            total_distance_km = baseline_period_data['distance_km'].sum()
            total_fuel_l = baseline_period_data['fuel_consumption_l'].sum()
            months_of_operation = len(baseline_period_data)

            if total_distance_km == 0:
                fuel_per_km = 0.0
            else:
                fuel_per_km = total_fuel_l / total_distance_km

            avg_annual_distance_km = (total_distance_km / months_of_operation) * 12
            avg_annual_fuel_l = (total_fuel_l / months_of_operation) * 12

            # 베이스라인 CO2 배출량 및 배출계수 계산
            # 경유(Diesel) 차량 계산
            if group['original_fuel_type'].iloc[0] == '경유':
                emissions_tco2 = (avg_annual_fuel_l / 1000) * NET_CALORIFIC_VALUE['경유'] * CO2_EMISSION_FACTOR['경유']
                baseline_co2_emission_kg = emissions_tco2 * 1000
                baseline_emission_factor = (baseline_co2_emission_kg / avg_annual_fuel_l) if avg_annual_fuel_l > 0 else 0.0
            # CNG 차량 계산
            elif group['original_fuel_type'].iloc[0] == 'CNG':
                # avg_annual_fuel_l (kg) -> m3 -> 천m3
                activity_data_m3 = avg_annual_fuel_l / CNG_DENSITY_KG_PER_M3
                activity_data_1000m3 = activity_data_m3 / 1000
                emissions_tco2 = activity_data_1000m3 * NET_CALORIFIC_VALUE['CNG'] * CO2_EMISSION_FACTOR['CNG']
                baseline_co2_emission_kg = emissions_tco2 * 1000
                baseline_emission_factor = (baseline_co2_emission_kg / avg_annual_fuel_l) if avg_annual_fuel_l > 0 else 0.0
            else:
                baseline_co2_emission_kg = 0.0
                baseline_emission_factor = 0.0

            baseline_data.append({
                'vehicle_plate_no': vehicle_plate_no,
                'baseline_start_ym': baseline_period_data['record_year_month'].min(),
                'baseline_end_ym': baseline_period_data['record_year_month'].max(),
                'months_of_operation': months_of_operation,
                'avg_annual_distance_km': avg_annual_distance_km,
                'avg_annual_fuel_l': avg_annual_fuel_l,
                'fuel_per_km': fuel_per_km,
                'baseline_co2_emission_kg': baseline_co2_emission_kg,
                'baseline_emission_factor': baseline_emission_factor
            })
        
        baseline_df = pd.DataFrame(baseline_data)
        
        if baseline_df.empty:
            print("⚠️ 모든 차량에 대해 베이스라인을 계산할 수 없었습니다.")
            close_db_connection(conn)
            return

        print("✅ 베이스라인 인자 계산을 완료했습니다.")

        # 3. bus_baseline_parameters 테이블 스키마에 맞게 컬럼 선택
        # 이미 위에서 필요한 컬럼만으로 DataFrame을 생성했으므로 추가 선택 불필요
        # baseline_df = baseline_df[[
        #     'vehicle_plate_no',
        #     'baseline_start_ym',
        #     'baseline_end_ym',
        #     'months_of_operation',
        #     'avg_annual_distance_km',
        #     'avg_annual_fuel_l',
        #     'fuel_per_km'
        # ]]
            
        # 4. 베이스라인 데이터 적재
        insert_or_update_baseline_data(conn, baseline_df)
    else:
        print("⚠️ 베이스라인을 계산할 데이터가 없습니다.")
    
    close_db_connection(conn)

if __name__ == '__main__':
    main()

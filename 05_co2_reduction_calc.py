import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from datetime import datetime
from db_config import db_connection_params
from db_utils import connect_to_db, close_db_connection
from constants import NET_CALORIFIC_VALUE, CO2_EMISSION_FACTOR, CNG_DENSITY_KG_PER_M3

def load_data_for_reduction_calc(conn):
    """감축량 계산에 필요한 베이스라인 및 차량 마스터 데이터를 DB에서 로드하는 함수."""
    if not conn: return pd.DataFrame()
    print("⏳ 감축량 계산을 위해 'bus_baseline_parameters'와 'bus_vehicle_master' 테이블에서 데이터를 로드합니다...")
    try:
        query = """
        SELECT
            vm.vehicle_plate_no,
            vm.business_type,
            vm.ev_registration_date,
            vm.original_fuel_type,
            bp.avg_annual_fuel_l, -- 베이스라인 연간 연료 소비량
            
            bmfd.distance_km AS ev_latest_month_distance_km -- 전기차의 최신 월별 주행 거리
        FROM
            bus_vehicle_master vm
        JOIN
            bus_baseline_parameters bp ON vm.original_ice_plate_no = bp.vehicle_plate_no -- 대체된 내연기관 차량의 베이스라인 조인
        LEFT JOIN LATERAL (
            SELECT
                bmfd_sub.distance_km,
                bmfd_sub.record_year_month
            FROM
                bus_monthly_fuel_data bmfd_sub
            WHERE
                bmfd_sub.vehicle_plate_no = vm.vehicle_plate_no
            ORDER BY
                bmfd_sub.record_year_month DESC
            LIMIT 1
        ) AS bmfd ON vm.vehicle_plate_no IS NOT NULL -- 전기차량에 대해서만 조인 시도
        WHERE
            vm.ev_registration_date IS NOT NULL
            AND vm.business_type = '대체도입';
        """
        df = pd.read_sql_query(query, conn)
        if df.empty:
            print("⚠️ 상세 감축량 계산 대상(CNG, 경유 대체도입 전기버스)이 없습니다.")
            return pd.DataFrame()
        
        print(f"✅ {len(df)}개의 계산 대상 차량 데이터를 성공적으로 로드했습니다.")
        return df
    except Exception as e:
        print(f"❌ 데이터 로드 중 오류 발생: {e}")
        return pd.DataFrame()

def insert_or_update_emission_reductions(conn, df):
    """계산된 감축량 데이터를 DB에 저장하거나 업데이트하는 함수."""
    if not conn or df.empty: return
    
    cols = df.columns.tolist()
    values = [tuple(row) for row in df.to_numpy()]
    
    update_cols = [col for col in cols if col != 'vehicle_plate_no']
    update_statement = ", ".join([f"{col}=EXCLUDED.{col}" for col in update_cols])
    
    insert_query = sql.SQL("""
        INSERT INTO bus_emission_reductions ({}) 
        VALUES %s
        ON CONFLICT (vehicle_plate_no) DO UPDATE SET {}
    """).format(
        sql.SQL(', ').join(map(sql.Identifier, cols)),
        sql.SQL(update_statement)
    )
    
    with conn.cursor() as cur:
        try:
            print("⏳ 'bus_emission_reductions' 테이블에 상세 계산된 감축량 데이터를 저장/업데이트합니다...")
            execute_values(cur, insert_query, values)
            conn.commit()
            print(f"✅ {cur.rowcount}개의 감축량 레코드가 성공적으로 저장/업데이트되었습니다.")
        except psycopg2.Error as e:
            print(f"❌ 감축량 데이터 저장 오류: {e}")
            conn.rollback()

def main():
    """메인 실행 함수."""
    print("\n--- [파일 5] 상세 CO2 감축량 계산 시작 (엑셀 로직 기반) ---")
    
    db_params = db_connection_params
    conn = connect_to_db(db_params)
    
    if conn:
        # 1. 계산 대상 데이터 로드
        calc_df = load_data_for_reduction_calc(conn)
        
        if not calc_df.empty:
            print("\n⏳ CO2 감축량을 상세 로직에 따라 계산합니다...")
            
            # 2. 이용연수 계산
            current_year = datetime.now().year
            calc_df['ev_registration_date'] = pd.to_datetime(calc_df['ev_registration_date'])
            calc_df['start_year'] = calc_df['ev_registration_date'].dt.year
            calc_df['usage_year'] = current_year - calc_df['start_year'] + 1
            
            # 3. 베이스라인 배출량 및 감축량 계산 (벡터화)
            calc_df['baseline_co2_emission_kg'] = 0.0

            # --- 경유(Diesel) 차량 계산 ---
            diesel_mask = calc_df['original_fuel_type'] == '경유'
            if diesel_mask.any():
                # 활동량 (L -> kL)
                activity_data_kl = calc_df.loc[diesel_mask, 'avg_annual_fuel_l'] / 1000
                # 배출량 (tCO2) = 활동량(kL) * 순발열량(TJ/kL) * 배출계수(tCO2/TJ)
                emissions_tco2 = activity_data_kl * NET_CALORIFIC_VALUE['경유'] * CO2_EMISSION_FACTOR['경유']
                # 단위를 kgCO2로 변환하여 저장
                calc_df.loc[diesel_mask, 'baseline_co2_emission_kg'] = emissions_tco2 * 1000

            # --- CNG 차량 계산 ---
            cng_mask = calc_df['original_fuel_type'] == 'CNG'
            if cng_mask.any():
                # 활동량 계산: DB의 'avg_annual_fuel_l' 컬럼이 CNG의 경우 질량(kg) 단위로 저장되었다고 가정.
                # 질량(kg)을 밀도(kg/m³)로 나누어 부피(m³)로 변환 후, 다시 1000으로 나누어 '천m³' 단위로 변환.
                print("\nℹ️  CNG 연료량은 DB의 'L' 단위 컬럼 값을 질량(kg)으로 간주하고, 밀도를 이용해 부피(m³)로 변환하여 계산합니다.")
                activity_data_kg = calc_df.loc[cng_mask, 'avg_annual_fuel_l']
                activity_data_m3 = activity_data_kg / CNG_DENSITY_KG_PER_M3
                activity_data_1000m3 = activity_data_m3 / 1000
                
                # 배출량 (tCO2) = 활동량(천m³) * 순발열량(TJ/천m³) * 배출계수(tCO2/TJ) 
                emissions_tco2 = activity_data_1000m3 * NET_CALORIFIC_VALUE['CNG'] * CO2_EMISSION_FACTOR['CNG']
                # 단위를 kgCO2로 변환하여 저장
                calc_df.loc[cng_mask, 'baseline_co2_emission_kg'] = emissions_tco2 * 1000

            # 4. 최종 데이터프레임 준비
            calc_df['calculated_year'] = current_year
            calc_df['baseline_annual_fuel_l'] = calc_df['avg_annual_fuel_l']
            # 유효 배출계수(kg/L 또는 kg/m³) 계산하여 저장
            calc_df['baseline_emission_factor'] = (calc_df['baseline_co2_emission_kg'] / calc_df['baseline_annual_fuel_l']).fillna(0)
            calc_df['ev_actual_co2_emission_kg'] = 0.0 # 전기차 직접배출량은 0
            calc_df['co2_reduction_kg'] = calc_df['baseline_co2_emission_kg'] # 감축량 = 베이스라인 배출량
            calc_df['reduction_category'] = '대체버스 감축 (상세)'

            # DB 테이블 스키마에 맞게 컬럼 선택 및 정렬
            final_reduction_df = calc_df[[
                'vehicle_plate_no', 'calculated_year', 'baseline_annual_fuel_l',
                'baseline_emission_factor', 'baseline_co2_emission_kg', 'ev_actual_co2_emission_kg',
                'co2_reduction_kg', 'reduction_category'
            ]].copy()

            print("\n[상세 계산된 감축량 데이터 (상위 5개 행)]")
            print(final_reduction_df.head())

            # 5. 감축량 결과 데이터 적재
            insert_or_update_emission_reductions(conn, final_reduction_df)
        
        close_db_connection(conn)

if __name__ == '__main__':
    main()

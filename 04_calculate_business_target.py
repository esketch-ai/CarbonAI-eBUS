import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from db_config import db_connection_params
from datetime import datetime
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

def load_data_from_db(conn, table_name):
    """DB에서 지정된 테이블의 데이터를 불러와 DataFrame으로 반환하는 함수."""
    if not conn: return pd.DataFrame()
    print(f"⏳ 	'{table_name}' 테이블에서 데이터를 로드합니다...")
    try:
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql_query(query, conn)
        print(f"✅ {len(df)}개의 	'{table_name}' 데이터를 성공적으로 로드했습니다.")
        return df
    except Exception as e:
        print(f"❌ 	'{table_name}' 데이터 로드 중 오류 발생: {e}")
        return pd.DataFrame()

def insert_or_update_emission_reductions(conn, df):
    """
    계산된 감축량 데이터를 DB에 저장하거나 업데이트하는 함수.
    :param conn: psycopg2 connection 객체
    :param df: 저장할 감축량 데이터프레임
    """
    if not conn or df.empty: return
    
    # bus_emission_reductions 테이블의 컬럼명에 맞게 DataFrame 컬럼명 변경 (이미 영문으로 가정)
    cols = df.columns.tolist()
    values = [tuple(row) for row in df.to_numpy()]
    
    # ON CONFLICT ... DO UPDATE 쿼리 작성
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
            print("⏳ 'bus_emission_reductions' 테이블에 데이터를 저장/업데이트합니다...")
            execute_values(cur, insert_query, values)
            conn.commit()
            print(f"✅ {cur.rowcount}개의 감축량 레코드가 성공적으로 저장/업데이트되었습니다.")
        except psycopg2.Error as e:
            print(f"❌ 감축량 데이터 저장 오류: {e}")
            conn.rollback()

def main():
    """메인 실행 함수."""
    print("\n--- [파일 4] 사업 목표 감축량 계산 시작 ---")
    
    db_params = db_connection_params  # db_config.py에서 가져온 DB 연결 정보
    conn = connect_to_db(db_params)
    
    if conn:
        # 1. 베이스라인 데이터 및 차량 마스터 데이터 로드
        baseline_df = load_data_from_db(conn, 'bus_baseline_parameters')
        vehicle_master_df = load_data_from_db(conn, 'bus_vehicle_master')
        
        if baseline_df.empty or vehicle_master_df.empty:
            print("⚠️ 필요한 데이터(베이스라인 또는 차량 마스터)가 없습니다. 01, 02번 스크립트를 먼저 실행해주세요.")
            conn.close()
            return

        # 베이스라인 데이터와 차량 마스터 정보를 조인
        merged_df = pd.merge(baseline_df, vehicle_master_df, on='vehicle_plate_no', how='inner')

        # CO2 배출 계수 (kg CO2 / L)
        emission_factors = {
            'CNG': 2.75,  # 예시 값
            '경유': 2.68   # 예시 값
        }

        # 2. 감축량 계산 (벡터화 방식 적용)
        print("\n⏳ CO2 감축량을 계산합니다...")
        
        # 배출 계수 매핑
        merged_df['baseline_emission_factor'] = merged_df['original_fuel_type'].map(emission_factors)
        
        # 계산에 필요한 마스크 정의
        replacement_buses_mask = (merged_df['business_type'] == '대체도입') & (merged_df['ev_registration_date'].notna())
        new_buses_mask = (merged_df['business_type'] != '대체도입') & (merged_df['ev_registration_date'].notna())
        valid_factor_mask = merged_df['baseline_emission_factor'].notna()

        # 계산용 컬럼 초기화
        merged_df['calculated_year'] = datetime.now().year
        merged_df['baseline_annual_fuel_l'] = 0.0
        merged_df['baseline_co2_emission_kg'] = 0.0
        merged_df['ev_actual_co2_emission_kg'] = 0.0  # 전기차는 직접 배출 0
        merged_df['co2_reduction_kg'] = 0.0
        merged_df['reduction_category'] = ''

        # 3. 대체 버스 감축량 계산 (벡터화)
        # 배출 계수가 정의된 대체 버스
        calc_mask = replacement_buses_mask & valid_factor_mask
        if calc_mask.any():
            merged_df.loc[calc_mask, 'baseline_annual_fuel_l'] = merged_df.loc[calc_mask, 'avg_annual_fuel_l']
            merged_df.loc[calc_mask, 'baseline_co2_emission_kg'] = merged_df.loc[calc_mask, 'baseline_annual_fuel_l'] * merged_df.loc[calc_mask, 'baseline_emission_factor']
            merged_df.loc[calc_mask, 'co2_reduction_kg'] = merged_df.loc[calc_mask, 'baseline_co2_emission_kg']
            merged_df.loc[calc_mask, 'reduction_category'] = '대체버스 감축'
            print(f"✅ {calc_mask.sum()}개의 대체 버스 감축량을 계산했습니다.")
        
        # 배출 계수가 정의되지 않은 대체 버스
        no_factor_mask = replacement_buses_mask & ~valid_factor_mask
        if no_factor_mask.any():
            merged_df.loc[no_factor_mask, 'reduction_category'] = '대체버스 (계수 미정의)'
            print(f"⚠️ {no_factor_mask.sum()}개의 대체 버스는 배출 계수가 정의되지 않아 감축량을 계산할 수 없습니다.")

        # 4. 신규 버스 처리 (벡터화)
        if new_buses_mask.any():
            merged_df.loc[new_buses_mask, 'reduction_category'] = '신규버스 (감축 미산정)'
            print(f"✅ {new_buses_mask.sum()}개의 신규 버스를 '미산정'으로 처리했습니다.")

        # 계산 후 NaN 값들을 0 또는 빈 문자열로 채움
        merged_df['baseline_emission_factor'] = merged_df['baseline_emission_factor'].fillna(0)

        # 최종 결과 데이터프레임 준비
        # bus_emission_reductions 테이블 스키마에 맞게 컬럼 선택
        final_reduction_df = merged_df[[
            'vehicle_plate_no', 'calculated_year', 'baseline_annual_fuel_l',
            'baseline_emission_factor', 'baseline_co2_emission_kg', 'ev_actual_co2_emission_kg',
            'co2_reduction_kg', 'reduction_category'
        ]].copy()

        print("\n[계산된 감축량 데이터 (상위 5개 행)]")
        print(final_reduction_df.head(10))

        # 5. 감축량 결과 데이터 적재
        insert_or_update_emission_reductions(conn, final_reduction_df)
        
        close_db_connection(conn)

if __name__ == '__main__':
    main()
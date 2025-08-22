import pandas as pd
import psycopg2
from datetime import datetime
from db_config import db_connection_params
from db_utils import connect_to_db, close_db_connection

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

def display_baseline_data(conn):
    """DB에서 베이스라인 인자 데이터를 불러와 출력하는 함수."""
    if not conn: return
    
    print("\n⏳ 베이스라인 인자 및 차량 마스터 데이터를 조회합니다...")
    try:
        baseline_df = load_data_from_db(conn, 'bus_baseline_parameters')
        vehicle_master_df = load_data_from_db(conn, 'bus_vehicle_master')

        if baseline_df.empty or vehicle_master_df.empty:
            print("⚠️ 조회된 데이터가 없습니다. 01, 02번 스크립트를 먼저 실행했는지 확인해주세요.")
            return

        # 베이스라인 데이터와 차량 마스터 정보를 조인
        merged_df = pd.merge(baseline_df, vehicle_master_df, on='vehicle_plate_no', how='inner')
        
        # 보기 좋게 출력하기 위해 컬럼명 변경 및 포맷팅
        merged_df.rename(columns={
            'vehicle_plate_no': '차량번호',
            'company_name': '업체명',
            'sequence_no': '순번',
            'business_type': '사업구분',
            'model_year': '연식',
            'ev_registration_date': '전기차량 등록일',
            'original_fuel_type': '기존 연료',
            'baseline_start_ym': '베이스라인_시작월',
            'baseline_end_ym': '베이스라인_종료월',
            'months_of_operation': '산정월수',
            'avg_annual_distance_km': '연평균주행거리(km)',
            'avg_annual_fuel_l': '연평균주유량(L)',
            'fuel_per_km': '연비(L/km)'
        }, inplace=True)
        
        # 소수점 2자리까지만 표시
        pd.options.display.float_format = '{:,.2f}'.format
        
        print("\n" + "="*100)
        print(" " * 38 + "[ 최종 베이스라인 인자 ]")
        print("="*100)
        print(merged_df[['차량번호', '업체명', '사업구분', '기존 연료', '베이스라인_시작월', '베이스라인_종료월', '산정월수', '연평균주행거리(km)', '연평균주유량(L)', '연비(L/km)']])
        print("="*100)
        
        # 엑셀 파일로 저장
        save_df_to_excel(merged_df[['차량번호', '업체명', '사업구분', '기존 연료', '베이스라인_시작월', '베이스라인_종료월', '산정월수', '연평균주행거리(km)', '연평균주유량(L)', '연비(L/km)']], "baseline_calculation_results")

    except Exception as e:
        print(f"❌ 데이터 조회 중 오류 발생: {e}")

def save_df_to_excel(df, filename_prefix):
    """DataFrame을 엑셀 파일로 저장하는 함수"""
    if df.empty:
        print(f"⚠️ {filename_prefix} 저장: 데이터프레임이 비어 있어 엑셀 파일을 생성하지 않습니다.")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = rf"C:\Users\USER\OneDrive - 주식회사후시파트너스\문서\Develope\GHGERC_BUS\reports\{filename_prefix}_{timestamp}.xlsx"
    
    try:
        df.to_excel(file_path, index=False, sheet_name="베이스라인 계산결과")
        print(f"✅ 엑셀 파일 저장 성공: {file_path}")
    except Exception as e:
        print(f"❌ 엑셀 파일 저장 중 오류 발생: {e}")

def main():
    """메인 실행 함수"""
    print("\n--- [파일 3] 베이스라인 인자 조회 및 출력 시작 ---")
    
    db_params = db_connection_params  # db_config.py에서 가져온 DB 연결 정보
    conn = connect_to_db(db_params)
    
    if conn:
        display_baseline_data(conn)
        close_db_connection(conn)

if __name__ == '__main__':
    main()
import pandas as pd
import os
from datetime import datetime
from db_config import db_connection_params
from db_utils import connect_to_db, close_db_connection

def generate_excel_report(conn):
    """
    DB의 모든 관련 테이블을 조인하여 종합 보고서용 데이터를 생성하고 Excel 파일로 저장하는 함수.
    - 시트 1: 종합 보고서 (마스터, 베이스라인, 감축량 정보 포함)
    - 시트 2: 월별 운행기록 (베이스라인 계산의 원본 데이터)
    - 시트 3: 베이스라인 계산결과 (차량별 베이스라인 요약)
    """
    if not conn: return

    try:
        import openpyxl
    except ImportError:
        print("\n⚠️ 'openpyxl' 라이브러리가 필요합니다. 'pip install openpyxl' 명령으로 설치 후 다시 실행해주세요.")
        return

    comprehensive_query = """
        SELECT
            vm.vehicle_plate_no,
            vm.company_name,
            vm.business_type,
            vm.model_year,
            vm.original_fuel_type,
            vm.ev_registration_date,
            bp.baseline_start_ym,
            bp.baseline_end_ym,
            bp.months_of_operation,
            bp.avg_annual_distance_km,
            bp.avg_annual_fuel_l,
            bp.fuel_per_km,
            er.calculated_year,
            er.baseline_emission_factor,
            er.baseline_co2_emission_kg,
            er.co2_reduction_kg,
            er.reduction_category
        FROM
            bus_vehicle_master vm
        LEFT JOIN
            bus_baseline_parameters bp ON vm.vehicle_plate_no = bp.vehicle_plate_no
        LEFT JOIN
            bus_emission_reductions er ON vm.vehicle_plate_no = er.vehicle_plate_no
        ORDER BY
            vm.company_name, vm.vehicle_plate_no;
        """
    
    comprehensive_rename_map = {
            'vehicle_plate_no': '차량번호',
            'company_name': '업체명',
            'business_type': '사업구분',
            'model_year': '연식',
            'original_fuel_type': '기존연료',
            'ev_registration_date': '전기차등록일',
            'baseline_start_ym': '베이스라인_시작월',
            'baseline_end_ym': '베이스라인_종료월',
            'months_of_operation': '베이스라인_산정월수',
            'avg_annual_distance_km': '베이스라인_연평균주행거리(km)',
            'avg_annual_fuel_l': '베이스라인_연평균연료량(L)',
            'fuel_per_km': '베이스라인_연비(L/km)',
            'calculated_year': '감축량_계산연도',
            'baseline_emission_factor': '감축량_적용배출계수(kg/L)',
            'baseline_co2_emission_kg': '감축량_베이스라인CO2(kg)',
            'co2_reduction_kg': '감축량_CO2감축량(kg)',
            'reduction_category': '감축량_산정방식'
        }

    monthly_query = """
    SELECT
        vm.company_name,
        dr.vehicle_plate_no,
        dr.year_month,
        dr.operating_days,
        dr.driving_distance_km,
        dr.fuel_quantity_l
    FROM
        bus_driving_records dr
    JOIN
        bus_vehicle_master vm ON dr.vehicle_plate_no = vm.vehicle_plate_no
    ORDER BY
        vm.company_name, dr.vehicle_plate_no, dr.year_month;
    """
    monthly_rename_map = {
        'company_name': '업체명',
        'vehicle_plate_no': '차량번호',
        'year_month': '운행년월',
        'operating_days': '운행일수',
        'driving_distance_km': '주행거리(km)',
        'fuel_quantity_l': '연료사용량(L)'
    }

    baseline_query = """
    SELECT
        vm.company_name,
        vm.business_type,
        vm.model_year,
        vm.original_fuel_type,
        bp.*
    FROM
        bus_baseline_parameters bp
    JOIN
        bus_vehicle_master vm ON bp.vehicle_plate_no = vm.vehicle_plate_no
    ORDER BY
        vm.company_name, bp.vehicle_plate_no;
    """
    baseline_rename_map = {
        'company_name': '업체명',
        'business_type': '사업구분',
        'model_year': '연식',
        'original_fuel_type': '기존연료',
        'vehicle_plate_no': '차량번호',
        'baseline_start_ym': '베이스라인_시작월',
        'baseline_end_ym': '베이스라인_종료월',
        'months_of_operation': '산정월수',
        'avg_annual_distance_km': '연평균주행거리(km)',
        'avg_annual_fuel_l': '연평균연료량(L)',
        'fuel_per_km': '연비(L/km)'
    }

    try:
        # --- 데이터 로드 ---
        print("⏳ [1/3] 종합 보고서 데이터를 로드합니다...")
        comprehensive_df = pd.read_sql_query(comprehensive_query, conn)
        print("⏳ [2/3] 월별 운행기록 데이터를 로드합니다...")
        monthly_df = pd.read_sql_query(monthly_query, conn)
        print("⏳ [3/3] 베이스라인 계산결과 데이터를 로드합니다...")
        baseline_df = pd.read_sql_query(baseline_query, conn)

        if comprehensive_df.empty:
            print("⚠️ 보고서를 생성할 데이터가 없습니다. 01번부터 스크립트를 실행했는지 확인해주세요.")
            return

        # --- 데이터 가공 (컬럼명 변경 및 포맷팅) ---
        comprehensive_df.rename(columns=comprehensive_rename_map, inplace=True)
        if '전기차등록일' in comprehensive_df.columns:
            comprehensive_df['전기차등록일'] = pd.to_datetime(comprehensive_df['전기차등록일']).dt.strftime('%Y-%m-%d').replace('NaT', '')

        monthly_df.rename(columns=monthly_rename_map, inplace=True)
        baseline_df.rename(columns=baseline_rename_map, inplace=True)

        # --- Excel 파일로 저장 ---
        output_dir = 'reports'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"✅ '{output_dir}' 폴더를 생성했습니다.")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f'bus_analysis_report_{timestamp}.xlsx'
        report_path = os.path.join(output_dir, report_filename)

        print(f"\n⏳ 생성된 보고서를 Excel 파일로 저장합니다: {report_path}")
        with pd.ExcelWriter(report_path, engine='openpyxl') as writer:
            comprehensive_df.to_excel(writer, sheet_name='종합 보고서', index=False)
            monthly_df.to_excel(writer, sheet_name='월별 운행기록', index=False)
            baseline_df.to_excel(writer, sheet_name='베이스라인 계산결과', index=False)

        print(f"✅ 보고서 저장이 완료되었습니다: {report_path}")

    except Exception as e:
        print(f"❌ 보고서 생성 중 오류 발생: {e}")

def main():
    """메인 실행 함수."""
    print("\n--- [파일 6] 종합 분석 보고서(Excel) 생성 시작 ---")
    db_params = db_connection_params
    conn = connect_to_db(db_params)
    if conn:
        generate_excel_report(conn)
        close_db_connection(conn)

if __name__ == '__main__':
    main()
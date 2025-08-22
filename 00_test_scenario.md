# 00_test_scenario.md - 테스트 시나리오 관리

## 1. 개요

본 문서는 프로젝트의 각 Python 스크립트에 대한 테스트 시나리오를 정의하고 관리합니다. 각 시나리오는 스크립트의 주요 기능과 예상 결과를 명확히 기술하여, 기능의 정확성을 검증하는 데 사용됩니다.

## 2. 테스트 환경

*   **데이터베이스:** PostgreSQL (db_config.py에 설정된 정보 사용)
*   **Python 버전:** 3.x
*   **필수 라이브러리:** pandas, numpy, psycopg2-binary, openpyxl

## 3. 공통 테스트 전제 조건

*   `db_config.py` 파일에 올바른 데이터베이스 연결 정보가 설정되어 있어야 합니다.
*   모든 Python 스크립트는 프로젝트 루트 디렉토리에 위치해야 합니다.

## 4. 스크립트별 테스트 시나리오

### 4.1. `00_edit_db.py` - 데이터베이스 스키마 초기화 및 생성

*   **목표:** 데이터베이스 테이블이 올바르게 생성되고 기존 테이블이 삭제되는지 확인합니다.
*   **시나리오:**
    1.  `00_edit_db.py`를 실행합니다.
    2.  PostgreSQL 클라이언트(pgAdmin 등)를 사용하여 `bus_vehicle_master`, `bus_driving_records`, `bus_baseline_parameters`, `bus_emission_reductions`, `bus_monthly_fuel_data` 테이블이 존재하는지 확인합니다.
    3.  각 테이블의 스키마(컬럼명, 데이터 타입, 제약 조건)가 `00_db_erd.md`에 정의된 내용과 일치하는지 확인합니다.
*   **예상 결과:** 모든 관련 테이블이 성공적으로 생성되고, 기존 데이터가 있다면 삭제됩니다.

### 4.2. `01_insert_monthly_data.py` - 차량 마스터 및 월별 운행 기록 데이터 생성 및 DB 적재

*   **목표:** 가상 데이터가 생성되고 `bus_vehicle_master`, `bus_driving_records`, `bus_monthly_fuel_data` 테이블에 올바르게 적재되는지 확인합니다.
*   **시나리오:**
    1.  `00_edit_db.py`를 실행하여 DB를 초기화합니다.
    2.  `01_insert_monthly_data.py`를 실행합니다.
    3.  `generated_data` 폴더에 엑셀 파일이 생성되었는지 확인하고, 파일 내용을 열어 데이터가 올바르게 생성되었는지 육안으로 확인합니다.
    4.  PostgreSQL 클라이언트에서 `bus_vehicle_master`, `bus_driving_records`, `bus_monthly_fuel_data` 테이블에 데이터가 삽입되었는지 확인합니다.
    5.  특히, `bus_vehicle_master`의 `replaced_by_ev_plate_no`와 `original_ice_plate_no` 관계가 올바르게 설정되었는지 확인합니다.
*   **예상 결과:** 가상 데이터가 성공적으로 생성되고, DB 테이블에 적재되며, 엑셀 파일로도 저장됩니다.

### 4.3. `02_calculate_baseline.py` - 베이스라인 인자 계산 및 DB 적재

*   **목표:** 월별 운행 기록을 기반으로 베이스라인 인자가 정확하게 계산되고 `bus_baseline_parameters` 테이블에 적재되는지 확인합니다.
*   **시나리오:**
    1.  `00_edit_db.py` 및 `01_insert_monthly_data.py`를 순서대로 실행합니다.
    2.  `02_calculate_baseline.py`를 실행합니다.
    3.  PostgreSQL 클라이언트에서 `bus_baseline_parameters` 테이블에 데이터가 삽입되었는지 확인합니다.
    4.  몇몇 차량의 `avg_annual_distance_km`, `avg_annual_fuel_l`, `fuel_per_km` 값이 논리적으로 올바른지 수동으로 검증합니다.
*   **예상 결과:** 베이스라인 인자가 성공적으로 계산되고 DB에 저장됩니다.

### 4.4. `04_calculate_business_target.py` - 사업 목표 감축량 계산 및 DB 적재

*   **목표:** 베이스라인 인자를 기반으로 CO2 감축량이 계산되고 `bus_emission_reductions` 테이블에 적재되는지 확인합니다.
*   **시나리오:**
    1.  `00_edit_db.py`, `01_insert_monthly_data.py`, `02_calculate_baseline.py`를 순서대로 실행합니다.
    2.  `04_calculate_business_target.py`를 실행합니다.
    3.  PostgreSQL 클라이언트에서 `bus_emission_reductions` 테이블에 데이터가 삽입되었는지 확인합니다.
    4.  `co2_reduction_kg` 값이 올바르게 계산되었는지, 특히 '대체버스 감축'과 '신규버스 (감축 미산정)' 카테고리가 정확한지 확인합니다.
*   **예상 결과:** CO2 감축량이 성공적으로 계산되고 DB에 저장됩니다.

### 4.5. `05_co2_reduction_calc.py` - 상세 CO2 감축량 계산 (엑셀 로직 기반) 및 DB 적재

*   **목표:** 엑셀 로직 기반의 상세 CO2 감축량이 정확하게 계산되고 `bus_emission_reductions` 테이블에 업데이트되는지 확인합니다.
*   **시나리오:**
    1.  `00_edit_db.py`, `01_insert_monthly_data.py`, `02_calculate_baseline.py`, `04_calculate_business_target.py`를 순서대로 실행합니다.
    2.  `05_co2_reduction_calc.py`를 실행합니다.
    3.  PostgreSQL 클라이언트에서 `bus_emission_reductions` 테이블의 `co2_reduction_kg` 및 `reduction_category` 컬럼이 `05_co2_reduction_calc.py`의 로직에 따라 업데이트되었는지 확인합니다.
    4.  특히 경유 및 CNG 차량에 대한 배출량 계산 로직이 `constants.py`의 계수를 사용하여 올바르게 적용되었는지 확인합니다.
*   **예상 결과:** 상세 CO2 감축량이 정확하게 계산되어 `bus_emission_reductions` 테이블에 반영됩니다.

### 4.6. `03_display_baseline.py` - 베이스라인 인자 조회 및 출력

*   **목표:** 계산된 베이스라인 인자가 콘솔에 올바르게 출력되고 엑셀 파일로 저장되는지 확인합니다.
*   **시나리오:**
    1.  `run_all.py`를 실행하여 모든 파이프라인을 완료합니다.
    2.  `03_display_baseline.py`를 실행합니다.
    3.  콘솔에 출력되는 베이스라인 데이터의 형식과 내용이 올바른지 확인합니다.
    4.  `reports` 폴더에 엑셀 파일이 생성되었는지 확인하고, 파일 내용을 열어 데이터가 올바르게 표시되는지 육안으로 확인합니다.
*   **예상 결과:** 베이스라인 데이터가 콘솔에 보기 좋게 출력되고, 엑셀 파일로도 성공적으로 저장됩니다.

### 4.7. `06_Report.py` - 종합 분석 보고서(Excel) 생성

*   **목표:** DB의 모든 관련 데이터를 통합하여 종합 Excel 보고서가 올바르게 생성되는지 확인합니다.
*   **시나리오:**
    1.  `run_all.py`를 실행하여 모든 파이프라인을 완료합니다.
    2.  `06_Report.py`를 실행합니다.
    3.  `reports` 폴더에 `bus_analysis_report_YYYYMMDD_HHMMSS.xlsx` 형식의 파일이 생성되었는지 확인합니다.
    4.  생성된 엑셀 파일을 열어 '종합 보고서', '월별 운행기록', '베이스라인 계산결과' 시트가 모두 존재하고 각 시트의 데이터가 올바르게 채워져 있는지 확인합니다.
    5.  특히, 컬럼명과 데이터 포맷이 가독성 좋게 변경되었는지 확인합니다.
*   **예상 결과:** 모든 관련 데이터가 통합된 종합 Excel 보고서가 성공적으로 생성됩니다.

### 4.8. `run_all.py` - 전체 파이프라인 자동 실행

*   **목표:** 모든 스크립트가 순서대로 오류 없이 실행되고, DB 초기화 옵션이 정상 작동하는지 확인합니다.
*   **시나리오:**
    1.  `run_all.py`를 실행하고, DB 초기화 여부를 묻는 메시지에 `y`를 입력합니다.
    2.  모든 스크립트가 성공적으로 실행되는지 콘솔 로그를 통해 확인합니다.
    3.  PostgreSQL 클라이언트에서 최종 데이터가 올바르게 적재되었는지 확인합니다.
    4.  `run_all.py`를 다시 실행하고, DB 초기화 여부를 묻는 메시지에 `n` 또는 다른 키를 입력합니다.
    5.  `00_edit_db.py`가 건너뛰어지고 나머지 스크립트가 실행되는지 확인합니다.
*   **예상 결과:** 모든 스크립트가 성공적으로 실행되며, DB 초기화 옵션이 정상 작동합니다.

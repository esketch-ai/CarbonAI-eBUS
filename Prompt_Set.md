# 프로젝트 스크립트 실행 가이드

이 문서는 프로젝트의 주요 Python 스크립트들을 실행하는 방법을 안내합니다. 각 스크립트는 순서대로 실행되어야 합니다.

---

## 권장 실행 방법: 전체 파이프라인 자동 실행

**스크립트:** `run_all.py`

**설명:**
이 마스터 스크립트는 아래의 모든 개별 스크립트를 올바른 순서대로 자동으로 실행합니다. 프로젝트의 전체 데이터 처리 파이프라인을 한번에 실행할 때 사용하는 것을 권장합니다.

**실행 방법:**

```bash
python run_all.py
```

스크립트 실행 시 데이터베이스 초기화 여부를 묻는 메시지가 나타납니다. `y`를 입력하면 `00_edit_db.py`부터 실행되며, 다른 키를 입력하면 데이터 초기화 없이 `01_insert_monthly_data.py`부터 실행됩니다.

---

## 0. 데이터베이스 스키마 초기화 및 생성

**스크립트:** `00_edit_db.py`

**설명:**
이 스크립트는 PostgreSQL 데이터베이스의 모든 기존 프로젝트 관련 테이블(bus_vehicle_master, bus_driving_records, bus_baseline_parameters, bus_emission_reductions)을 삭제하고, 최신 스키마 정의에 따라 새로 생성합니다. **기존 데이터가 모두 삭제되므로 주의하여 사용하십시오.**

**실행 방법:**

```bash
python 00_edit_db.py
```

---

## 1. 차량 마스터 및 월별 운행 기록 데이터 생성 및 DB 적재

**스크립트:** `01_insert_monthly_data.py`

**설명:**
가상의 버스 차량 마스터 데이터와 월별 운행 기록 데이터를 생성합니다. 차량 마스터 데이터는 `bus_vehicle_master` 테이블에, 월별 운행 기록은 `bus_driving_records` 테이블에 각각 적재됩니다. 이 스크립트는 대체 관계를 포함한 차량 정보를 생성하고 관리합니다.

**실행 방법:**

```bash
python 01_insert_monthly_data.py
```

---

## 2. 베이스라인 인자 계산 및 DB 적재

**스크립트:** `02_calculate_baseline.py`

**설명:**
`bus_driving_records` 테이블의 월별 운행 기록과 `bus_vehicle_master` 테이블의 차량 정보를 기반으로 각 차량의 베이스라인 인자(연평균 주행거리, 연평균 주유량, 연비 등)를 계산합니다. 계산된 베이스라인 인자는 `bus_baseline_parameters` 테이블에 저장됩니다.

**실행 방법:**

```bash
python 02_calculate_baseline.py
```

---

## 4. 사업 목표 감축량 계산 및 DB 적재

**스크립트:** `04_calculate_business_target.py`

**설명:**
`bus_baseline_parameters` 테이블의 베이스라인 인자와 `bus_vehicle_master` 테이블의 차량 정보를 기반으로 대체 버스 및 신규 버스에 대한 CO2 감축량을 계산합니다. 계산된 감축량 데이터는 `bus_emission_reductions` 테이블에 저장됩니다.

**실행 방법:**

```bash
python 04_calculate_business_target.py
```

---

## 3. 베이스라인 인자 조회 및 출력

**스크립트:** `03_display_baseline.py`

**설명:**
`bus_baseline_parameters` 테이블에 저장된 베이스라인 인자와 `bus_vehicle_master` 테이블의 차량 정보를 함께 조회하여 콘솔에 출력합니다. 이 스크립트를 통해 계산 및 저장된 베이스라인 데이터를 확인할 수 있습니다.

**실행 방법:**

```bash
python 03_display_baseline.py
```

---

**참고:**
*   스크립트 실행 전에 `db_config.py` 파일에 PostgreSQL 연결 정보가 올바르게 설정되어 있는지 확인해주세요.
*   필요한 Python 라이브러리(pandas, numpy, psycopg2 등)가 설치되어 있어야 합니다. (`pip install pandas numpy psycopg2-binary`)

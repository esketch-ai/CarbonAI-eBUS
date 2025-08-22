# 00_db_erd.md - 데이터베이스 ERD

## 1. 개요

본 문서는 프로젝트에서 사용되는 PostgreSQL 데이터베이스의 엔티티-관계 다이어그램(ERD)을 설명합니다. 각 테이블의 구조, 컬럼 정보, 기본 키(Primary Key), 그리고 외래 키(Foreign Key) 관계를 포함합니다.

## 2. 테이블 상세

### bus_baseline_parameters

| 컬럼명 | 데이터 타입 | Nullable | 비고 |
|---|---|---|---|| vehicle_plate_no | character varying | NO | PK, FK (-> bus_vehicle_master.vehicle_plate_no) |
| baseline_start_ym | character varying | YES |  |
| baseline_end_ym | character varying | YES |  |
| months_of_operation | integer | YES |  |
| avg_annual_distance_km | double precision | YES |  |
| avg_annual_fuel_l | double precision | YES |  |
| fuel_per_km | double precision | YES |  |
| baseline_co2_emission_kg | double precision | NO |  |
| baseline_emission_factor | double precision | NO |  |

### bus_driving_records

| 컬럼명 | 데이터 타입 | Nullable | 비고 |
|---|---|---|---|| id | integer | NO | PK |
| vehicle_plate_no | character varying | NO | FK (-> bus_vehicle_master.vehicle_plate_no) |
| year_month | character varying | NO |  |
| operating_days | integer | YES |  |
| driving_distance_km | double precision | YES |  |
| fuel_quantity_l | double precision | YES |  |
| charging_amount_kwh | double precision | YES |  |

### bus_emission_reductions

| 컬럼명 | 데이터 타입 | Nullable | 비고 |
|---|---|---|---|| vehicle_plate_no | character varying | NO | PK, FK (-> bus_vehicle_master.vehicle_plate_no) |
| calculated_year | integer | NO |  |
| baseline_annual_fuel_l | double precision | YES |  |
| baseline_emission_factor | double precision | YES |  |
| baseline_co2_emission_kg | double precision | YES |  |
| ev_actual_co2_emission_kg | double precision | YES |  |
| co2_reduction_kg | double precision | YES |  |
| reduction_category | character varying | NO |  |

### bus_monthly_fuel_data

| 컬럼명 | 데이터 타입 | Nullable | 비고 |
|---|---|---|---|| vehicle_plate_no | character varying | NO | PK |
| record_year_month | character varying | NO | PK |
| fuel_consumption_l | double precision | YES |  |
| distance_km | double precision | YES |  |

### bus_vehicle_master

| 컬럼명 | 데이터 타입 | Nullable | 비고 |
|---|---|---|---|| vehicle_plate_no | character varying | NO | PK |
| company_name | character varying | NO |  |
| sequence_no | integer | YES |  |
| business_type | character varying | NO |  |
| model_year | integer | YES |  |
| ev_registration_date | date | YES |  |
| original_fuel_type | character varying | YES |  |
| chassis_number | character varying | YES |  |
| replaced_by_ev_plate_no | character varying | YES | FK (-> bus_vehicle_master.vehicle_plate_no) |
| original_ice_plate_no | character varying | YES | FK (-> bus_vehicle_master.vehicle_plate_no) |


import subprocess
import sys
import os
from log_config import logger # 로거 임포트

def run_script(script_name):
    """
    주어진 Python 스크립트를 현재 인터프리터로 실행하고 결과를 확인하는 함수.
    :param script_name: 실행할 스크립트 파일명
    :return: 성공 시 True, 실패 시 False
    """
    logger.info("="*60)
    logger.info(f"🚀 Executing: {script_name}")
    logger.info("="*60)
    
    # 스크립트 파일이 존재하는지 확인
    if not os.path.exists(script_name):
        logger.error(f"Script not found at '{script_name}'. Please check the file path.")
        return False

    # 자식 프로세스가 UTF-8 인코딩을 사용하도록 환경 변수 설정
    # Windows 콘솔의 기본 인코딩(cp949)이 특정 유니코드 문자(예: ⏳)를 지원하지 않아 발생하는 오류를 방지합니다.
    env = os.environ.copy()
    env['PYTHONIOENCODING'] = 'utf-8'

    try:
        # 현재 파이썬 실행 파일을 사용하여 스크립트 실행
        result = subprocess.run(
            [sys.executable, script_name], 
            check=True, 
            capture_output=True,
            text=True,
            encoding='utf-8', # 자식 프로세스의 출력이 UTF-8이므로, 해당 인코딩으로 해석
            env=env
        )
        # 스크립트의 표준 출력을 그대로 보여줌
        logger.info(f"--- Output from {script_name} ---\n{result.stdout.strip()}")
        logger.info(f"✅ Success: '{script_name}' finished successfully.")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"'{script_name}' failed to execute.")
        if e.stdout:
            logger.error(f"--- STDOUT ---\n{e.stdout.strip()}")
        if e.stderr:
            logger.error(f"--- STDERR ---\n{e.stderr.strip()}")
        return False

def main():
    """메인 함수: 모든 프로젝트 스크립트를 순서대로 실행합니다."""
    logger.info("===== 🚌 Bus CO2 Reduction Calculation Pipeline Start =====")

    # 사용자에게 DB 초기화 여부 확인
    reset_db = input("\n❓ 데이터베이스를 초기화하시겠습니까? ('00_edit_db.py' 실행) [y/N]: ").lower().strip()
    
    scripts_to_run = []
    if reset_db == 'y':
        scripts_to_run.append('00_edit_db.py')

    # 파이프라인의 핵심 스크립트 목록
    pipeline_scripts = [
        '01_insert_monthly_data.py',        # 1. 데이터 생성 및 적재
        '02_calculate_baseline.py',         # 2. 베이스라인 계산
        '04_calculate_business_target.py',  # 3. 감축량 계산 (단순)
        '05_co2_reduction_calc.py',         # 4. 감축량 계산 (상세, 덮어쓰기)
        '03_display_baseline.py',           # 5. 베이스라인 결과 확인 (콘솔)
        '06_Report.py'                      # 6. 최종 결과 보고서 생성 (Excel)
    ]
    scripts_to_run.extend(pipeline_scripts)

    for script in scripts_to_run:
        if not run_script(script):
            logger.critical(f"Pipeline stopped due to an error in '{script}'.")
            sys.exit(1)  # 오류 발생 시 스크립트 종료

    logger.info("🎉🎉🎉 All scripts executed successfully! Pipeline finished. 🎉🎉🎉")

if __name__ == '__main__':
    main()
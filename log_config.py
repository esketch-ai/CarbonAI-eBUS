import logging
import logging.handlers
import os

def setup_logging():
    """프로젝트 전반에 걸쳐 사용할 표준 로깅을 설정합니다."""
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file_path = os.path.join(log_dir, 'project.log')

    # 로거 생성 (프로젝트의 최상위 로거)
    logger = logging.getLogger('GHGERC_BUS_PROJECT')
    logger.setLevel(logging.INFO) # 기본 로그 레벨 설정

    # 이미 핸들러가 설정되어 있다면 중복 추가 방지
    if logger.hasHandlers():
        return logger

    # 포매터 생성 (로그 메시지 형식 정의)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # 콘솔 핸들러 설정
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 파일 핸들러 설정 (TimedRotatingFileHandler로 매일 자정 로그 파일 교체, 7일치 보관)
    file_handler = logging.handlers.TimedRotatingFileHandler(
        log_file_path, when='midnight', interval=1, backupCount=7, encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

# 다른 모듈에서 `from log_config import logger`로 가져다 쓸 전역 로거 인스턴스
logger = setup_logging()
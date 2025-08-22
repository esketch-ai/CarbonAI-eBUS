# db_config.py

# !!! 사용자 환경에 맞게 아래 DB 연결 정보를 수정해주세요 !!!
# 이 파일에만 DB 정보를 저장하고, 다른 스크립트에서는 이 변수를 import하여 사용합니다.
db_connection_params = {
    "host": "esketch.synology.me",      # 예: "localhost" 또는 DB 서버 IP 주소
    "dbname": "postgres", # 예: "mydatabase"
    "user": "postgres",  # 예: "postgres"
    "password": "postgres",
    "port": "9191"            # PostgreSQL 기본 포트
}

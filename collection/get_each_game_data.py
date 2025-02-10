import json
import requests
import paramiko

# 📌 NBA 게임 ID
game_id = "0022400714"

# 📌 원격 서버 정보
host = "server02.hadoop.com"
port = 22  # SSH 기본 포트
username = "root"
password = "adminuser"

# 📌 원격 로그 파일 경로
remote_log_path = "/home/nba_project/working/nba-live-log/nba-live.log"

# 🔹 SSH 연결
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(host, port, username, password)

# 🔹 NBA 데이터 가져오기
today_games = requests.get(f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json").json()

# 🔹 게임 데이터를 한 줄씩 원격 서버에 추가
for game in today_games['game']['actions']:
    log_entry = json.dumps(game, ensure_ascii=False).replace("'", '"')  # 작은따옴표 문제 해결
    command = f"echo '{log_entry}' >> {remote_log_path}"

    stdin, stdout, stderr = ssh.exec_command(command)
    error_msg = stderr.read().decode()

    if error_msg:
        print(f"❌ 오류 발생: {error_msg}")
    else:
        print(f"✅ 로그 추가됨: {log_entry}")

# 🔹 SSH 연결 종료
ssh.close()

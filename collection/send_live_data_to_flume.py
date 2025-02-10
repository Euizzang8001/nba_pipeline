import paramiko
import os

# 서버 정보
host = "server02.hadoop.com"
port = 22  # SFTP는 22번 포트
username = "root"
password = "adminuser"

# SFTP 연결
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(host, port, username, password)

sftp = ssh.open_sftp()

# 원격 디렉토리 및 로컬 파일 경로 설정
remote_path = "/home/nba_project/working/nba-live-log/nba-live.log"
local_path = "C:/Users/qkrdm/Desktop/nba/nba-live-log/nba_live.log"

if not os.path.exists(local_path):
    print(f"❌ 파일이 존재하지 않습니다: {local_path}")
else:
    print(f"✅ 파일이 존재합니다: {local_path}")

# 파일 업로드
sftp.put(local_path, remote_path)

print(f"파일이 {host}:{remote_path} 에 업로드되었습니다.")

# 연결 종료
sftp.close()
ssh.close()

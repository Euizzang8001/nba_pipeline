import requests
import dotenv
import os
import psycopg2
from airflow.models import TaskInstance
from datetime import datetime, timezone, timedelta
import json
import paramiko
import pendulum

dotenv.load_dotenv(verbose=True)

db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_port = os.getenv("DB_PORT")

def listen_new_data(task_instance: TaskInstance):
    conn = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password,
        port=db_port
    )
    cur = conn.cursor()
    eastern_tz = pendulum.timezone("America/New_York")
    today_date = pendulum.now(eastern_tz)
    today_str_date = today_date.strftime('%Y-%m-%d')
    try:
        today_games = task_instance.xcom_pull(task_ids='listen_new_data', key=today_str_date)

        if today_games is None:
            today_games_json = requests.get("https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json").json()
            today_game_list = []

            for game in today_games_json['scoreboard']['games']:
                game_id = game['gameId']
                today_game_list.append(game_id)

            task_instance.xcom_push(value=today_game_list, key=today_str_date)
            for game_id in today_game_list:
                cur.execute("select * from nba_game_action_number where game_id = %s;", (game_id,))
                if cur.rowcount == 0:
                    cur.execute("insert into nba_game_action_number values (%s, %s);",
                                (game_id, 0))
                    conn.commit()

        cur.close()
        conn.close()
        return True
    except Exception as e:
        print("에러: ", e)
        conn.rollback()
        return False

def test1(task_instance: TaskInstance):
    eastern_tz = pendulum.timezone("America/New_York")
    today_date = pendulum.now(eastern_tz)
    today_str_date = today_date.strftime('%Y-%m-%d')
    today_games = task_instance.xcom_pull(task_ids='listen_new_data', key=today_str_date)
    print(today_games)
    return True

def send_live_game_data(task_instance: TaskInstance, game_num: int, **kwargs):
    eastern_tz = pendulum.timezone("America/New_York")
    today_date = pendulum.now(eastern_tz)
    today_str_date = today_date.strftime('%Y-%m-%d')
    game_ids = task_instance.xcom_pull(task_ids=f'listen_new_data', key=today_str_date)

    conn = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password,
        port=db_port
    )

    cur = conn.cursor()

    if len(game_ids)<=game_num:
        print(f'No game{game_num} found')
        return True

    game_id = game_ids[game_num]
    cur.execute("select * from nba_game_action_number where game_id = %s;", (game_id,))
    action_number = int(cur.fetchone()[1])
    print(action_number)

    host = "server02.hadoop.com"
    port = 22  # SSH 기본 포트
    username = "root"
    password = "adminuser"

    remote_log_path = "/home/nba_project/working/nba-live-log/nba-live.log"

    # 🔹 SSH 연결
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, port, username, password)

    today_game = requests.get(f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json").json()

    for game in today_game['game']['actions']:
        if game['actionNumber'] > action_number:
            log_entry = json.dump   s(game, ensure_ascii=False).replace("'", '"')  # 작은따옴표 문제 해결
            command = f"echo '{log_entry}' >> {remote_log_path}"

            stdin, stdout, stderr = ssh.exec_command(command)
            error_msg = stderr.read().decode()

            if error_msg:
                print(f"❌ {game_id}오류 발생: {error_msg}")
            else:
                print(f"✅ {game_id}로그 추가됨: {log_entry}")
            action_number = game['actionNumber']
        else:
            continue
    cur.execute("update nba_game_action_number set action_number = %s where game_id = %s;", (action_number, game_id))
    conn.commit()
    cur.close()
    print(f"{game_id}완료")
import requests
import dotenv
import os
import psycopg2
from airflow.models import TaskInstance
from datetime import datetime, timedelta

def listen_new_data(task_instance: TaskInstance):
    today_date = datetime.today()
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
    except:
        task_instance.xcom_push(key=today_str_date, value=None)

    return True

def test1(task_instance: TaskInstance):
    today_date = datetime.today()
    today_str_date = today_date.strftime('%Y-%m-%d')
    today_games = task_instance.xcom_pull(task_ids='listen_new_data', key=today_str_date)
    print(today_games)
    return True
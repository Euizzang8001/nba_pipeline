import nba_api as nba
import requests
import dotenv
import os
import psycopg2

dotenv.load_dotenv(verbose=True)

db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_port = os.getenv("DB_PORT")

conn = psycopg2.connect(
    host = db_host,
    database = db_name,
    user = db_user,
    password = db_password,
    port = db_port
)

cur = conn.cursor()

today_games = requests.get("https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json").json()

# print(today_games)
for game in today_games['scoreboard']['games']:
    game_id = game['gameId']
    game_date = game["gameCode"][0:8]
    game_away = game["gameCode"][9:12]
    game_home = game["gameCode"][12:15]
    cur.execute("select * from nba_game_schedule where game_id = %s;", (game_id,))
    if cur.rowcount == 0:
        cur.execute("insert into nba_game_schedule values (%s, %s, %s, %s);", (game_id, game_date, game_home, game_away))


conn.commit()

cur.close()
conn.close()
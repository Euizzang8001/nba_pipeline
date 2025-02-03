import requests

game_id = "0022400698"

today_games = requests.get(f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json").json()

for game in today_games['game']['actions']:
    print(game)
import requests 
import json 
import pandas as pd
import pandas_gbq 
from google.cloud import bigquery as bq
from google.cloud import secretmanager as sm
from time import sleep
import datetime 
from flask import Flask
import os
import threading 
from datetime import datetime, timezone
import pyarrow as pa

season = str(datetime.now().year)

###################   API Requests   ###################

# Retrieves the list of datasets from the BigQuery project
def getDatasets():
    client = bq.Client(project='aflanalyticsproject')  # Instantiate BigQuery client
    datasets = list(client.list_datasets())  # List all datasets in the project
    project = client.project
    print("Datasets in project {}:".format(project))
    for dataset in datasets:
        print("\t{}".format(dataset.dataset_id))  # Print each dataset ID

# Retrieves the secret API key stored in Google Secret Manager
def getSecret():
    client = sm.SecretManagerServiceClient()
    name = f"projects/aflanalyticsproject/secrets/Sport-Data-Keys/versions/1"  # Secret resource name
    response = client.access_secret_version(request={"name": name})  # Access the secret
    return response.payload.data.decode("UTF-8")  # Decode and return the secret value

# Fetches team data from the AFL API and returns it as a DataFrame
def getTeams(secret):
    headers = {
        'x-rapidapi-key': secret,
        'x-rapidapi-host': 'v1.afl.api-sports.io'
    }
    url = "https://v1.afl.api-sports.io/teams"
    payload = {}
    r = make_request_with_retries(url, headers, payload)  # API request with retry logic
    r = json.loads(r.text)['response']  # Parse response
    return pd.DataFrame(r)  # Return as DataFrame

# Fetches player statistics and compiles them into a DataFrame
def getPlayerStats(secret):
    headers = {
        'x-rapidapi-key': secret,
        'x-rapidapi-host': 'v1.afl.api-sports.io'
    }
    output = pd.DataFrame(columns=[
        'player_id', 'games_played', 'goals_total', 'assists_total', 'behinds_total',
        'disposals_total', 'kicks_total', 'handballs_total', 'marks_total', 'tackles_total',
        'hitouts_total', 'clearances_total', 'free_kicks_for_total', 'free_kicks_against_total'
    ])
    players = getPlayers(secret)  # Fetch all players
    for p in players['id']:
        #url = f"https://v1.afl.api-sports.io/players/statistics?season={season}&id={p}"
        url = f"https://v1.afl.api-sports.io/players/statistics?season=2024&id={p}"
        payload = {}
        r = make_request_with_retries(url, headers, payload)  # Request player stats
        r = json.loads(r.text)['response'][0]  # Parse response
        # Flatten nested JSON structure
        flat_data = {
            'player_id': r['player']['id'],
            'games_played': r['statistics']['games']['played'],
            'goals_total': r['statistics']['goals']['total']['total'],
            'assists_total': r['statistics']['goals']['assists']['total'],
            'behinds_total': r['statistics']['behinds']['total'],
            'disposals_total': r['statistics']['disposals']['total'],
            'kicks_total': r['statistics']['kicks']['total'],
            'handballs_total': r['statistics']['handballs']['total'],
            'marks_total': r['statistics']['marks']['total'],
            'tackles_total': r['statistics']['tackles']['total'],
            'hitouts_total': r['statistics']['hitouts']['total'],
            'clearances_total': r['statistics']['clearances']['total'],
            'free_kicks_for_total': r['statistics']['free_kicks']['for']['total'],
            'free_kicks_against_total': r['statistics']['free_kicks']['against']['total']
        }
        df = pd.DataFrame([flat_data])  # Convert to DataFrame
        output = pd.concat([output, df])  # Append to output DataFrame
        print(f"Fetched player stats for player {p}")
        sleep(0.21)  # Prevent rate-limiting
    return output

# Fetches player data from the AFL API and returns it as a DataFrame
def getPlayers(secret):
    headers = {
        'x-rapidapi-key': secret,
        'x-rapidapi-host': 'v1.afl.api-sports.io'
    }
    output = pd.DataFrame(columns=['id', 'name', 'teamID'])
    teams = getTeams(secret)  # Fetch all teams
    for t in teams['id']:
        #url = f"https://v1.afl.api-sports.io/players?season={season}&team={t}"
        url = f"https://v1.afl.api-sports.io/players?season=2024&team={t}"
        payload = {}
        r = make_request_with_retries(url, headers, payload)  # API request
        r = json.loads(r.text)['response']
        df = pd.DataFrame(r)  # Convert response to DataFrame
        df['teamID'] = t  # Add team ID column
        output = pd.concat([output, df])  # Append to output DataFrame
        sleep(0.25)  # Prevent rate-limiting
    return output

# Fetches match statistics for players and compiles them into a DataFrame
def getMatchStatsPlayers(secret):
    headers = {
        'x-rapidapi-key': secret,
        'x-rapidapi-host': 'v1.afl.api-sports.io'
    }
    output = pd.DataFrame(columns=[
        'game_id', 'team_id', 'player_id', 'player_number', 'goals_total', 'goals_assists',
        'behinds', 'disposals', 'kicks', 'handballs', 'marks', 'tackles', 'hitouts',
        'clearances', 'free_kicks_for', 'free_kicks_against'
    ])
    games = getMatches(secret)  # Fetch all games
    for g in games['game_id']:
        url = f"https://v1.afl.api-sports.io/games/statistics/players?id={g}"
        payload = {}
        r = make_request_with_retries(url, headers, payload)  # API request
        r = json.loads(r.text)['response']  # Parse response
        rows = []  # Initialize list for storing player data
        for game in r:
            game_id = game['game']['id']
            for team in game['teams']:
                team_id = team['team']['id']
                for player in team['players']:
                    # Flatten nested JSON structure
                    player_data = {
                        'game_id': game_id,
                        'team_id': team_id,
                        'player_id': player['player']['id'],
                        'player_number': player['player']['number'],
                        'goals_total': player['goals']['total'],
                        'goals_assists': player['goals']['assists'],
                        'behinds': player['behinds'],
                        'disposals': player['disposals'],
                        'kicks': player['kicks'],
                        'handballs': player['handballs'],
                        'marks': player['marks'],
                        'tackles': player['tackles'],
                        'hitouts': player['hitouts'],
                        'clearances': player['clearances'],
                        'free_kicks_for': player['free_kicks']['for'],
                        'free_kicks_against': player['free_kicks']['against']
                    }
                    rows.append(player_data)
        df = pd.DataFrame(rows)  # Convert rows to DataFrame
        output = pd.concat([output, df])  # Append to output DataFrame
    return output

# Fetches match data from the AFL API and compiles it into a DataFrame
def getMatches(secret):
    headers = {
        'x-rapidapi-key': secret,
        'x-rapidapi-host': 'v1.afl.api-sports.io'
    }
    url = "https://v1.afl.api-sports.io/games?season=2024&league=1"
    payload = {}
    r = make_request_with_retries(url, headers, payload)  # API request
    r = json.loads(r.text)['response']  # Parse response
    output = pd.DataFrame({
    "game_id": pd.Series(dtype='int64'),
    "season": pd.Series(dtype='int64'),
    "date": pd.Series(dtype='int'),
    "round": pd.Series(dtype='str'),
    "week": pd.Series(dtype='str'),
    "venue": pd.Series(dtype='str'),
    "status": pd.Series(dtype='str'),
    "home_team_id": pd.Series(dtype='int64'),
    "away_team_id": pd.Series(dtype='int64'),
    "home_score": pd.Series(dtype='int64'),
    "home_goals": pd.Series(dtype='int64'),
    "home_behinds": pd.Series(dtype='int64'),
    "home_psgoals": pd.Series(dtype='int64'),
    "home_psbehinds": pd.Series(dtype='int64'),
    "away_score": pd.Series(dtype='int64'),
    "away_goals": pd.Series(dtype='int64'),
    "away_behinds": pd.Series(dtype='int64'),
    "away_psgoals": pd.Series(dtype='int64'),
    "away_psbehinds": pd.Series(dtype='int64'),
    })

    for i in r:
        # Flatten nested JSON structure
        matchData = {
            "game_id": i['game']['id'],
            "season": i['league']['season'],
            "date": i['date'],
            "round": i['round'],
            "week": i['week'],
            "venue": i['venue'],
            "status": i['status']['long'],
            "home_team_id": i['teams']['home']['id'],
            "away_team_id": i['teams']['away']['id'],
            "home_score": i['scores']['home']['score'],
            "home_goals": i['scores']['home']['goals'],
            "home_behinds": i['scores']['home']['behinds'],
            "home_psgoals": i['scores']['home']['psgoals'],
            "home_psbehinds": i['scores']['home']['psbehinds'],
            "away_score": i['scores']['away']['score'],
            "away_goals": i['scores']['away']['goals'],
            "away_behinds": i['scores']['away']['behinds'],
            "away_psgoals": i['scores']['away']['psgoals'],
            "away_psbehinds": i['scores']['away']['psbehinds']
        }
        df = pd.DataFrame([matchData])  # Convert match data to DataFrame
        output = pd.concat([output, df])  # Append to output DataFrame
    
    #cast all values to correct type
    output['date'] = pd.to_datetime(output['date'])
    output['round'] = output['round'].astype(str)
    output['week'] = output['week'].astype(str)
    output['venue'] = output['venue'].astype(str)
    output['status'] = output['status'].astype(str)
    return output

# Helper function to handle API requests with retries
def make_request_with_retries(url, headers, payload):
    retries = 0
    while retries < 3:  # Retry up to 3 times
        try:
            r = requests.request("GET", url, headers=headers, data=payload)  # Make API request
            if r and r.text:  # Check for a valid response
                return r
            else:
                print(f"Attempt {retries + 1}: Empty response. Retrying...")
        except requests.RequestException as e:
            print(f"Attempt {retries + 1}: Error occurred: {e}. Retrying...")
        retries += 1
        sleep(1)  # Wait before retrying

###################   GCP BigQuery Table Updates   ###################

# Replaces a BigQuery table with new data
def replaceTable(table, df):
    t = 'AFL.' + table  # Define table name
    df = clean_dataframe(df)
    pandas_gbq.to_gbq(df, t, project_id='aflanalyticsproject', if_exists='replace')  # Replace table
    createLog(table, "Replace")  # Log the action

# Appends new data to an existing BigQuery table
def updateTable(table, df):
    t = 'AFL.' + table
    pandas_gbq.to_gbq(df, t, project_id='aflanalyticsproject', if_exists='append')
    createLog(table, "Update")

# Creates a new BigQuery table if it doesn't exist
def createTable(table, df, schema):
    t = 'AFL.' + table
    pandas_gbq.to_gbq(df, t, project_id='aflanalyticsproject', if_exists='fail', table_schema=schema)
    createLog(table, "Create")

# Logs table actions to a logging table in BigQuery
def createLog(t, action):
    current_utc_time = datetime.now(timezone.utc)  # Get current UTC time
    data = {"table": t, "utctimestamp": current_utc_time, "action": action}  # Create log data
    df = pd.DataFrame([data])  # Convert to DataFrame
    pandas_gbq.to_gbq(df, "AFL.Logs", project_id='aflanalyticsproject', if_exists='append')  # Append log to BigQuery

# Clean DF
def clean_dataframe(df):
    # Ensure all int64 columns are numeric, filling invalid values with 0
    int_columns = df.select_dtypes(include=['int64']).columns
    for col in int_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
        print(str(col) + " int")  

    # Ensure datetime columns are parsed correctly
    datetime_columns = df.select_dtypes(include=['datetime64[ns]']).columns
    for col in datetime_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        print(str(col) + " date")

    # Ensure string columns are consistent and replace NaN with empty strings
    str_columns = df.select_dtypes(include=['string']).columns
    for col in str_columns:
        df[col] = df[col].fillna('').astype('str')
        print(str(col) + " str")

    #for col, dtype in df.dtypes.items():
        #print(f"Column: {col}, Data Type: {dtype}")

    return df

###################   Flask App   ###################

# Initialize Flask application
app = Flask(__name__)

# Endpoint to update player data
@app.route("/players")
def updatePlayers():
    secret = getSecret()
    players = getPlayers(secret)
    replaceTable('Players', players)
    return "Players updated successfully!", 200

# Endpoint to update all data in parallel
@app.route("/all")
def updateAll():
    # Create threads for updating data
    thread1 = threading.Thread(target=updatePlayers)
    thread2 = threading.Thread(target=updateTeams)
    thread3 = threading.Thread(target=updatePlayerStats)
    thread4 = threading.Thread(target=updatePlayerMatchStats)
    thread5 = threading.Thread(target=updateMatches)

    # Start threads
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()

    # Wait for threads to complete
    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()

    return "All tables updated successfully!", 200

# Similar endpoints for other update functions
@app.route("/teams")
def updateTeams():
    secret = getSecret()
    teams = getTeams(secret)
    replaceTable('Teams', teams)
    return "Teams updated successfully!", 200

@app.route("/playerstats")
def updatePlayerStats():
    secret = getSecret()
    playerStatistics = getPlayerStats(secret)
    replaceTable('PlayerStatistics', playerStatistics)
    return "Player's stats updated successfully!", 200

@app.route("/playermatchstats")
def updatePlayerMatchStats():
    secret = getSecret()
    playerMatchStatistics = getMatchStatsPlayers(secret)
    playerMatchStatistics = playerMatchStatistics.astype(str)  # Convert to string for compatibility
    replaceTable('PlayerMatchStatistics', playerMatchStatistics)
    return "Player's match stats returned successfully!", 200

@app.route("/matches")
def updateMatches():
    secret = getSecret()
    matches = getMatches(secret)
    replaceTable('Matches', matches)
    return "Matches updated successfully", 200

# Uncomment the following to run the Flask app locally
# if __name__ == "__main__":
#     app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

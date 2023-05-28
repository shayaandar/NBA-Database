#getting data from the nba api

import requests
import sqlite3
import time
import os
import json
import nba_api 
import pandas as pd 
from nba_api.stats.endpoints import commonplayerinfo, playercareerstats, commonteamroster, teamplayerdashboard
from nba_api.stats.static.players import get_players
from nba_api.stats.static.teams import teams

# Index stats 
stat_names = ["PLAYER_ID", "SEASON_ID", "LEAGUE_ID", "TEAM_ID", "TEAM", "PLAYER_AGE", "GP", "GS", "MIN", "FGM","FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT", "FTM", "FTA", "FT_PCT", "OREB", "DREB", "REB", "AST", "STL", "BLK", "TOV", "PF", "PTS"]
index = {stat_names[i]: i for i in range(len(stat_names))}

def set_up_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def set_up_table(cur, conn):
    #cur.execute('DROP TABLE IF EXISTS NBA') #only if resetting the tables
    #cur.execute('DROP TABLE IF EXISTS players') #only if resetting the tables
    cur.execute('CREATE TABLE IF NOT EXISTS players (player_id INTEGER PRIMARY KEY, name TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS regular_season_stats (player_id INTEGER, season_id TEXT, league_id TEXT, team TEXT, minutes FLOAT, age INTEGER, gamesplayed INTEGER, points FLOAT, rebounds FLOAT, assists FLOAT, field_goal_attempts FLOAT, field_goal_percentage FLOAT, three_attempts FLOAT, three_percentage FLOAT, freethrow_attempts FLOAT, freethrow_percentage FLOAT, steals FLOAT, blocks FLOAT, turnovers INTEGER,FOREIGN KEY (player_id) references players(player_id))')
    cur.execute('CREATE TABLE IF NOT EXISTS post_season_stats (player_id INTEGER, season_id TEXT, league_id TEXT, team TEXT, minutes FLOAT, age INTEGER, gamesplayed INTEGER, points FLOAT, rebounds FLOAT, assists FLOAT, field_goal_attempts FLOAT, field_goal_percentage FLOAT, three_attempts FLOAT, three_percentage FLOAT, freethrow_attempts FLOAT, freethrow_percentage FLOAT, steals FLOAT, blocks FLOAT, turnovers INTEGER,FOREIGN KEY (player_id) references players(player_id))')

    conn.commit()

def insert_players(cur, conn):
    players = pd.DataFrame.from_dict(get_players())
    for index,row in players.iterrows():
        cur.execute('INSERT INTO players (player_id, name) VALUES (?,?)', (int(row['id']),row['full_name']))
    conn.commit()

def insert_regular_season_stats(cur, conn,max_retries=3, backoff_factor=3):
    # Count 
    count = 0 
    # Get player list 
    players = cur.execute('SELECT player_id FROM players ORDER BY player_id').fetchall()
    retries = 0
    delay = 1
    # Retry if API call fails 
    while retries < max_retries:
        for x in players:
            count=count+1 
            try:
                # Get career stats 
                stat_req = playercareerstats.PlayerCareerStats(player_id=x[0], per_mode36='PerGame')
                reg_stats = stat_req.season_totals_regular_season.get_dict()['data']
                for season in reg_stats:
                    cur.execute('INSERT INTO regular_season_stats (player_id, season_id, league_id, team, minutes, age, gamesplayed, points, rebounds, assists, field_goal_attempts, field_goal_percentage, three_attempts, three_percentage, freethrow_attempts, freethrow_percentage, steals, blocks, turnovers) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (season[index['PLAYER_ID']], season[index['SEASON_ID']], season[index['LEAGUE_ID']], season[index['TEAM']], season[index['MIN']], season[index['PLAYER_AGE']], season[index['GP']], season[index['PTS']], season[index['REB']], season[index['AST']], season[index['FGA']], season[index['FG_PCT']], season[index['FG3A']],season[index['FG3_PCT']], season[index['FTA']], season[index['FT_PCT']],season[index['STL']], season[index['BLK']],season[index['TOV']]))
                    print(f"Processing player id: {x[0]} in season: {season[index['SEASON_ID']]} number {count} of {len(players)}")
                    conn.commit()
            except Exception as e:
                print(e)
                print("Errored on index: "+count)
                retries += 1
                delay *= backoff_factor
                time.sleep(delay)
    print("Completed")

def insert_post_season_stats(cur, conn,max_retries=3, backoff_factor=3):
    # Count 
    count = 0 
    # Get player list 
    players = cur.execute('SELECT player_id FROM players ORDER BY player_id').fetchall()
    retries = 0
    delay = 1
    # Retry if API call fails 
    while retries < max_retries:
        for x in players:
            count=count+1 
            try:
                # Get career stats 
                stat_req = playercareerstats.PlayerCareerStats(player_id=x[0], per_mode36='PerGame')
                post_stats = stat_req.season_totals_post_season.get_dict()['data']
                for season in post_stats:
                    cur.execute('INSERT INTO regular_season_stats (player_id, season_id, league_id, team, minutes, age, gamesplayed, points, rebounds, assists, field_goal_attempts, field_goal_percentage, three_attempts, three_percentage, freethrow_attempts, freethrow_percentage, steals, blocks, turnovers) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (season[index['PLAYER_ID']], season[index['SEASON_ID']], season[index['LEAGUE_ID']], season[index['TEAM']], season[index['MIN']], season[index['PLAYER_AGE']], season[index['GP']], season[index['PTS']], season[index['REB']], season[index['AST']], season[index['FGA']], season[index['FG_PCT']], season[index['FG3A']],season[index['FG3_PCT']], season[index['FTA']], season[index['FT_PCT']],season[index['STL']], season[index['BLK']],season[index['TOV']]))
                    print(f"Processing player id: {x[0]} in season: {season[index['SEASON_ID']]} number {count} of {len(players)}")
                    conn.commit()
            except Exception as e:
                print(e)
                print("Errored on index: "+count)
                retries += 1
                delay *= backoff_factor
                time.sleep(delay)
    print("Completed")

def update_database(cur, conn):
    try:
        insert_players(cur, conn)
        insert_regular_season_stats(cur, conn)
        insert_post_season_stats(cur,conn)
        conn.commit()
    except Exception as e:
        try:
             insert_regular_season_stats(cur, conn)
             insert_post_season_stats(cur,conn)
             conn.commit()
        except Exception as e:
            print(e)         
        
if __name__ == '__main__':
    cur, conn = set_up_database('NBA_statistics.db')
    set_up_table(cur, conn)
    update_database(cur,conn)
    
    
#getting data from the nba api

import requests
import sqlite3
import random
import time
import os
import json
import nba_api 
import pandas as pd 
import player_accolades
import player_stats 
import team_data
import player_info
from nba_api.stats.endpoints import commonplayerinfo, playercareerstats, commonteamroster, teamplayerdashboard
from nba_api.stats.static.players import get_players
from nba_api.stats.static.teams import teams 

def set_up_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def set_up_table(cur, conn):
    #cur.execute('DROP TABLE IF EXISTS teams') #only if resetting the tables
    #cur.execute('DROP TABLE IF EXISTS players') #only if resetting the tables
    cur.execute('CREATE TABLE IF NOT EXISTS players (player_id INTEGER PRIMARY KEY, name TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS teams (team_id INTEGER PRIMARY KEY, fullname TEXT, abbreviation TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS regular_season_stats (player_id INTEGER, season_id TEXT, league_id TEXT, team TEXT, minutes FLOAT, age INTEGER, gamesplayed INTEGER, points FLOAT, rebounds FLOAT, assists FLOAT, field_goal_attempts FLOAT, field_goal_percentage FLOAT, three_attempts FLOAT, three_percentage FLOAT, freethrow_attempts FLOAT, freethrow_percentage FLOAT, steals FLOAT, blocks FLOAT, turnovers INTEGER,FOREIGN KEY (player_id) references players(player_id))')
    cur.execute('CREATE TABLE IF NOT EXISTS post_season_stats (player_id INTEGER, season_id TEXT, league_id TEXT, team TEXT, minutes FLOAT, age INTEGER, gamesplayed INTEGER, points FLOAT, rebounds FLOAT, assists FLOAT, field_goal_attempts FLOAT, field_goal_percentage FLOAT, three_attempts FLOAT, three_percentage FLOAT, freethrow_attempts FLOAT, freethrow_percentage FLOAT, steals FLOAT, blocks FLOAT, turnovers INTEGER,FOREIGN KEY (player_id) references players(player_id))')
    cur.execute('CREATE TABLE IF NOT EXISTS player_awards (player_id INTEGER, name TEXT, team TEXT, description TEXT, all_nba_team INTEGER, season TEXT, conference TEXT, FOREIGN KEY (player_id) references players(player_id))')
    conn.commit()

def insert_players(cur, conn):
    try:
        # Create dataframe of player name and player id 
        players = pd.DataFrame.from_dict(get_players())
        # Add each player to database 
        for index,row in players.iterrows():
            cur.execute('INSERT INTO players (player_id, name) VALUES (?,?)', (int(row['id']),row['full_name']))
        conn.commit()
    except:
        return "Players table already populated"

def update_player_stats(cur, conn):
    try:
        insert_players(cur, conn)
        player_stats.insert_regular_season_stats(cur, conn)
        player_stats.insert_post_season_stats(cur,conn)
        conn.commit()
    except Exception as e:
            print(e)         
        
if __name__ == '__main__':
    cur, conn = set_up_database('NBA_statistics.db')
    set_up_table(cur, conn)
    #update_player_stats(cur,conn)
    #team_data.insert_teams(cur,conn)
    #player_accolades.insert_player_awards(cur,conn)
    player_info.get_player_info(cur,conn)
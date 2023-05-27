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

def set_up_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def set_up_table(cur, conn):
    #cur.execute('DROP TABLE IF EXISTS NBA') #only if resetting the tables
    #cur.execute('DROP TABLE IF EXISTS players') #only if resetting the tables
    cur.execute('CREATE TABLE IF NOT EXISTS players (player_id INTEGER PRIMARY KEY, name TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS NBA (player_id INTEGER, season_id TEXT, team TEXT, minutes FLOAT, points FLOAT, rebounds FLOAT, assists FLOAT, field_goal_percentage FLOAT, three_percentage FLOAT, steals FLOAT, blocks FLOAT,FOREIGN KEY (player_id) references players(player_id))')
    conn.commit()

def insert_players(cur, conn):
    players = pd.DataFrame.from_dict(get_players())
    for index,row in players.iterrows():
        cur.execute('INSERT INTO players (player_id, name) VALUES (?,?)', (int(row['id']),row['full_name']))
    conn.commit()

def insert_stats(cur, conn,max_retries=3, backoff_factor=2):
    stat_names = ["PLAYER_ID", "SEASON_ID", "LEAGUE_ID", "TEAM_ID", "TEAM", "PLAYER_AGE", "GP", "GS", "MIN", "FGM",
    "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT", "FTM", "FTA", "FT_PCT", "OREB", "DREB", "REB", "AST", "STL", "BLK", "TOV", "PF", "PTS"]
    index = {stat_names[i]: i for i in range(len(stat_names))}
    count = 0 
    players = cur.execute('SELECT player_id FROM players ORDER BY player_id').fetchall()
    for id in players:
        count=count+1 
        retries = 0
        delay = 1
        while retries < max_retries:
            try:
                stats = playercareerstats.PlayerCareerStats(player_id=id[0], per_mode36='PerGame').get_dict()
                for season in stats['resultSets'][0]['rowSet']:
                    cur.execute('INSERT INTO NBA (player_id, season_id, team, minutes, points, rebounds, assists, field_goal_percentage, three_percentage, steals, blocks) \
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)', (season[index['PLAYER_ID']], season[index['SEASON_ID']], season[index['TEAM']], season[index['MIN']], season[index['PTS']], \
                            season[index['REB']], season[index['AST']], season[index['FG_PCT']], season[index['FG3_PCT']], season[index['STL']], season[index['BLK']],))
                    print(f"Finished player id: {id}, number {count} of {len(players)}")
                    conn.commit()
            except:
                retries += 1
                delay *= backoff_factor
                time.sleep(delay)


def update_database(cur, conn):
    insert_players(cur, conn)
    insert_stats(cur, conn)      
    conn.commit()
        

if __name__ == '__main__':

    cur, conn = set_up_database('player_stats.db')
    set_up_table(cur, conn)
    update_database(cur, conn)
    
    
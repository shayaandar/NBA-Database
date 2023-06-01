import requests
import sqlite3
import random
import time
import os
import main_database
import json
import nba_api 
import pandas as pd 
import player_accolades
import player_stats 
from nba_api.stats.endpoints import commonplayerinfo, playercareerstats, commonteamroster, teamplayerdashboard
from nba_api.stats.static.players import get_players

info_names = ['PERSON_ID', 'FIRST_NAME', 'LAST_NAME', 'DISPLAY_FIRST_LAST', 'DISPLAY_LAST_COMMA_FIRST', 'DISPLAY_FI_LAST', 'PLAYER_SLUG', 'BIRTHDATE', 'SCHOOL', 'COUNTRY', 'LAST_AFFILIATION', 'HEIGHT', 'WEIGHT', 'SEASON_EXP', 'JERSEY', 'POSITION', 'ROSTERSTATUS', 'TEAM_ID', 'TEAM_NAME', 'TEAM_ABBREVIATION', 'TEAM_CODE', 'TEAM_CITY', 'PLAYERCODE', 'FROM_YEAR', 'TO_YEAR', 'DLEAGUE_FLAG', 'NBA_FLAG', 'GAMES_PLAYED_FLAG', 'DRAFT_YEAR', 'DRAFT_ROUND', 'DRAFT_NUMBER']
index = {info_names[i]: i for i in range(len(info_names))}

def add_table(cur,conn):
    #cur.execute('DROP TABLE IF EXISTS player_info') #only if resetting the tables
    cur.execute('CREATE TABLE IF NOT EXISTS player_info (player_id INTEGER PRIMARY KEY, name TEXT, school TEXT, country TEXT, height TEXT, weight INTEGER, position TEXT, rosterstatus TEXT, seasons_played INTEGER, team_id INTEGER, team TEXT, draft_year INTEGER, draft_round INTEGER, draft_no INTEGER)')

def get_player_info(cur,conn):
    count = 0
    players = cur.execute('SELECT player_id FROM players ORDER BY player_id').fetchall()
    for x in players:
        try:
            count = count+1
            info = commonplayerinfo.CommonPlayerInfo(player_id=x[0]).get_dict()
            for result in info['resultSets'][0]['rowSet']:
                print(f"Player info: Processing player id: {x[0]}, {count} of {len(players)}")
                cur.execute('INSERT INTO player_info (player_id, name , school , country , height , weight , position , rosterstatus , seasons_played, team_id  , team , draft_year , draft_round , draft_no) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (result[index['PERSON_ID']],result[index['FIRST_NAME']]+" "+result[index['LAST_NAME']],result[index['SCHOOL']],result[index['COUNTRY']],result[index['HEIGHT']],result[index['WEIGHT']],result[index['POSITION']],result[index['ROSTERSTATUS']], result[index['SEASON_EXP']],result[18],result[19],result[29],result[30],result[31]))
                conn.commit() 
                time.sleep(random.randint(0,2))
        except Exception as e:
            print(e)
            print("Errored on index: "+str(count))
            if isinstance(e, requests.exceptions.ReadTimeout):
                print("Sleeping for 30 minutes")
                time.sleep(1805)
    print("Completed")

if __name__ == '__main__':
    cur, conn = main_database.set_up_database('NBA_statistics.db')
    add_table(cur, conn)
    get_player_info(cur,conn)
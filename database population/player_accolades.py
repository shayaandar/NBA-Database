
import requests
import sqlite3
import random
import time
import os
import json
import nba_api 
import pandas as pd 
from nba_api.stats.endpoints import commonplayerinfo, playercareerstats, commonteamroster, teamplayerdashboard, playerawards
from nba_api.stats.static.teams import teams 
import main_database

award_data = ['PERSON_ID', 'FIRST_NAME', 'LAST_NAME', 'TEAM', 'DESCRIPTION', 'ALL_NBA_TEAM_NUMBER', 'SEASON', 'MONTH', 'WEEK', 'CONFERENCE', 'TYPE', 'SUBTYPE1', 'SUBTYPE2', 'SUBTYPE3']
index = {award_data[i]: i for i in range(len(award_data))}

def create_award_table(cur,conn):
    cur.execute('CREATE TABLE IF NOT EXISTS player_awards (player_id INTEGER, name TEXT, team TEXT, description TEXT, all_nba_team INTEGER, season TEXT, conference TEXT)')
    conn.commit() 

def insert_player_awards(cur,conn):
    count = 0
    players = cur.execute('SELECT player_id FROM players ORDER BY player_id').fetchall()
    for x in players:
        count = count+1
        try:
            awards = playerawards.PlayerAwards(player_id=x[0]).get_dict()
            for award in awards['resultSets'][0]['rowSet']:
                cur.execute('INSERT INTO player_awards (player_id, name, team, description, all_nba_team_number, season, conference) VALUES (?,?,?,?,?,?,?)', (award[index['PERSON_ID']], award[index['FIRST_NAME']]+ " " + award[index[ 'LAST_NAME']], award[index['TEAM']],  award[index['DESCRIPTION']],  award[index['ALL_NBA_TEAM_NUMBER']], award[index['SEASON']],  award[index['CONFERENCE']]))
                conn.commit()
                time.sleep(random.randint(0,2)) 
        except Exception as e:
            print(e)
            print("Errored on index: "+str(count))
            if isinstance(e, requests.exceptions.ReadTimeout):
                print("Sleeping for 30 minutes")
                time.sleep(1805)
    print("Completed")



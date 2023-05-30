import requests
import sqlite3
import random
import time
import os
import json
import nba_api 
import main_database 
import pandas as pd 
from nba_api.stats.endpoints import commonplayerinfo, playercareerstats, commonteamroster, teamplayerdashboard
from nba_api.stats.static.players import get_players
from nba_api.stats.static.teams import find_team_by_abbreviation

def insert_teams(cur,conn):
    team_list = cur.execute('SELECT distinct team FROM regular_season_stats').fetchall() 
    for x in team_list:
        try:
          # Get career stats 
            team_data = find_team_by_abbreviation(x[0])
            if team_data==None:
                continue
            print(f"Processing team: {x[0]}")
            cur.execute('INSERT INTO teams (team_id, fullname, abbreviation) VALUES (?,?,?)', (team_data['id'],team_data['full_name'],team_data['abbreviation'])) 
            conn.commit()
            time.sleep(random.randint(0,2))
        except Exception as e:
            print(e)
            if isinstance(e, requests.exceptions.ReadTimeout):
                print("Sleeping for 30 minutes")
                time.sleep(1805)
    print("Completed")
    
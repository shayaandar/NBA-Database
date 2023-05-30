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
from nba_api.stats.endpoints import commonplayerinfo, playercareerstats, commonteamroster, teamplayerdashboard
from nba_api.stats.static.players import get_players

info_names = ['PERSON_ID', 'FIRST_NAME', 'LAST_NAME', 'DISPLAY_FIRST_LAST', 'DISPLAY_LAST_COMMA_FIRST', 'DISPLAY_FI_LAST', 'PLAYER_SLUG', 'BIRTHDATE', 'SCHOOL', 'COUNTRY', 'LAST_AFFILIATION', 'HEIGHT', 'WEIGHT', 'SEASON_EXP', 'JERSEY', 'POSITION', 'ROSTERSTATUS', 'TEAM_ID', 'TEAM_NAME', 'TEAM_ABBREVIATION', 'TEAM_CODE', 'TEAM_CITY', 'PLAYERCODE', 'FROM_YEAR', 'TO_YEAR', 'DLEAGUE_FLAG', 'NBA_FLAG', 'GAMES_PLAYED_FLAG', 'DRAFT_YEAR', 'DRAFT_ROUND', 'DRAFT_NUMBER']
index = {info_names[i]: i for i in range(len(info_names))}

def add_table(cur,conn):
    cur.execute('CREATE TABLE IF NOT EXISTS player_info (player_id INTEGER PRIMARY KEY, name TEXT, school TEXT, country TEXT, height TEXT, weight INTEGER, position TEXT, rosterstatus TEXT, team_id TEXT INTEGER, team TEXT, team_abbv TEXT, draft_year INTEGER, draft_round INTEGER, draft_no INTEGER)')

def get_player_info(cur,conn):
    count = 0
    players = cur.execute('SELECT player_id FROM players ORDER BY player_id').fetchall()
    for x in players:
        count = count+1
        info = commonplayerinfo.CommonPlayerInfo(player_id=x[0]).get_dict()
        for result in info['resultSets']:
            print(result)


import sys 
sys.path.append('/Users/shayaandar/nba/nbagoat/database')
import main_database 
import requests
import sqlite3
import random
import time
import os
import json
import nba_api 
import pandas as pd 
import sys 

#Create methods to speed up common data processes 

def filter_by_name(df,name):
    return df[df['name']==name]

def filter_by_season(df,season):
    if 'season_id' in df.columns:
        return df[df['season_id']==season]
    else:
        return df[df['season']==season]

def filter_by_season_range(df,start,end):
    if 'season_id' in df.columns:
        df[['start_season','end']] = df['season_id'].str.split('-',expand=True)
        df['start_season'] = df['start_season'].astype(int)
        df = df[df['start_season']>=start]
        df = df[df['start_season']<=end]
        df = df.drop(columns=['start_season','end'])
        return df 
    else:
        df[['start_season','end']] = df['season'].str.split('-',expand=True)
        df['start_season'] = df['start_season'].astype(int)
        df = df[df['start_season']>=start]
        df = df[df['start_season']<=end]
        df = df.drop(columns=['start_season','end'])
        return df 

if __name__ == '__main__':
    # Creating views 
    cur, conn = main_database.set_up_database('NBA_statistics.db')
    #cur.execute("CREATE VIEW IF NOT EXISTS vw_regularseasonstats AS SELECT players.name,regular_season_stats.* FROM regular_season_stats LEFT JOIN players on players.player_id=regular_season_stats.player_id")
    #cur.execute("CREATE VIEW IF NOT EXISTS vw_playoffstats AS SELECT players.name,post_season_stats.* FROM post_season_stats LEFT JOIN players on players.player_id=post_season_stats.player_id")
    #cur.execute("CREATE VIEW IF NOT EXISTS vw_awardandinfo AS SELECT player_awards.*, player_info.draft_year,player_info.draft_round,player_info.draft_no FROM player_awards LEFT JOIN player_info on player_awards.player_id=player_info.player_id")

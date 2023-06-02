import requests
import sqlite3
import random
import time
import os
import json
import nba_api 
import pandas as pd 
from nba_api.stats.endpoints import commonplayerinfo, playercareerstats, commonteamroster, teamplayerdashboard, playerawards, leaguegamelog, leaguedashteamstats
from nba_api.stats.static.teams import teams 
import main_database


league_dash = ['TEAM_ID', 'TEAM_NAME', 'GP', 'W', 'L', 'W_PCT', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'BLKA', 'PF', 'PFD', 'PTS', 'PLUS_MINUS', 'GP_RANK', 'W_RANK', 'L_RANK', 'W_PCT_RANK', 'MIN_RANK', 'FGM_RANK', 'FGA_RANK', 'FG_PCT_RANK', 'FG3M_RANK', 'FG3A_RANK', 'FG3_PCT_RANK', 'FTM_RANK', 'FTA_RANK', 'FT_PCT_RANK', 'OREB_RANK', 'DREB_RANK', 'REB_RANK', 'AST_RANK', 'TOV_RANK', 'STL_RANK', 'BLK_RANK', 'BLKA_RANK', 'PF_RANK', 'PFD_RANK', 'PTS_RANK', 'PLUS_MINUS_RANK', 'CFID', 'CFPARAMS']
index = {league_dash[i]: i for i in range(len(league_dash))}

def create_leaguedata_table(cur,conn):
    cur.execute('DROP TABLE IF EXISTS league_team_data') #only if resetting the tables
    cur.execute('CREATE TABLE IF NOT EXISTS league_team_data (team_id INTEGER, team_name INTEGER, season TEXT, wins INTEGER, losses INTEGER, win_pct FLOAT, fg_attempts FLOAT, fg_pct FLOAT, fg3_attempts FLOAT, fg3_pct FLOAT, fta FLOAT, ft_pct FLOAT, rebound FLOAT, assist FLOAT, tov FLOAT, stl FLOAT, blk FLOAT, pts FLOAT, pts_rank INTEGER, fg_pct_rank INTEGER, fg3_pct_rank INTEGER, tov_rank INTEGER, stl_rank INTEGER)')
    conn.commit() 

def get_regularseason_teamstats(cur,conn):
    # Only works on 1996+
    count = 0 
    no = 0 
    season_list = cur.execute('SELECT distinct season_id FROM regular_season_stats').fetchall()
    for x in season_list:
        split = x[0].split('-')
        if int(split[0]) < 1996:
            no=no+1
            continue 
        count = count+1
        try:
            print(f"Processing: {x[0]}, {count}")
            df = leaguedashteamstats.LeagueDashTeamStats(
            team_id_nullable='0',
            league_id_nullable='00',
            season= x[0],
            per_mode_detailed='PerGame',
            season_type_all_star='Regular Season').get_data_frames()[0]
            for index,row in df.iterrows(): 
                cur.execute('INSERT INTO league_team_data (team_id, team_name, season, wins, losses, win_pct, fg_attempts, fg_pct, fg3_attempts, fg3_pct, fta, ft_pct, rebound, assist, tov, stl, blk, pts, pts_rank, fg_pct_rank, fg3_pct_rank, tov_rank, stl_rank) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (row['TEAM_ID'], row['TEAM_NAME'], x[0],row['W'],row['L'],row['W_PCT'],row['FGA'],row['FG_PCT'],row['FG3A'],row['FG3_PCT'],row['FTA'],row['FT_PCT'],row['REB'],row['AST'],row['TOV'],row['STL'],row['BLK'],row['PTS'],row['PTS_RANK'],row['FG_PCT_RANK'],row['FG3_PCT_RANK'],row['TOV_RANK'],row['STL_RANK']))
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
    create_leaguedata_table(cur,conn)
    get_regularseason_teamstats(cur,conn)
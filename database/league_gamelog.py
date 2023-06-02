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

gamelog = ['year', 'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION','TEAM_NAME', 'GAME_ID', 'GAME_DATE', 'MATCHUP', 'WL', 'MIN', 'FGM','FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT','OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS','PLUS_MINUS', 'FANTASY_PTS', 'VIDEO_AVAILABLE']
index = {gamelog[i]: i for i in range(len(gamelog))}


def create_gamelog_table(cur,conn):
    cur.execute('DROP TABLE IF EXISTS playoffs_gamelog') #only if resetting the tables
    cur.execute('CREATE TABLE IF NOT EXISTS playoffs_gamelog (year INTEGER, player_id INTEGER, name TEXT, team_id INTEGER, team TEXT, game_id INTEGER, game_date TEXT, matchup TEXT, win_loss TEXT, minutes INTEGER, fgm INTEGER, fga INTEGER, fg_pct FLOAT, fg3m INTEGER, fg3a INTEGER, fg3_pct FLOAT, ftm INTEGER, fta INTEGER, ft_pct FLOAT, oreb INTEGER, dreb INTEGER, reb INTEGER, ast INTEGER, stl INTEGER, blk INTEGER, tov INTEGER, pts INTEGER, plus_minus INTEGER)')
    conn.commit() 

def insert_gamelog(cur,conn):
    count = 0 
    cur, conn = main_database.set_up_database('NBA_statistics.db')
    season_list = cur.execute('SELECT distinct season_id FROM regular_season_stats').fetchall()
    for x in season_list:
        split = x[0].split('-')
        if int(split[0]) >= 1980:
            try:
                count = count+1 
                gamelog_data = leaguegamelog.LeagueGameLog(direction='ASC',player_or_team_abbreviation='P',season=x,season_type_all_star='Playoffs',sorter='DATE').get_dict()['resultSets'][0]['rowSet']
                print(f"Processing gamelogs: {count} of {len(gamelog_data)} for season: {x[0]}")
                time.sleep(1.5)
                for game in gamelog_data:
                    cur.execute("INSERT INTO playoffs_gamelog (year, player_id, name, team_id, team, game_id, game_date, matchup, win_loss, minutes, fgm, fga, fg_pct , fg3m , fg3a , fg3_pct , ftm , fta , ft_pct , oreb , dreb , reb , ast , stl , blk , tov , pts , plus_minus) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (int(game[0][1:]), game[index['PLAYER_ID']],game[index['PLAYER_NAME']],game[index['TEAM_ID']],game[index['TEAM_NAME']],game[index['GAME_ID']],game[index['GAME_DATE']],game[index['MATCHUP']],game[index['WL']],game[index['MIN']],game[index['FGM']],game[index['FGA']],game[index['FG_PCT']],game[index['FG3M']],game[index['FG3A']],game[index['FG3_PCT']],game[index['FTM']],game[index['FTA']],game[index['FT_PCT']],game[index['OREB']],game[index['DREB']],game[index['REB']],game[index['AST']],game[index['STL']],game[index['BLK']],game[index['TOV']],game[index['PTS']],game[index['PLUS_MINUS']]))
                    conn.commit()
            except Exception as e:
                print(e)
                print("Errored on index: "+str(count))
                if isinstance(e, requests.exceptions.ReadTimeout):
                    print("Sleeping for 30 minutes")
                    time.sleep(1805)
    print("Completed")

if __name__ == '__main__':
    cur, conn = main_database.set_up_database('NBA_statistics.db')
    create_gamelog_table(cur,conn)
    insert_gamelog(cur,conn)

    
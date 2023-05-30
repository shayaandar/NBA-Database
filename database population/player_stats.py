#getting data from the nba api

import requests
import sqlite3
import random
import time
import os
import json
import nba_api 
import pandas as pd 
import main_database
from nba_api.stats.endpoints import commonplayerinfo, playercareerstats, commonteamroster, teamplayerdashboard
from nba_api.stats.static.players import get_players
from nba_api.stats.static.teams import teams 

stat_names = ["PLAYER_ID", "SEASON_ID", "LEAGUE_ID", "TEAM_ID", "TEAM", "PLAYER_AGE", "GP", "GS", "MIN", "FGM","FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT", "FTM", "FTA", "FT_PCT", "OREB", "DREB", "REB", "AST", "STL", "BLK", "TOV", "PF", "PTS"]
index = {stat_names[i]: i for i in range(len(stat_names))}

def insert_regular_season_stats(cur, conn):
    # Count 
    count = 0 
    # Get player list 
    players = cur.execute('SELECT player_id FROM players ORDER BY player_id').fetchall()
    # Retry if API call fails 
    for x in players:
        count=count+1 
        try:
            # Get career stats 
            stat_req = playercareerstats.PlayerCareerStats(player_id=x[0], per_mode36='PerGame')
            reg_stats = stat_req.season_totals_regular_season.get_dict()['data']
            for season in reg_stats:
                cur.execute('INSERT INTO regular_season_stats (player_id, season_id, league_id, team, minutes, age, gamesplayed, points, rebounds, assists, field_goal_attempts, field_goal_percentage, three_attempts, three_percentage, freethrow_attempts, freethrow_percentage, steals, blocks, turnovers) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (season[index['PLAYER_ID']], season[index['SEASON_ID']], season[index['LEAGUE_ID']], season[index['TEAM']], season[index['MIN']], season[index['PLAYER_AGE']], season[index['GP']], season[index['PTS']], season[index['REB']], season[index['AST']], season[index['FGA']], season[index['FG_PCT']], season[index['FG3A']],season[index['FG3_PCT']], season[index['FTA']], season[index['FT_PCT']],season[index['STL']], season[index['BLK']],season[index['TOV']]))
                print(f"Regular season stats: Processing player id: {x[0]} in season: {season[index['SEASON_ID']]} number {count} of {len(players)}")
                conn.commit()
                time.sleep(random.randint(0,2))
        except Exception as e:
            print(e)
            print("Errored on index: "+str(count))
            if isinstance(e, requests.exceptions.ReadTimeout):
                print("Sleeping for 30 minutes")
                time.sleep(1805)
    print("Completed")

def insert_post_season_stats(cur, conn):
    # Count 
    count = 0 
    # Get player list 
    players = cur.execute('SELECT player_id FROM players ORDER BY player_id').fetchall()
    # Retry if API call fails 
    for x in players:
        count=count+1 
        try:
            # Get career stats 
            stat_req = playercareerstats.PlayerCareerStats(player_id=x[0], per_mode36='PerGame')
            post_stats = stat_req.season_totals_post_season.get_dict()['data']
            for season in post_stats:
                cur.execute('INSERT INTO post_season_stats (player_id, season_id, league_id, team, minutes, age, gamesplayed, points, rebounds, assists, field_goal_attempts, field_goal_percentage, three_attempts, three_percentage, freethrow_attempts, freethrow_percentage, steals, blocks, turnovers) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (season[index['PLAYER_ID']], season[index['SEASON_ID']], season[index['LEAGUE_ID']], season[index['TEAM']], season[index['MIN']], season[index['PLAYER_AGE']], season[index['GP']], season[index['PTS']], season[index['REB']], season[index['AST']], season[index['FGA']], season[index['FG_PCT']], season[index['FG3A']],season[index['FG3_PCT']], season[index['FTA']], season[index['FT_PCT']],season[index['STL']], season[index['BLK']],season[index['TOV']]))
                print(f"Post season stats: Processing player id: {x[0]} in season: {season[index['SEASON_ID']]} number {count} of {len(players)}")
                conn.commit()
                time.sleep(random.randint(0,2))
        except Exception as e:
            print(e)
            print("Errored on index: "+str(count))
            if isinstance(e, requests.exceptions.ReadTimeout):
                print("Sleeping for 30 minutes")
                time.sleep(1805)
    print("Completed")

def reconcile_missing_regstats(cur,conn):
    # Compare master list of player and ids to list of unique IDs in stats table to get missing ids 
    master_id_list = cur.execute('SELECT player_id FROM players').fetchall()
    regstat_id_list = cur.execute('SELECT distinct player_id FROM regular_season_stats').fetchall()
    missing_reg_ids = list(set(master_id_list)-set(regstat_id_list))
    print("Processing # of ids: "+ str(len(missing_reg_ids)))

    for x in missing_reg_ids: 
        stat_req = playercareerstats.PlayerCareerStats(player_id=x[0], per_mode36='PerGame')
        reg_stats = stat_req.season_totals_regular_season.get_dict()['data']
        if reg_stats ==[]:
            continue 
        for season in reg_stats:
            cur.execute('INSERT INTO regular_season_stats (player_id, season_id, league_id, team, minutes, age, gamesplayed, points, rebounds, assists, field_goal_attempts, field_goal_percentage, three_attempts, three_percentage, freethrow_attempts, freethrow_percentage, steals, blocks, turnovers) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (season[index['PLAYER_ID']], season[index['SEASON_ID']], season[index['LEAGUE_ID']], season[index['TEAM']], season[index['MIN']], season[index['PLAYER_AGE']], season[index['GP']], season[index['PTS']], season[index['REB']], season[index['AST']], season[index['FGA']], season[index['FG_PCT']], season[index['FG3A']],season[index['FG3_PCT']], season[index['FTA']], season[index['FT_PCT']],season[index['STL']], season[index['BLK']],season[index['TOV']]))
            print(f"Regular season stats: Processing player id: {x[0]} in season: {season[index['SEASON_ID']]}")
            conn.commit()
            time.sleep(random.randint(0,2))
    print("Completed reg stat reconcile")

def reconcile_missing_poststats(cur,conn):
    master_id_list = cur.execute('SELECT player_id FROM players').fetchall()
    poststat_id_list = cur.execute('SELECT distinct player_id FROM post_season_stats').fetchall()
    missing_post_ids = list(set(master_id_list)-set(poststat_id_list))
    print("Processing # of ids: "+str(len(missing_post_ids)))

    for x in missing_post_ids:
        stat_req = playercareerstats.PlayerCareerStats(player_id=x[0], per_mode36='PerGame')
        post_stats = stat_req.season_totals_post_season.get_dict()['data']
        if post_stats ==[]:
            continue 
        for season in post_stats:
            cur.execute('INSERT INTO post_season_stats (player_id, season_id, league_id, team, minutes, age, gamesplayed, points, rebounds, assists, field_goal_attempts, field_goal_percentage, three_attempts, three_percentage, freethrow_attempts, freethrow_percentage, steals, blocks, turnovers) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (season[index['PLAYER_ID']], season[index['SEASON_ID']], season[index['LEAGUE_ID']], season[index['TEAM']], season[index['MIN']], season[index['PLAYER_AGE']], season[index['GP']], season[index['PTS']], season[index['REB']], season[index['AST']], season[index['FGA']], season[index['FG_PCT']], season[index['FG3A']],season[index['FG3_PCT']], season[index['FTA']], season[index['FT_PCT']],season[index['STL']], season[index['BLK']],season[index['TOV']]))
            print(f"Post season stats: Processing player id: {x[0]} in season: {season[index['SEASON_ID']]}")
            conn.commit()
            time.sleep(random.randint(0,2))
    print("Completed post stats reconcilation")

def reconcile(cur,conn):
    reconcile_missing_regstats(cur,conn)
    reconcile_missing_poststats(cur,conn)

if __name__ == '__main__':
     cur, conn = main_database.set_up_database('NBA_statistics.db')
     reconcile(cur,conn)



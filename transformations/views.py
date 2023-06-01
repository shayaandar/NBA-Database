import sys 
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


# Creating views 
#cur, conn = main_database.set_up_database('NBA_statistics.db')
#cur.execute("CREATE VIEW IF NOT EXISTS vw_regularseasonstats AS SELECT players.name,regular_season_stats.* FROM regular_season_stats LEFT JOIN players on players.player_id=regular_season_stats.player_id")
#cur.execute("CREATE VIEW IF NOT EXISTS vw_playoffstats AS SELECT players.name,post_season_stats.* FROM post_season_stats LEFT JOIN players on players.player_id=post_season_stats.player_id")
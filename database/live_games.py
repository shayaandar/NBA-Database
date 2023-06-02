# Query nba.live.endpoints.scoreboard and  list games in localTimeZone
from datetime import datetime, timezone
from dateutil import parser
from nba_api.live.nba.endpoints import scoreboard
from nba_api.live.nba.endpoints import boxscore
import main_database
import sqlite3

def get_current_games():
    f = "{gameId}: {awayTeam} vs. {homeTeam} @ {gameTimeLTZ}" 

    board = scoreboard.ScoreBoard()
    print("ScoreBoardDate: " + board.score_board_date)
    games = board.games.get_dict()
    for game in games:
        gameTimeLTZ = parser.parse(game["gameTimeUTC"]).replace(tzinfo=timezone.utc).astimezone(tz=None)
        print(f.format(gameId=game['gameId'], awayTeam=game['awayTeam']['teamName'], homeTeam=game['homeTeam']['teamName'], gameTimeLTZ=gameTimeLTZ))
    return [game['gameId']]

def create_live_table(cur,conn):
    cur.execute('DROP TABLE IF EXISTS live_data') #only if resetting the tables
    cur.execute('CREATE TABLE IF NOT EXISTS live_data (name TEXT, team TEXT, home_away TEXT, pts INTEGER, reb INTEGER, assists INTEGER, stl INTEGER, tov INTEGER, blocks INTEGER, fga INTEGER, fgm INTEGER, fg_pct FLOAT, fg3_attempt INTEGER, fg3_made INTEGER, fg3_pct FLOAT, fta INTEGER, ftm INTEGER, ft_pct FLOAT,blocks_received INTEGER, offensivefoul INTEGER, fouldrawn TEXT, foulpersonal INTEGER, foultech INTEGER, points_fastbreak INTEGER, points_paint INTEGER, secondchance_pts INTEGER, oreb INTEGER, dreb INTEGER)')
    conn.commit() 

def insert_live_data(cur,conn):
    for x in get_current_games():
        box = boxscore.BoxScore(x).get_dict()
        hteam=box['game']['homeTeam']['teamTricode']
        ateam= team=box['game']['awayTeam']['teamTricode']
        for x in box['game']['homeTeam']['players']:
                print("Processing home team stats")
                if x['played']=='1':
                    cur.execute("INSERT INTO live_data (name, team, home_away, pts, reb, assists, stl,tov, blocks, fga, fgm, fg_pct, fg3_attempt, fg3_made, fg3_pct, fta, ftm, ft_pct,blocks_received, offensivefoul, fouldrawn, foulpersonal, foultech, points_fastbreak, points_paint, secondchance_pts, oreb , dreb) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(x['name'],hteam,"Home",x['statistics']['points'],x['statistics']['reboundsTotal'],x['statistics']['assists'],x['statistics']['steals'],x['statistics']['turnovers'],x['statistics']['blocks'],x['statistics']['fieldGoalsAttempted'],x['statistics']['fieldGoalsMade'],x['statistics']['fieldGoalsPercentage'],x['statistics']['threePointersAttempted'],x['statistics']['threePointersMade'],x['statistics']['threePointersPercentage'],x['statistics']['freeThrowsAttempted'],x['statistics']['freeThrowsMade'],x['statistics']['freeThrowsPercentage'],x['statistics']['blocksReceived'],x['statistics']['foulsOffensive'],x['statistics']['foulsDrawn'],x['statistics']['foulsPersonal'],x['statistics']['foulsTechnical'],x['statistics']['pointsFastBreak'],x['statistics']['pointsInThePaint'],x['statistics']['pointsSecondChance'],x['statistics']['reboundsOffensive'],x['statistics']['reboundsDefensive']))
                    conn.commit()
        for x in box['game']['awayTeam']['players']:
                print("Process away stats")
                if x['played']=='1':
                    cur.execute("INSERT INTO live_data (name, team, home_away, pts, reb, assists, stl,tov, blocks, fga, fgm, fg_pct, fg3_attempt, fg3_made, fg3_pct, fta, ftm, ft_pct,blocks_received, offensivefoul, fouldrawn, foulpersonal, foultech, points_fastbreak, points_paint, secondchance_pts, oreb , dreb) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(x['name'],hteam,"Home",x['statistics']['points'],x['statistics']['reboundsTotal'],x['statistics']['assists'],x['statistics']['steals'],x['statistics']['turnovers'],x['statistics']['blocks'],x['statistics']['fieldGoalsAttempted'],x['statistics']['fieldGoalsMade'],x['statistics']['fieldGoalsPercentage'],x['statistics']['threePointersAttempted'],x['statistics']['threePointersMade'],x['statistics']['threePointersPercentage'],x['statistics']['freeThrowsAttempted'],x['statistics']['freeThrowsMade'],x['statistics']['freeThrowsPercentage'],x['statistics']['blocksReceived'],x['statistics']['foulsOffensive'],x['statistics']['foulsDrawn'],x['statistics']['foulsPersonal'],x['statistics']['foulsTechnical'],x['statistics']['pointsFastBreak'],x['statistics']['pointsInThePaint'],x['statistics']['pointsSecondChance'],x['statistics']['reboundsOffensive'],x['statistics']['reboundsDefensive']))
                    conn.commit()

if __name__ == '__main__':
    cur, conn = main_database.set_up_database('NBA_statistics.db')
    create_live_table(cur,conn)
    insert_live_data(cur,conn)
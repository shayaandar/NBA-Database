# NBA Stats Database
This repository contains Python code for creating and managing a database that stores NBA (National Basketball Association) statistics. The database is populated by utilizing the NBA API to fetch and store relevant data into an SQLite database. It populates the database by utilizing a nba_api client package that can be found in this repository: https://github.com/swar/nba_api 

# Features
SQLite Database: The code includes functionality to create and manage an SQLite database, which serves as a centralized storage system for NBA statistics.

NBA API Integration: The NBA API is utilized to retrieve data such as player information, team statistics, game results, and more. The retrieved data is then processed and stored in the SQLite database.

Data Population: The code includes scripts to populate the database with NBA statistics. These scripts fetch data from the NBA API and insert it into the appropriate tables in the database.

Data Querying: The database allows for efficient querying of NBA statistics. Various SQL queries can be executed to retrieve specific data, such as player stats, team rankings, game results, and more.

Transformations: There are example transformations inside the transformations folder showing some of the ways this data can be used.

## Additions

The following additions are planned for future updates to this NBA Stats Database repository:

1. Iterative Update: In the future, I plan to enhance the database functionality to support iterative updates of new statistics. This would involve implementing a mechanism to periodically fetch and update the database with the latest player stats, team data, and other relevant NBA statistics. By automating this process, users can ensure that the database remains up-to-date with the latest information.

2. Live Game Data Integration: We aim to integrate live game data into the database, allowing users to access real-time NBA statistics during ongoing games. This would involve connecting to live data feeds or APIs to fetch and store live game updates, including player performances, scores, and other game-related information. With this addition, users can have access to the most current NBA statistics as games unfold.

If you have any suggestions or ideas for additional features, feel free to share. 

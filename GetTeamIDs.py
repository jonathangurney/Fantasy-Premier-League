# -*- coding: utf-8 -*-
"""
Created on Sat Oct 15 23:39:53 2022

@author: jonat
"""

project_dir = "C://Users/jonat/OneDrive/Documents/Projects/Fantasy Premier League"

import os
import datetime as dt
import pandas as pd

os.chdir(project_dir)

from GeneralFunctions import increment_season

# The team names map is used to match the names of the teams to those used in
# the FBRef data.
# 
# Maybe try to find some way of automating this?
team_names_map = {"Cardiff" : "Cardiff City",
                  "Hull" : "Hull City",
                  "Leeds" : "Leeds United",
                  "Leicester" : "Leicester City",
                  "Man City" : "Manchester City",
                  "Man Utd" : "Manchester Utd",
                  "Newcastle" : "Newcastle Utd",
                  "Norwich" : "Norwich City",
                  "Nott'm Forest" : "Nott'ham Forest",
                  "Spurs" : "Tottenham",
                  "Stoke" : "Stoke City",
                  "Swansea" : "Swansea City"}

def get_team_ids(write=True, write_dir=None):
    # Check that a directory is specified fi write==True
    if write:
        if not os.path.isdir(write_dir):
            raise ValueError("Write directory does not exist.")
    
    # Get team master list from vaastav Github
    team_id_url = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/master_team_list.csv"
    team_id_data = pd.read_csv(team_id_url)
    
    # Use team names map to convert to
    team_id_data["team_name"] = [team_names_map[name] if name in team_names_map else name for name in team_id_data["team_name"]]
    
    team_id_filename = write_dir + "/team_id_list.csv"
    if write:
        team_id_data.to_csv(team_id_filename, index=False)
    else:
        return(team_id_data)

# A helper function to update the team_id_list.csv file.
def update_team_ids(team_ids_dir):
    # Read the csv file
    team_id_filename = team_ids_dir + "/team_id_list.csv"
    team_id_data = pd.read_csv(team_id_filename)
    
    # Determine the last season from which team ids are recorded
    latest_season = team_id_data.iloc[-1, 0]
    
    # Set current season
    current_year = dt.datetime.now().year
    current_season = str(current_year - 1) + "-" + str(current_year)[2:4]
    next_season = increment_season(current_season, 1)
    
    # Loop over seasons upto the current season
    season = increment_season(latest_season, 1)
    while season != next_season:
        # Retrieve team ids for season from vaastav Github
        season_teams_url = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/{}/teams.csv".format(season)
        season_teams_data = pd.read_csv(season_teams_url)
        season_team_ids = season_teams_data[["id", "name"]]
        
        # Rename columns in line with team_id DataFrame
        season_team_ids.columns = ["team", "team_name"]
        
        # Map team names using team_names_map
        season_team_ids["team_name"] = [team_names_map[name] if name in team_names_map else name for name in season_team_ids["team_name"]]
        
        # Add season column
        season_team_ids["season"] = season
        
        # Join the two DataFrames
        team_id_data = pd.concat([team_id_data, season_team_ids], ignore_index=True)
        
        # Move to next season
        season = increment_season(season, 1)
    
    # Write updated DataFrame to file
    team_id_data.to_csv(team_id_filename, index=False)
    
team_ids_dir = "C://Users/jonat/OneDrive/Documents/Projects/Fantasy Premier League/Data"

update_team_ids(team_ids_dir)
        
        
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 19 18:18:16 2022

@author: jonat
"""

###############################################################################
#
# Create a main data file for the 2016-17 season to perform EDA with
#
###############################################################################

import datetime as dt
import numpy as np
import os
import pandas as pd
import pytz

# Set working directory
os.chdir('C://users/jonat/OneDrive/Documents/Projects/Fantasy Premier League')

# Set directories for reading and saving data
data_dir = './Data'
results_data_dir = './Data/results_data'
league_tables_dir = './Data/historical_league_tables'

# Set season
season = '2016-17'
season_start = season[2:4]
season_end = season[5:7]

#%%

# 
# Import the various data sources that we will be using throughout the modelling
# process
# 

#%%

# Import the individual FPL data from merged_seasons_data.csv
fpl_data_filename = data_dir + '/20{0}_{1}/20{0}_{1}_main_data.csv'.format(season_start, season_end)
fpl_data = pd.read_csv(fpl_data_filename)

#%%

# Import FiveThirtyEight's SPI data
spi_data_filename = data_dir + '/pl_spi_data.csv'
spi_data = pd.read_csv(spi_data_filename, index_col=0)

#%%

# Import historical league table data
hist_table_data_filename = (league_tables_dir +
                            '/premier_league_{0}_{1}_hist_league_tables.csv'.format(season_start, season_end))

#%%

#
# Add the SPI data
#

spi_stats = ['spi', 'prob', 'proj_score', 'importance', 'score', 'xg', 'nsxg', 'adj_score']
team_colnames = ['team_' + statname for statname in spi_stats]
opp_colnames = ['opp_' + statname for statname in spi_stats]

# Define a function that takes a team and kickoff date and returns the relevant
# SPI data

def get_spi_data(team, date):
    spi_series = spi_data.loc[(spi_data['date'] == date) & ((spi_data['team1'] == team) | (spi_data['team2'] == team))]
    
    if all(spi_series['team1'] == team):
        team_pos = '1'
        opp_pos = '2'
    else:
        team_pos = '2'
        opp_pos = '1'
    
    team_spi_data = dict()
    team_indices = [statname + team_pos for statname in spi_stats]
    opp_indices = [statname + opp_pos for statname in spi_stats]
    
    team_spi_data.update(zip(team_colnames, spi_series[team_indices].values.tolist()[0]))
    team_spi_data.update(zip(opp_colnames, spi_series[opp_indices].values.tolist()[0]))
    return(team_spi_data)

#%%

# Add SPI data to the fpl dataframe
teams = list(fpl_data.team_x.unique())
for team in teams:
    ko_datetimes = fpl_data.loc[fpl_data['team_x'] == team, 'kickoff_datetime'].unique().tolist()
    for ko_datetime in ko_datetimes:
        print('{0} : {1}'.format(team, ko_datetime))
        
        # Extract kickoff date from kickoff_datetime
        kickoff_date = ko_datetime[0:10]
        
        # Get SPI data using get_spi_data
        team_spi_data = get_spi_data(team, kickoff_date)
        
        # Add this to the data frame
        team_fixture_idx = (fpl_data['team_x'] == team) & (fpl_data['kickoff_datetime'] == ko_datetime)
        fpl_data.loc[team_fixture_idx, list(team_spi_data)] = list(team_spi_data.values())


#%%

# Add player position data
raw_data_url = 'https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/{0}/players_raw.csv'.format(season)
raw_data = pd.read_csv(raw_data_url)

# Create column for player name
raw_data['name'] = raw_data['first_name'] + ' ' + raw_data['web_name']

# Use 'element_type' to add position data
element_type_map = {1: 'GK', 2: 'DEF', 3: 'MID', 4: 'FWD'}
raw_data['position'] = [element_type_map[code] for code in raw_data['element_type']]

fpl_data = pd.merge(fpl_data, raw_data[['name', 'position']], how='left', on='name')

#%%

name_groupby = fpl_data.groupby('name')

# Set number of games to include in lagged statistics
prev_games = 4

# Declare the individual lagged and aggregate statistics to be included from
# the fpl_data dataframe
lagged_stats = ['assists', 'attempted_passes', 'big_chances_created', 'big_chances_missed',
                'bonus', 'bps', 'clean_sheets', 'clearances_blocks_interceptions',
                'completed_passes', 'creativity', 'dribbles', 'errors_leading_to_goal',
                'errors_leading_to_goal_attempt', 'fouls', 'goals_conceded',
                'goals_scored', 'ict_index', 'influence', 'key_passes', 'minutes',
                'offside', 'open_play_crosses', 'opp_adj_score', 'opp_nsxg',
                'opp_score', 'opp_xg', 'own_goals', 'penalties_conceded',
                'penalties_missed', 'penalties_saved', 'recoveries', 'red_cards',
                'saves', 'selected', 'tackled', 'tackles', 'target_missed',
                'team_adj_score', 'team_nsxg', 'team_score', 'team_xg','threat',
                'total_points', 'transfers_in', 'transfers_out', 'value', 'winning_goals',
                'yellow_cards']

indiv_aggregate_stats = ['assists', 'attempted_passes', 'big_chances_created', 'big_chances_missed',
                       'bonus', 'bps', 'clean_sheets', 'clearances_blocks_interceptions',
                       'completed_passes', 'creativity', 'dribbles', 'errors_leading_to_goal',
                       'errors_leading_to_goal_attempt', 'fouls', 'goals_conceded',
                       'goals_scored', 'ict_index', 'influence', 'key_passes', 'minutes',
                       'offside', 'open_play_crosses', 'own_goals', 'penalties_conceded',
                       'penalties_missed', 'penalties_saved', 'recoveries', 'red_cards',
                       'saves', 'tackled', 'tackles', 'target_missed', 'threat',
                       'total_points', 'winning_goals', 'yellow_cards']

team_aggregate_stats = ['team_adj_score', 'team_nsxg', 'team_score', 'team_xg',
                        'opp_adj_score', 'opp_nsxg', 'opp_score', 'opp_xg']

#%%

data_main = pd.DataFrame()
counter = 0
for key, data in name_groupby:
    if counter % 50 == 0:
        print('{0}/{1}'.format(counter, len(name_groupby)))
    counter += 1
    
    # Skip players without sufficient data
    if len(data) <= prev_games:
        next
    
    # Sort in chronological order
    data.sort_values(by='kickoff_datetime', inplace=True)
    
    # Get individual lagged data
    for i in range(1, prev_games + 1):
        lagged_col_names = [colname + str(i) for colname in lagged_stats]
        
        lagged_data = data.shift(i)[lagged_stats]
        lagged_data.columns = lagged_col_names
        
        if i == 1:
            new_data = pd.concat([data, lagged_data], axis=1)
        else:
            new_data = pd.concat([new_data, lagged_data], axis=1)
            
    # Create 'appearances' column in order to produce aggregate statistics
    data['made_appearance'] = data['minutes'] > 0
    data['appearances'] = np.cumsum(data['made_appearance'])
    
    # Create individual aggregate statistics
    for agg_stat in indiv_aggregate_stats:
        aggregate_col_name = agg_stat + '_per_app'
        new_data[aggregate_col_name] = [stat/apps if apps != 0 else 0 
                                    for stat, apps in zip(np.cumsum(data[agg_stat]), data['appearances'])]
    
    for agg_stat in team_aggregate_stats:
        aggregate_col_name = agg_stat + '_per_game'
        new_data[aggregate_col_name] = [stat/number for stat, number in zip(np.cumsum(data[agg_stat]), list(range(1, 39)))]
    
    # Append the data to the main dataframe
    data_main = pd.concat([data_main, new_data.iloc[prev_games:, ]])

#%%

#
# Clean up data ready for EDA
#

# Make 'was_home' numeric
data_main['was_home'] = [int(value) for value in data_main['was_home']]

#%%

# Write data to file
data_main_filename = (data_dir 
                     + '/20{0}_{1}/{0}_{1}_eda_data.csv'.format(season_start, season_end))
data_main.to_csv(data_main_filename, index=False)
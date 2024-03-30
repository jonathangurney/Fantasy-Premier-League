# -*- coding: utf-8 -*-
"""
Created on Thu Aug 10 16:14:07 2023

@author: jonat
"""

from AddData import add_player_understat_data, add_team_understat_data, add_opp_understat_data
from collections import Counter
import datetime as dt
from GetClubELOData import add_elo
from GetFPLData import load_fpl_data
import GetUnderstatData as getus
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import pytz
from scipy import stats
import seaborn as sn
import sklearn
import statsmodels.api as sm
import statsmodels.tools as st

#%%

data_dir = "C:/Users/jonat/OneDrive/Documents/Projects/Fantasy Premier League/Data"
seasons = ["{}-{}".format(y, str(y + 1)[2:4]) for y in range(2021, 2023)]

# Find the columns in the FPL data that exist for all seasons of the data
initial = True
for season in seasons:
    fpl_data = load_fpl_data(season)
    
    if initial:
        columns = fpl_data.columns
    
    columns = columns.intersection(fpl_data.columns)
    
    initial = False

# Create set of FPL data
print("\nLoading FPL data")
train_df = pd.DataFrame()
for season in seasons:
    fpl_df = load_fpl_data(season)
    train_df = pd.concat([train_df, fpl_df[columns]], ignore_index=True)

# Only select data for goalkeepers
train_df = train_df.loc[train_df['element_type'] == 1, :]

#%%

# Add Understat data to the dataset
print("\nAdding player Understat data")
train_df = add_player_understat_data(train_df)

#%%

# Add team Understat data to the dataframe
print("\nAdding team Understat data")
train_df = add_team_understat_data(train_df)

# Create a dataframe showing entries with NA values that have played more than
# 0 minutes
# na_df = train_df.loc[((train_df.isna().any(axis=1)) & (train_df.minutes > 0))]
# print(na_df[['season_x', 'name', 'element']].value_counts())

#%%

# print("\nAdding opposition Understat data")
# train_df = add_opp_understat_data(train_df)

#%%

# Add team and opposition Elo scores to the dataframe
print("\nAdding ClubELO scores")
train_df = add_elo(train_df)

#%%

# Make "was_home" numeric
train_df['was_home'] = train_df.was_home.astype("int")

#%%

# Get player and team lagged statistics
player_lag_stats = ['assists', 'bonus', 'bps', 'clean_sheets', 'creativity', 
                    'goals_conceded', 'goals_scored', 'ict_index', 'influence', 
                    'minutes', 'own_goals', 'penalties_missed', 'penalties_saved',
                    'red_cards', 'saves', 'selected', 'team_a_score', 
                    'team_h_score', 'threat', 'total_points', 'transfers_in', 
                    'transfers_out', 'value', 'was_home', 'yellow_cards', 
                    'starts', 'shots', 'xG', 'xA', 'key_passes', 'npg', 'npxG', 
                    'xGChain', 'xGBuildup']
team_lag_stats = ['team_xG', 'team_xGA', 'team_npxG', 'team_npxGA', 'team_deep', 
                  'team_deep_allowed', 'team_scored', 'team_missed', 'team_xpts', 
                  'team_wins', 'team_draws', 'team_loses', 'team_ppda_att', 
                  'team_ppda_def', 'team_ppda_allowed_att', 'team_ppda_allowed_def']

lag_stats = player_lag_stats + team_lag_stats

max_lag = 4
lags = range(1, max_lag+1)

#%%

for lag in lags:
    lag_colnames = [colname + "_{}".format(lag) for colname in lag_stats]
    
    train_df[lag_colnames] = train_df.sort_values(by='kickoff_date').groupby(
        ['season_x', 'element'])[lag_stats].shift(lag)
    
#%%

# Add opposition lagged statistics
opp_df = train_df[['kickoff_date', 'opp_team_name']].drop_duplicates()
opp_keys = zip(opp_df.kickoff_date, opp_df.opp_team_name)

team_lagged_colnames = [stat + "_{}".format(lag) for lag in lags for stat in team_lag_stats]
opp_lagged_colnames = [colname.replace("team", "opp") for colname in team_lagged_colnames]

for date, opp_name in opp_keys:
    opp_lagged_stats = train_df.loc[((train_df.kickoff_date == date) & 
                                        (train_df.team_x == opp_name)), 
                                       team_lagged_colnames].drop_duplicates()
    
    # Select row with fewest null values
    min_na_idx = opp_lagged_stats.isna().astype(int).sum(axis=1).idxmin()
    
    train_df.loc[((train_df.kickoff_date == date) & (train_df.opp_team_name == opp_name)), 
                 opp_lagged_colnames] = opp_lagged_stats.loc[min_na_idx].tolist()

#%%

# Drop columns that will not be used in the models
drop_cols = ['element', 'fixture', 'kickoff_time', 'opponent_team', 'round',
              'transfers_balance', 'name', 'season_x', 'opp_team_name', 
              'kickoff_datetime', 'kickoff_date', 'team_x', 'element_type', 
              'position', 'team_npxGD', 'team_result', 'team_pts']

keep_stats = ['total_points', 'selected', 'transfers_in', 'transfers_out', 'value',
              'was_home']
drop_stats = list(set(lag_stats) - set(keep_stats))

train_df.drop((drop_cols + drop_stats), axis=1, inplace=True)

#%%

# Use statsmodels to perform linear regression



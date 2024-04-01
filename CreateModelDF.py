# -*- coding: utf-8 -*-
"""
Created on Sun Mar 31 11:15:21 2024

@author: jonat
"""

from AddData import add_player_understat_data, add_team_understat_data
from GetClubELOData import add_elo
from GetFPLData import load_fpl_data
import pandas as pd

seasons = ["{}-{}".format(y, str(y + 1)[2:4]) for y in range(2021, 2023)]
max_lag = 4
write_dir = "./Data"

# Find the columns in the FPL data that exist for all seasons of the data
initial = True
for season in seasons:
    fpl_data = load_fpl_data(season)
    
    if initial:
        columns = fpl_data.columns
    
    columns = columns.intersection(fpl_data.columns)
    initial = False

# Create set of FPL data
fpl_df = pd.DataFrame()
for season in seasons:
    fpl_df1 = load_fpl_data(season)
    fpl_df = pd.concat([fpl_df, fpl_df1[columns]], ignore_index=True)    

# Add Understat data to the dataset
fpl_df = add_player_understat_data(fpl_df)

# Add team Understat data to the dataframe
fpl_df = add_team_understat_data(fpl_df)

# Add team and opposition Elo scores to the dataframe
fpl_df = add_elo(fpl_df)

# Make "was_home" numeric
fpl_df['was_home'] = fpl_df.was_home.astype("int")

# Add zeros instead of NaNs in player Understat statistics where the player played zero minutes
player_us_stats = ["shots", "xG", "position", "xA", "key_passes", "npg", "npxG", "xGChain", "xGBuildup"]
fpl_df.loc[fpl_df.minutes == 0, player_us_stats] = 0

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

lags = range(1, max_lag+1)

for lag in lags:
    lag_colnames = [colname + "_{}".format(lag) for colname in lag_stats]
    
    fpl_df[lag_colnames] = fpl_df.sort_values(by='kickoff_date').groupby(
        ['season_x', 'element'])[lag_stats].shift(lag)
    

# Add opposition lagged statistics
opp_df = fpl_df[['kickoff_date', 'opp_team_name']].drop_duplicates()
opp_keys = zip(opp_df.kickoff_date, opp_df.opp_team_name)

team_lagged_colnames = [stat + "_{}".format(lag) for lag in lags for stat in team_lag_stats]
opp_lagged_colnames = [colname.replace("team", "opp") for colname in team_lagged_colnames]

for date, opp_name in opp_keys:
    opp_lagged_stats = fpl_df.loc[((fpl_df.kickoff_date == date) & 
                                        (fpl_df.team_x == opp_name)), 
                                       team_lagged_colnames].drop_duplicates()
    
    # Select row with fewest null values
    min_na_idx = opp_lagged_stats.isna().astype(int).sum(axis=1).idxmin()
    
    fpl_df.loc[((fpl_df.kickoff_date == date) & (fpl_df.opp_team_name == opp_name)), 
                 opp_lagged_colnames] = opp_lagged_stats.loc[min_na_idx].tolist()
    
fpl_df_filename = (write_dir + "/{}_{}_model_data.csv".format(seasons[0], seasons[-1])
                   .replace("-", "_"))
fpl_df.to_csv(fpl_df_filename, index=False)

# -*- coding: utf-8 -*-
"""
Created on Sun Sep  3 11:06:50 2023

@author: jonat
"""

import GetUnderstatData as getus
import numpy as np
import pandas as pd

def add_player_understat_data(fpl_df):
    initial = True
    
    for season in fpl_df.season_x.unique().tolist():
        print(season)
        
        if season == "2016-17":
            err_fixtures_filename = ("C:/Users/jonat/OneDrive/Documents/" + 
                                     "Projects/Fantasy Premier League/Data/" + 
                                     "2016_17/erroneous_fixtures.csv")
            err_fixtures = pd.read_csv(err_fixtures_filename)
        
        # Load player and team id dicts
        player_id_df = getus.load_player_id_df(season)
        team_id_df = getus.load_team_id_df(season)
            
        season_ids = fpl_df.loc[fpl_df['season_x'] == season, 'element'].unique().tolist()
        
        for fpl_id in season_ids:
            understat_id = float(player_id_df.loc[player_id_df['fpl_id'] == fpl_id, 
                                            'understat_id'].tolist()[0])
            
            if np.isnan(understat_id):
                continue
            
            player_data = getus.get_understat_match_stats(understat_id, season)
            
            # Remove any rows in player_data where a Premier League team is not
            # present
            player_data = player_data.loc[
                player_data.isin(team_id_df.understat_name.tolist()).any(axis=1)]
            
            player_data['fpl_id'] = fpl_id
            
            understat_stats = ['fpl_id', 'date', 'shots', 'xG', 'position', 'xA', 
                               'key_passes', 'npg', 'npxG', 'xGChain', 'xGBuildup']
            
            # Change the dates held by understat where necessary in the 2016-17
            # season.
            if season == "2016-17":
                in_err_fixtures = [fix_id in err_fixtures.us_fixture_id.tolist()
                                   for fix_id in player_data.id.tolist()]
                if any(in_err_fixtures):
                    player_data = pd.merge(player_data, 
                                           err_fixtures[['us_fixture_id', 'fpl_date']],
                                           how='left',
                                           left_on='id',
                                           right_on='us_fixture_id')
                    
                    player_data['date'] = player_data['fpl_date'].fillna(player_data.date)
                    print(player_data['date'])
                    print(player_data['fpl_date'])
                    player_data.drop(['us_fixture_id', 'fpl_date'], 
                                     axis=1, 
                                     inplace=True)
                    
            try:
                fpl_df = pd.merge(fpl_df,
                                  player_data[understat_stats],
                                  how='left',
                                  left_on=['element', 'kickoff_date'],
                                  right_on=['fpl_id', 'date'])
                
                if not initial:
                    for stat in understat_stats[2:]:
                        stat_x = stat + '_x'
                        stat_y = stat + '_y'                     
                        fpl_df[stat] = fpl_df[stat_x].fillna(fpl_df[stat_y])
                        fpl_df.drop([stat_x, stat_y], axis=1, inplace=True)
                
                fpl_df.drop(['fpl_id', 'date'], axis=1, inplace=True)
                
            except KeyError:
                fpl_df.drop(fpl_df.loc[((fpl_df.season_x == season) & 
                                       (fpl_df.element == fpl_id))].index,
                            inplace=True)
                
            initial = False
    
    return fpl_df

def add_team_understat_data(fpl_df):
    initial = True
    
    for season in fpl_df.season_x.unique().tolist():
        print(season)
        
        # Code to deal with the incorrect dates held by understat in the
        # 2016-17 season
        if season == "2016-17":
            err_fixtures_filename = ("C:/Users/jonat/OneDrive/Documents/" + 
                                     "Projects/Fantasy Premier League/Data/" + 
                                     "2016_17/erroneous_fixtures.csv")
            err_fixtures = pd.read_csv(err_fixtures_filename)
            
            err_fixtures = pd.concat(
                [err_fixtures[['us_h_team_name', 'us_date', 'fpl_date']],
                 err_fixtures[['us_a_team_name', 'us_date', 'fpl_date']]],
                ignore_index=True)
            err_fixtures['us_name'] = err_fixtures.us_h_team_name.fillna(
                err_fixtures.us_a_team_name)
            err_fixtures.drop(['us_h_team_name', 'us_a_team_name'], 
                              axis=1, 
                              inplace=True)
            err_fixtures_list = [(team, date) for team, date in 
                                 zip(err_fixtures.us_name, err_fixtures.us_date)]
        
        team_id_df = getus.load_team_id_df(season)
        
        team_cols = ['xG', 'xGA', 'npxG', 'npxGA', 'deep', 'deep_allowed', 
                     'scored', 'missed', 'xpts', 'result', 'date', 'wins', 
                     'draws', 'loses', 'pts', 'npxGD', 'ppda_att', 'ppda_def',
                     'ppda_allowed_att', 'ppda_allowed_def']
        cols_new_names = ['team_' + x for x in team_cols]
        rename_dict = dict(zip(team_cols, cols_new_names))
        
        for us_team, fpl_team in zip(team_id_df.understat_name, team_id_df.fpl_name):
            team_data = getus.get_understat_team_data(us_team, season)
            team_data = team_data[team_cols]
            
            team_data['date'] = [date[0:10] for date in team_data.date]
            team_data['team'] = fpl_team
            
            if season == "2016-17":
                for date in team_data.date:
                    if (us_team, date) in err_fixtures_list:
                        fpl_date = err_fixtures.loc[err_fixtures.us_date == date, 'fpl_date']
                        team_data.loc[team_data.date == date, 'date'] = fpl_date
                    
            team_data.rename(rename_dict, axis=1, inplace=True)
            
            fpl_df = pd.merge(fpl_df, 
                              team_data,
                              how='left',
                              left_on=['team_x', 'kickoff_date'],
                              right_on=['team', 'team_date'])
            
            if not initial:
                cols_x = [colname + '_x' for colname in cols_new_names]
                cols_y = [colname + '_y' for colname in cols_new_names]
                for col, col_x, col_y in zip(cols_new_names, cols_x, cols_y):
                    fpl_df[col] = fpl_df[col_x].fillna(fpl_df[col_y])
                
                fpl_df.drop((cols_x + cols_y), axis=1, inplace=True)
                
            fpl_df.drop('team', axis=1, inplace=True)
            
            initial = False
            
    fpl_df.drop('team_date', axis=1, inplace=True)
    return fpl_df
    
def add_opp_understat_data(fpl_df):
    team_data_cols = ['h_a', 'xG', 'xGA', 'npxG', 'npxGA', 'deep', 
                      'deep_allowed', 'scored', 'missed', 'xpts', 'result', 
                      'date', 'wins', 'draws', 'loses', 'pts', 'npxGD', 
                      'ppda_att', 'ppda_def', 'ppda_allowed_att',
                      'ppda_allowed_def']
    
    numeric_cols = team_data_cols.copy()
    numeric_cols.remove('h_a')
    numeric_cols.remove('result')
    numeric_cols.remove('date')
    
    cum_avg_colnames = ['opp_' + colname + '_avg' for colname in numeric_cols]
    ema_colnames = ['opp_' + colname + '_ema' for colname in numeric_cols]
    
    lag_period = 1
    ema_alpha = 0.5
    
    lag_colnames_dict = dict()
    for x in range(1, lag_period + 1):
        colnames = [('opp_' + colname + '_' + str(x))
                    for colname in numeric_cols]
        lag_colnames_dict[x] = colnames
        
    for season in fpl_df.season_x.unique().tolist():
        print(season)
        
        # Code to deal with the incorrect dates held by understat in the
        # 2016-17 season
        if season == "2016-17":
            err_fixtures_filename = ("C:/Users/jonat/OneDrive/Documents/" + 
                                     "Projects/Fantasy Premier League/Data/" + 
                                     "2016_17/erroneous_fixtures.csv")
            err_fixtures = pd.read_csv(err_fixtures_filename)
            
            err_fixtures = pd.concat(
                [err_fixtures[['us_h_team_name', 'us_date', 'fpl_date']],
                 err_fixtures[['us_a_team_name', 'us_date', 'fpl_date']]],
                ignore_index=True)
            err_fixtures['us_name'] = err_fixtures.us_h_team_name.fillna(
                err_fixtures.us_a_team_name)
            err_fixtures.drop(['us_h_team_name', 'us_a_team_name'], 
                              axis=1, 
                              inplace=True)
        
        team_id_df = getus.load_team_id_df(season)
        
        initial = True
        for opp_fpl_name, opp_us_name in zip(team_id_df.fpl_name, 
                                             team_id_df.understat_name):
            opp_data = getus.get_understat_team_data(opp_us_name, season)
            
            # Compute the cumulative mean and the ema of the statistics
            opp_data[cum_avg_colnames] = opp_data[numeric_cols].expanding(0).mean()
            opp_data[ema_colnames] = opp_data[numeric_cols].ewm(alpha=ema_alpha).mean()
            
            # Add the lagged statistics up to the max lag given
            for lag in range(1, lag_period + 1):
                opp_data[lag_colnames_dict[lag]] = opp_data[numeric_cols].shift(lag)
            
            drop_columns = team_data_cols.copy()
            drop_columns.remove('date')
            opp_data.drop(drop_columns, axis=1, inplace=True)
            
            # Fix the dates if season is 2016-17
            if season == "2016-17":
                opp_data['us_name'] = opp_us_name
                opp_data = pd.merge(opp_data, err_fixtures,
                                    how='left',
                                    left_on=['us_name', 'date'],
                                    right_on=['us_name', 'us_date'])
                opp_data['date'] = opp_data['fpl_date'].fillna(opp_data.date)
                opp_data.drop(['us_name', 'us_date', 'fpl_date'], 
                              axis=1, 
                              inplace=True)
            
            opp_data['fpl_name'] = opp_fpl_name
            fpl_df = pd.merge(fpl_df, opp_data,
                              how='left',
                              left_on=['opp_team_name', 'kickoff_date'],
                              right_on=['fpl_name', 'date'])
            
            fpl_df.drop(['fpl_name', 'date'], axis=1, inplace=True)
            
            if not initial:
                lag_colnames = [item for key in lag_colnames_dict 
                                for item in lag_colnames_dict[key]]
                total_cols = cum_avg_colnames + ema_colnames + lag_colnames
                cols_x = [col + '_x' for col in total_cols]
                cols_y = [col + '_y' for col in total_cols]
                for col, col_x, col_y in zip(total_cols, cols_x, cols_y):
                    fpl_df[col] = fpl_df[col_x].fillna(fpl_df[col_y])
                
                fpl_df.drop((cols_x + cols_y), axis=1, inplace=True)
                
            initial = False
    return fpl_df
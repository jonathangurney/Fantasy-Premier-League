# -*- coding: utf-8 -*-
"""
Created on Thu Aug  3 17:36:55 2023

@author: jonat
"""

import aiohttp
import asyncio
import datetime as dt

import nest_asyncio
nest_asyncio.apply()

import os
import pandas as pd
from understat import Understat

# Load FPL data
def load_fpl_data(season, return_filename=False):
    season_us = season.replace('-', '_')
    data_dir = ("C:/Users/jonat/OneDrive/Documents/Projects/" + 
                "Fantasy Premier League/Data/{}".format(season_us))
    fpl_data_filename = data_dir + "/{}_fpl_data.csv".format(season_us)
    fpl_data = pd.read_csv(fpl_data_filename)
    
    if return_filename:
        return fpl_data, fpl_data_filename
    
    return fpl_data

# A script to access the understat API to download stats for players in the EPL
# in a given season.

# Use get_league_players to get the players in a league in the given season
async def get_understat_ids_main(season):
    async with aiohttp.ClientSession() as session:
        understat = Understat(session)
        year = season[0:4]
        players = await understat.get_league_players('epl', year)
        
        player_names = [player['player_name'] for player in players]
        player_ids = [player['id'] for player in players]
        
        players_df = pd.DataFrame({'name': player_names, 'id': player_ids})
        
        return players_df
    
def get_understat_ids(season):
    loop = asyncio.get_event_loop()
    player_ids = loop.run_until_complete(get_understat_ids_main(season))
    return player_ids

# Use get_player_matches to get the match stats of the player for this season
async def get_understat_match_stats_main(player_id, season):
    async with aiohttp.ClientSession() as session:
        understat = Understat(session)
        year = season[0:4]
        player_stats = await understat.get_player_matches(player_id, season=year)
        
        return(player_stats)
    
def get_understat_match_stats(player_id, season):
    player_id = int(player_id)
    loop = asyncio.get_event_loop()
    player_stats = loop.run_until_complete(
        get_understat_match_stats_main(player_id, season))
    player_stats = pd.DataFrame(player_stats)

    return player_stats

async def get_understat_team_main(player_id):
    async with aiohttp.ClientSession() as session:
        understat = Understat(session)
        grouped_stats = await understat.get_player_grouped_stats(player_id)

        return grouped_stats

###############################################################################
# 
# Future plans:
#    - Combine the small "get" understat into one function with options to
#      return certain statistics
# 
###############################################################################

def get_understat_team(player_id, season):
    player_id = int(player_id)
    loop = asyncio.get_event_loop()
    player_stats = loop.run_until_complete(get_understat_team_main(player_id))
    player_stats = player_stats['season']
    year = season[0:4]
    
    for dictx in player_stats:
        if dictx['season'] == year:
            return dictx['team']
        
    return

async def get_understat_apps_main(player_id):
    async with aiohttp.ClientSession() as session:
        understat = Understat(session)
        grouped_stats = await understat.get_player_grouped_stats(player_id)

        return grouped_stats

def get_understat_apps(player_id, season):
    player_id = int(player_id)
    loop = asyncio.get_event_loop()
    player_stats = loop.run_until_complete(get_understat_apps_main(player_id))
    player_stats = player_stats['season']
    year = season[0:4]
    
    for dictx in player_stats:
        if dictx['season'] == year:
            return dictx['games']
        
    return

async def get_understat_mins_main(player_id):
    async with aiohttp.ClientSession() as session:
        understat = Understat(session)
        grouped_stats = await understat.get_player_grouped_stats(player_id)

        return grouped_stats

def get_understat_mins(player_id, season):
    player_id = int(player_id)
    loop = asyncio.get_event_loop()
    player_stats = loop.run_until_complete(get_understat_mins_main(player_id))
    player_stats = player_stats['season']
    year = season[0:4]
    
    for dictx in player_stats:
        if dictx['season'] == year:
            return dictx['time']
        
    return

async def get_understat_season_data_main(player_id):
    async with aiohttp.ClientSession() as session:
        understat = Understat(session)
        grouped_stats = await understat.get_player_grouped_stats(player_id)

        return grouped_stats

def get_understat_season_data(player_id, season):
    player_id = int(player_id)
    loop = asyncio.get_event_loop()
    player_stats = loop.run_until_complete(get_understat_season_data_main(player_id))
    player_stats = player_stats['season']
    year = season[0:4]
    
    for dictx in player_stats:
        if dictx['season'] == year:
            return dictx
    
    return 

async def get_understat_team_data_main(team, season):
    async with aiohttp.ClientSession() as session:
        understat = Understat(session)
        year = season[0:4]
        team_stats = await understat.get_teams(league_name="EPL",  
                                               season=year,
                                               title=team)
        return team_stats

def get_understat_team_data(team, season):
    loop = asyncio.get_event_loop()
    team_stats = loop.run_until_complete(get_understat_team_data_main(team, season))
    
    for result in team_stats[0]['history']:
        result['ppda_att'] = result['ppda']['att']
        result['ppda_def'] = result['ppda']['def']
        result['ppda_allowed_att'] = result['ppda_allowed']['att']
        result['ppda_allowed_def'] = result['ppda_allowed']['def']
        
        del result['ppda']
        del result['ppda_allowed']
        
    team_stats = pd.DataFrame(team_stats[0]['history'])
    return team_stats

def get_understat_team_season_data(team, season):
    team_data = get_understat_team_data(team, season)
    
    n_games = len(team_data[0]['history'])
    team_season_data = {}
    numeric_keys = ['xG', 'xGA', 'npxG', 'npxGA', 'deep', 'deep_allowed', 
                    'scored', 'missed', 'xpts', 'wins', 'draws', 'loses', 
                    'pts', 'npxGD']
    
    for d in team_data[0]['history']:
        if len(team_season_data) == 0:
            for key in numeric_keys:
                team_season_data[key] = d[key]
        else:
            for key in numeric_keys:
                team_season_data[key] += d[key]
    
    team_season_data['games'] = n_games
    
    return team_season_data

def get_understat_team_game_data(team, date):    
    if isinstance(date, str):
        date = dt.datetime.strptime(date, "%Y-%m-%d").date()
        
    if date.month >= 8:
        season = str(date.year) + "-" + str(date.year + 1)[2:4]
    else:
        season = str(date.year - 1) + "-" + str(date.year)[2:4]
    
    team_data = get_understat_team_data(team, season)
    team_data = team_data[0]['history']
    
    for game_dict in team_data:
        if dt.datetime.strptime(game_dict['date'], "%Y-%m-%d %X").date() > date:
            raise ValueError('Game for team {} on date {} not found.'.format(
                team, date.strftime("%F")))
        
        if date.strftime("%F") in game_dict['date']:
            return game_dict
    
    return

async def get_understat_team_results_main(team, season):
    year = season[0:4]
    async with aiohttp.ClientSession() as session:
        understat = Understat(session)
        results = await understat.get_team_results(team, year)
    
        return results

def get_understat_team_results(team, season):
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(get_understat_team_results_main(team, season))
    
    return results
    
async def get_understat_league_results_main(season):
    year = season[0:4]
    async with aiohttp.ClientSession() as session:
        understat = Understat(session)
        fixtures = await understat.get_league_results("epl", year)

        return fixtures

def get_understat_league_results(season):
    loop = asyncio.get_event_loop()
    league_results = loop.run_until_complete(get_understat_league_results_main(season))
    
    return league_results

def get_fpl_idlist(season):
    idlist_file = ("https://raw.githubusercontent.com/vaastav/" + 
                   "Fantasy-Premier-League/master/data/" + 
                   "{}/player_idlist.csv".format(season))
    player_df = pd.read_csv(idlist_file)
    return player_df

def load_player_id_df(season):
    season_us = season.replace('-', '_')
    data_dir = ("C:/Users/jonat/OneDrive/Documents/Projects/" + 
                "Fantasy Premier League/Data/{}".format(season_us))
    id_df_filename = data_dir + "/{}_id_dict.csv".format(season_us)
    id_df = pd.read_csv(id_df_filename)
    
    return id_df

def create_team_id_dict(season):
    data_dir = "C:/Users/jonat/OneDrive/Documents/Projects/Fantasy Premier League/Data"
    
    fpl_team_id_filename = data_dir + "/team_id_list.csv"
    fpl_team_df = pd.read_csv(fpl_team_id_filename)
    fpl_team_df = fpl_team_df.loc[fpl_team_df['season'] == season, :]
    
    league_results = get_understat_league_results(season)
    
    team_id_df = pd.DataFrame()
    i = 0
    while len(team_id_df) < 20:
        fixture = league_results[i]
        h_df = pd.DataFrame(fixture['h'], index=range(1))
        a_df = pd.DataFrame(fixture['a'], index=range(1))
        team_id_df = pd.concat([team_id_df, h_df], ignore_index=True)
        team_id_df = pd.concat([team_id_df, a_df], ignore_index=True)
        
        team_id_df.drop_duplicates(inplace=True, ignore_index=True)
        
        i += 1
        
    team_id_df.rename({'id': 'understat_id', 'title': 'understat_name'},
                      axis=1,
                      inplace=True)
    
    team_id_df = pd.merge(team_id_df, fpl_team_df, 
                          how='left', 
                          left_on='understat_name',
                          right_on='team_name')
    
    team_id_df.drop('season', axis=1, inplace=True)
    
    spare_teams_index = [True if team not in team_id_df.team_name.tolist() 
                         else False for team in fpl_team_df.team_name.tolist()]
    
    spare_fpl_teams = fpl_team_df[spare_teams_index]
    
    for i in range(len(team_id_df)):
        if not team_id_df.loc[i].isnull().any():
            continue
        
        print(spare_fpl_teams)
        print('')
        print("{}: {}\n".format(team_id_df.loc[i, 'understat_id'], 
                              team_id_df.loc[i, 'understat_name']))
        fpl_element = input("FPL team element to match to current team: ")
        print('')
        
        missing_values = spare_fpl_teams.loc[spare_fpl_teams['team'] == int(fpl_element),
                                             ['team', 'team_name']]
        
        team_id_df.loc[i, ['team', 'team_name']] = missing_values.values[0].tolist()
        
    team_id_df.rename({'team': 'fpl_id', 'team_name': 'fpl_name'}, 
                      axis=1, 
                      inplace=True)
    
    team_id_df['fpl_id'] = [int(idx) for idx in team_id_df.fpl_id]
    
    season_us = season.replace("-", "_")
    team_id_dict_filename = (data_dir + "/" + season_us + 
                             "/{}_team_id_dict.csv".format(season_us))
    
    team_id_df.to_csv(team_id_dict_filename, index=False)
    
def load_team_id_df(season):
    season_us = season.replace("-", "_")
    data_dir = "C:/Users/jonat/OneDrive/Documents/Projects/Fantasy Premier League/Data"
    df_filename = data_dir + "/{0}/{0}_team_id_dict.csv".format(season_us)
    team_id_df = pd.read_csv(df_filename)
    
    return team_id_df
    
###############################################################################
#
# Future plans:
#   - Combine the functions match_ids and final_match ids.
#
#   - Let id_dicts read from id_dicts from the past to fill in missing records.
#
#   - When relying on user input for the understat_name, cross-reference this
#     with the understat name relating to the understat id provided.
#
###############################################################################

def match_ids(season, write_dir, write=False, return_dict=True):
    print("Matching ids for {}".format(season))
    
    # Load players and ids from FPL data
    fpl_idlist = get_fpl_idlist(season)
    fpl_idlist['name'] = [' '.join([f_name, s_name]) for f_name, s_name in 
                    zip(fpl_idlist['first_name'], fpl_idlist['second_name'])]
    fpl_players_df = fpl_idlist.rename(columns={'id':'fpl_id'})
    fpl_players_df['fpl_name'] = fpl_players_df.name
    
    # Get understat names and ids
    understat_players = get_understat_ids(season).rename(
        columns={'id':'understat_id'})
    understat_players['understat_name'] = understat_players.name
    
    # Check for dupicate names in the FPL data and create list of duplicate
    # names.
    duplicate_names = []
    is_duplicate_names = (len(fpl_players_df.name.unique()) != len(fpl_players_df.name))
    if is_duplicate_names:
        duplicate_names = fpl_players_df.loc[
            fpl_players_df.name.duplicated(), 'name'].unique()
        print("Duplicate names: {}".format(duplicate_names))
        
    duplicate = pd.Series([name in duplicate_names for 
                           name in fpl_players_df['name']])
    fpl_no_dups_df = fpl_players_df[~duplicate]
    matched_ids = pd.merge(fpl_no_dups_df, understat_players, how='left', on='name')        

    # Create id dictionary
    id_dict = pd.concat([matched_ids, 
                  fpl_players_df[duplicate]]).sort_index().drop('name', axis=1)
    
    # Find missing players that have played more than 0 minutes
    
    # Load FPL data
    fpl_df = load_fpl_data(season)
    player_minutes = fpl_df.groupby('name').sum().minutes
    player_minutes.index.rename('fpl_name', inplace=True)
    
    # Add minutes to id_dict
    id_dict = pd.merge(id_dict, player_minutes, how='left', on='fpl_name')
    
    unmatched_fpl_players = id_dict[((id_dict['understat_name'].isna()) & 
                                     (id_dict['minutes'] != 0))]
    
    missing_us_players = understat_players.loc[[name not in
                       id_dict['understat_name'].tolist() for name in 
                       understat_players['understat_name']], :]    
    
    cancel = False
    for name, us_id in zip(missing_us_players.understat_name, 
                           missing_us_players.understat_id):
        print("{}, {}".format(name, us_id))
        
        for f_name, s_name, fpl_id in zip(unmatched_fpl_players.first_name, 
                                          unmatched_fpl_players.second_name,
                                          unmatched_fpl_players.fpl_id):
            if ((f_name in name) | (s_name in name)):
                fpl_name = ' '.join([f_name, s_name])
                print("FPL name/id: {}/{}\n".format(fpl_name, fpl_id))
                
                if fpl_name in duplicate_names:
                    print("\n--------------------------------------")
                    print("Current player name found as duplicate")
                    print("--------------------------------------\n")
                    
                fpl_team = fpl_df.loc[fpl_df['element'] == fpl_id, 'team_x'].unique()
                us_team = get_understat_team(us_id, season)
                fpl_mins = fpl_df.loc[fpl_df['element'] == fpl_id, 'minutes'].sum()
                us_mins = get_understat_mins(us_id, season)
                fpl_apps = fpl_df.loc[((fpl_df['element'] == fpl_id) & 
                                       (fpl_df['minutes'] > 0)), 'minutes'].count()
                us_apps = get_understat_apps(us_id, season)
                
                print("FPL teams for {}, {}: {}".format(fpl_name, fpl_id, fpl_team))
                print("Understat team for {}, {}: {}".format(name, us_id, us_team))
                print("FPL apps/mins: {}/{}".format(fpl_apps, fpl_mins))
                print("Understat apps/mins: {}/{}".format(us_apps, us_mins))
                
                try_count = 0
                max_try = 3
                combine = False
                while ((try_count < max_try) & (not combine)):
                    combine_input = input("Combine these records? ")
                    if combine_input.lower() in ['y', 'yes']:
                        combine = True
                    if combine_input.lower() in ['n', 'no']:
                        try_count = max_try
                    if combine_input.lower() in ['cancel']:
                        cancel = True
                        try_count = max_try
                    try_count += 1
                        
                if combine:
                    id_dict.loc[id_dict['fpl_name'] == fpl_name, 
                                ['understat_name', 'understat_id']] = [name, us_id]
        
        if cancel:
            break
        
        print("\n-----------------------------------------------\n")
        
    if cancel:
        return
    
    # Drop unnecessary columns from id_dict
    id_dict = id_dict[['fpl_name', 'fpl_id', 'understat_name', 'understat_id']]
    
    # Write the id dictionary to file
    
    # Check that the path given in write_dir exists
    if write:
        if not os.path.isdir(write_dir):
            raise FileNotFoundError("The path {} is not a valid directory."
                                    .format(write_dir))
            
        season_us = season.replace("-", "_")
        id_dict_filename = write_dir + "/{}_id_dict.csv".format(season_us)
        id_dict.to_csv(id_dict_filename, index=False)
    
    if return_dict:
        return id_dict

def final_match_ids(season, write_dir, write=True, return_dict=False):
    print("Matching ids for {}".format(season))
    print("---------------------------------------\n")
    season_us = season.replace("-", "_")
    
    # Load the id_dict for this season
    id_dict_dir = ("C:/Users/jonat/OneDrive/Documents/Projects/" + 
                   "Fantasy Premier League/Data/{}".format(season_us))
    id_dict_filename = id_dict_dir + "/{}_id_dict.csv".format(season_us)
    id_dict = pd.read_csv(id_dict_filename)
    
    # Get understat names and ids
    understat_players = get_understat_ids(season).rename(
        columns={'id':'understat_id'})
    understat_players['understat_name'] = understat_players.name
    
    # Find missing players that have played more than 0 minutes
    
    # Load FPL data
    fpl_df = load_fpl_data(season)
    player_minutes = fpl_df.groupby('name').sum().minutes
    player_minutes.index.rename('fpl_name', inplace=True)
    
    # Add minutes to id_dict
    id_dict = pd.merge(id_dict, player_minutes, how='left', on='fpl_name')
    
    unmatched_fpl_players = id_dict[((id_dict['understat_name'].isna()) & 
                                     (id_dict['minutes'] != 0))]
    
    missing_us_players = understat_players.loc[[name not in
                       id_dict['understat_name'].tolist() for name in 
                       understat_players['understat_name']], :]    
    cancel = False
    max_try = 5
    for fpl_name, fpl_id in zip(unmatched_fpl_players.fpl_name,
                                unmatched_fpl_players.fpl_id):
        try_count = 0
        combined = False
        while ((try_count < max_try) & (not combined)):
            for name, idx in zip(missing_us_players.understat_name,
                                 missing_us_players.understat_id):
                print("{}, {}".format(name, idx))
                
            fpl_team = fpl_df.loc[fpl_df['element'] == fpl_id, 'team_x'].unique()
            fpl_mins = fpl_df.loc[fpl_df['element'] == fpl_id, 'minutes'].sum()
            fpl_apps = fpl_df.loc[((fpl_df['element'] == fpl_id) & 
                                   (fpl_df['minutes'] > 0)), 'minutes'].count()
            
            print("\nFPL name/id: {}/{}".format(fpl_name, fpl_id))
            
            us_name_input = input("Understat name: ")
            if not any(missing_us_players.understat_name == us_name_input):
                print("Warning: Invalid name\n")
                try_count += 1
                continue
            us_id_input = input("Understat id: ")
            if not any(missing_us_players.understat_id == us_id_input):
                print("Invalid id\n")
                try_count += 1
                continue
            
            fpl_team = fpl_df.loc[fpl_df['element'] == fpl_id, 'team_x'].unique()
            us_team = get_understat_team(us_id_input, season)
            fpl_mins = fpl_df.loc[fpl_df['element'] == fpl_id, 'minutes'].sum()
            us_mins = get_understat_mins(us_id_input, season)
            fpl_apps = fpl_df.loc[((fpl_df['element'] == fpl_id) & 
                                   (fpl_df['minutes'] > 0)), 'minutes'].count()
            us_apps = get_understat_apps(us_id_input, season)
            
            print("\nFPL teams for {}, {}: {}".format(fpl_name, fpl_id, fpl_team))
            print("Understat team for {}, {}: {}".format(us_name_input, 
                                                         us_id_input, 
                                                         us_team))
            print("FPL apps/mins: {}/{}".format(fpl_apps, fpl_mins))
            print("Understat apps/mins: {}/{}".format(us_apps, us_mins))
            
            combine_input = input("Combine these records (y/n)? ")
            print("\n---------------\n")
            
            if combine_input.lower() in ['y', 'yes']:
                id_dict.loc[id_dict['fpl_name'] == fpl_name, 
                    ['understat_name', 'understat_id']] = [us_name_input, us_id_input]
                missing_us_players.drop(
                    missing_us_players.loc[
                        missing_us_players.understat_id == us_id_input].index,
                    inplace=True)
                combined = True
            
            try_count += 1
        if cancel:
            break
        
    # Write the id dictionary to file
    
    id_dict.drop('minutes', axis=1, inplace=True)
    
    # Check that the path given in write_dir exists
    if write:
        if not os.path.isdir(write_dir):
            raise FileNotFoundError("The path {} is not a valid directory."
                                    .format(write_dir))
            
        season_us = season.replace("-", "_")
        id_dict_filename = write_dir + "/{}_id_dict.csv".format(season_us)
        id_dict.to_csv(id_dict_filename, index=False)
    
    if return_dict:
        return id_dict


# A piece of code for creating id dicts for given seasons

# data_dir = "C:/Users/jonat/OneDrive/Documents/Projects/Fantasy Premier League/Data"
# seasons = ["{}-{}".format(y, str(y + 1)[2:4]) for y in range(2022, 2023)]
# for season in seasons:
#     season_us = season.replace("-", "_")
#     write_dir = data_dir + "/" + season_us
#     match_ids(season, write_dir, write=True, return_dict=False)
#     final_match_ids(season, write_dir, write=True)
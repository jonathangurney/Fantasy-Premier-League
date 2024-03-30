# -*- coding: utf-8 -*-
"""
Created on Sat Oct 15 19:18:05 2022

@author: jonat
"""

from bs4 import BeautifulSoup
import datetime as dt
from GetUnderstatData import get_understat_match_stats
from io import StringIO
import os
import pandas as pd
import pytz
import requests

# Helper function which takes a row from a DataFrame containing columns
# 'kickoff_datetime' and 'opp_team_name' and a DataFrame of results and returns
# the name of a player's team.
def find_team(row, results_df):
    kickoff_date = row['kickoff_date']
    opposition = row['opp_team_name']
    opp_set = set([opposition])
    
    try:
        teams = results_df.loc[(results_df['kickoff_date'] == kickoff_date) & 
                               ((results_df['Home'] == opposition) | 
                                (results_df['Away'] == opposition)), ['Home', 'Away']].values[0]
    except IndexError:
        print(("Result for game at {} with opposition".format(kickoff_date) + 
               " {} not found".format(opposition)))
        
    team = set(teams).difference(opp_set)
    return(list(team)[0])

###############################################################################
#
# get_results_data
# 
# Function to scrape FBRef.com for results data for the specified season
# 
###############################################################################
def get_results_data(season, return_df=False, write=True, write_dir=None):
    # Check that the write directory is valid if write == True
    if write:
        if not os.path.isdir(write_dir):
            raise NotADirectoryError("Directory specified for writing does not exist.")
    
    y1 = season[0:4]
    y2 = str(int(y1) + 1)
    long_season = y1 + "-" + y2
    
    # Scrape results data from FBref
    results_url = ("https://fbref.com/en/comps/9/{0}/schedule/" + 
                   "{0}-Premier-League-Scores-and-Fixtures").format(long_season)
    resp = requests.get(results_url)
    soup = BeautifulSoup(resp.text, 'lxml')
    
    ###########################################################################
    # Create DataFrame of results data
    ###########################################################################
    table = soup.find('table')
    rows = table.findAll('tr')
    head_row = rows[0]
    body_rows = rows[1:]
    
    # Get column names
    col_names = [cell.text for cell in head_row.findAll('th')]
    col_names = col_names[1:]
    
    # Create data frame
    results_df = pd.DataFrame(columns=col_names)
    for row in body_rows:
        row_entry = [cell.text for cell in row.findAll('td')]
        if row_entry[5] != "":
            results_df.loc[len(results_df)] = row_entry
    
    if write:
        results_filename = write_dir + "/premier_league_{0}_{1}_results.csv".format(y1, y2[2:])
        results_df.to_csv(results_filename, index=False)
    
    if return_df:
        return results_df

###############################################################################
#
# get_fpl_data
# 
# Function to scrape the FPL API data from vaastav's Github page. The data is
# sourced from master/data/{season}/players. For all seasons
# specified using the start_season and end_season variables, the function
# proceeds via the following steps:
#   1. Create list of all players in current season.
#   2. Create DataFrame for each player containing gameweek-by-gameweek data.
#   3. Combine into single DataFrame containing all player data for the season.
#   4. Use team_id data to insert a "team_x" column containing players'
#      team names.
#   5. Write DataFrame to file.
# 
###############################################################################
def get_fpl_data(start_season, end_season, data_dir):
    # Create a list of seasons from the start and end seasons given
    year_start = int(start_season[0:4])
    year_end = int(end_season[0:4]) + 1
    
    seasons = [str(y1) + "-" + str(y1 + 1)[2:4] for y1 in range(year_start, year_end)]
    
    for season in seasons:
        print(season)
        season_y1 = season[2:4]
        
        # Access the names of the players playing in the given season
        id_url = ("https://raw.githubusercontent.com/vaastav/" + 
                  "Fantasy-Premier-League/master/data/{}/player_idlist.csv".format(season))
        id_data = pd.read_csv(id_url)
        
        # Create single DataFrame containing all gameweek-by-gameweek player data
        fpl_data = pd.DataFrame()
        for i in range(len(id_data)):
            if i == 0:
                print('{}/{}'.format(i + 1, len(id_data)))
            if ((i + 1) % 100) == 0:
                print('{}/{}'.format(i + 1, len(id_data)))
            if i == len(id_data) - 1:
                print('{}/{}'.format(i + 1, len(id_data)))
            
            first_name = id_data.loc[i, 'first_name']
            second_name = id_data.loc[i, 'second_name']
            player_id = id_data.loc[i, 'id']
            
            # SPECIFIC FIX FOR DAVID DE GEA SEASONS 2017-18 AND 2018-19
            if ((season == '2017-18') & (second_name == "De Gea")):
                second_name = "de Gea"
            if ((season == '2018-19') & (second_name == "de Gea")):
                second_name = "De Gea"
            
            if int(season_y1) <= 17:
                player_url = ("https://raw.githubusercontent.com/vaastav/" + 
                              "Fantasy-Premier-League/master/data/{0}/".format(season) + 
                              "players/{0}_{1}/gw.csv".format(first_name, second_name)
                              ).replace(' ', '%20')
            else:
                player_url = ("https://raw.githubusercontent.com/vaastav/" + 
                              "Fantasy-Premier-League/master/data/{0}/".format(season) + 
                              "players/{0}_{1}_{2}/gw.csv".format(first_name, second_name, player_id)
                              ).replace(' ', '%20')
            resp = requests.get(player_url)
            if resp.status_code != 200:
                raise Exception("Response status code {}".format(resp.status_code))
            else:
                data = pd.read_csv(StringIO(resp.text), sep=',')
                data['name'] = ' '.join([first_name, second_name])
                fpl_data = pd.concat([fpl_data, data], ignore_index=True)
        
        fpl_data['season_x'] = season
        
        # Load the team_id data
        team_id_data_filename = data_dir + '/team_id_list.csv'
        team_id_data = pd.read_csv(team_id_data_filename)
        team_id_data = team_id_data.loc[team_id_data['season'] == season]

        # Create dictionary for mapping team id to team name
        team_id_map = dict(zip(team_id_data['team'], team_id_data['team_name']))

        # Create column for opposition team name
        fpl_data['opp_team_name'] = [team_id_map[idx] for idx in fpl_data['opponent_team']]
        
        #######################################################################
        # 
        # Create column for player's team name using the results data
        # 
        #######################################################################
        
        # Check if results data for this season exists and download if it does not
        results_data_filename = (data_dir + "/results_data/premier_league" + 
                                 "_{}_results.csv".format(season.replace("-", "_")))
        if not os.path.exists(results_data_filename):
            results_dir = data_dir + '/results_data'
            get_results_data(season, write_dir=results_dir)
            
        
        # Import the results data
        results_data = pd.read_csv(results_data_filename)
        
        # Create column for datetime objects of kickoff
        fpl_data['kickoff_datetime'] = [pytz.utc.localize(dt.datetime.strptime(ko_time, '%Y-%m-%dT%H:%M:%SZ'))
                                        for ko_time in fpl_data['kickoff_time']]
        
        # Create column for kickoff date
        fpl_data['kickoff_date'] = [dtx.date() for dtx in fpl_data['kickoff_datetime']]
        
        # Set a London timezone object
        london_tz = pytz.timezone('Europe/London')
        
        # Create 'kickoff_datetime' column with kickoff datetime objects in UTC time
        results_data['kickoff_datetime'] = [london_tz.localize(dt.datetime.strptime((x1 + x2).strip(), '%Y-%m-%d%H:%M')).astimezone(pytz.utc)
                                            for x1, x2 in zip(results_data['Date'], results_data['Time'])]
        
        # Create column for kickoff date in UTC time
        results_data['kickoff_date'] = [dtx.date() for dtx in results_data['kickoff_datetime']]
        
        fpl_data['team_x'] = fpl_data.apply(find_team, axis=1, results_df=results_data)
        
        # Write the DataFrame to file
        season_us = season.replace('-', '_')
        season_data_dir = data_dir + '/{}'.format(season_us)
        
        if not os.path.isdir(season_data_dir):
            os.mkdir(season_data_dir)
        
        fpl_data_filename = season_data_dir + '/{}_fpl_data.csv'.format(season_us)
        fpl_data.to_csv(fpl_data_filename, index=False)

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


def add_element_type(season):
    fpl_data, fpl_data_filename = load_fpl_data(season, return_filename=True)
    
    if 'element_type' in fpl_data.columns:
        fpl_data.drop("element_type", axis=1, inplace=True)
        
    raw_url = ("https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League" + 
               "/master/data/{}/players_raw.csv".format(season))
    raw_data = pd.read_csv(raw_url)
    
    raw_data.rename({'id': 'element'}, axis=1, inplace=True)
    
    fpl_data = pd.merge(fpl_data, raw_data[['element', 'element_type']], 
                        how='left', on='element')
    
    fpl_data.to_csv(fpl_data_filename, index=False)
    
def load_id_df(season):
    season_us = season.replace('-', '_')
    data_dir = ("C:/Users/jonat/OneDrive/Documents/Projects/" + 
                "Fantasy Premier League/Data/{}".format(season_us))
    id_df_filename = data_dir + "/{}_id_dict.csv".format(season_us)
    id_df = pd.read_csv(id_df_filename)
    
    return id_df
    
def add_starts(season, overwrite=False):
    fpl_data, fpl_data_filename = load_fpl_data(season, return_filename=True)
    
    if (('starts' in fpl_data.columns) & (not overwrite)):
        raise Exception("Data already contains starts for {} ".format(season) + 
                        "season and data cannot be overwritten.")
    elif (('starts' in fpl_data.columns) & (overwrite)):
        fpl_data.drop('starts', axis=1, inplace=True)
    
    id_df = load_id_df(season)
    
    # Since id_df contains NaN values in the 'understat_id' column, we drop the
    # NaN values
    id_df.dropna(inplace=True)
    
    # Adjustment made for the incorrect fixture dates held by understat in 
    # the 2016-17 season
    if season == "2016-17":
        err_fixtures_filename = ("C:/Users/jonat/OneDrive/Documents/" + 
                                 "Projects/Fantasy Premier League/Data/" + 
                                 "2016_17/erroneous_fixtures.csv")
        err_fixtures = pd.read_csv(err_fixtures_filename)
    
    initial = True
    count = 0
    n_players = id_df.shape[0]
    q_prev = 0
    for (fpl_id, us_id) in zip(id_df.fpl_id, id_df.understat_id): 
        us_data = get_understat_match_stats(us_id, season)
        
        if len(us_data) == 0:
            continue
        
        us_data['element'] = fpl_id
        us_data['starts'] = [0 if position.lower() in ['sub'] else 1 for 
                             position in us_data.position]
        
        if season == "2016-17":            
            in_err_fixtures = [idx in err_fixtures.us_fixture_id.tolist() for 
                               idx in us_data.id.tolist()]
            
            if any(in_err_fixtures):
                us_data = pd.merge(us_data, 
                                   err_fixtures[['us_fixture_id', 'fpl_date']],
                                   how='left',
                                   left_on='id',
                                   right_on='us_fixture_id')
                
                us_data['date'] = us_data['fpl_date'].fillna(us_data.date)
                us_data.drop(['us_fixture_id', 'fpl_date'], axis=1, inplace=True)
        
        fpl_data = pd.merge(left=fpl_data,
                            how='left',
                            right=us_data[['element', 'date', 'starts']],
                            left_on=['element', 'kickoff_date'],
                            right_on=['element', 'date'])
        
        if not initial:
            fpl_data['starts'] = fpl_data['starts_x'].fillna(fpl_data['starts_y'])
            fpl_data.drop(['starts_x', 'starts_y'], axis=1, inplace=True)
            
        fpl_data.drop('date', axis=1, inplace=True)
        
        count += 1
        q_new = (count * 10) // n_players
        if q_new > q_prev:
            print("{:.0f}%".format((count * 100)/n_players))
        q_prev = q_new
        
        initial = False
        
    # Erase starts that have been included despite the player being at another
    # club at the time. This will show up as the player having started in 
    # Understat but will show as having 0 mins in the fpl data.
    fpl_data.loc[(fpl_data['starts'] == 1) & (fpl_data['minutes'] == 0), 'starts'] = 0
    fpl_data.fillna({'starts' : 0}, inplace=True)
    
    fpl_data.to_csv(fpl_data_filename, index=False)
        
def get_raw_data():
    data_dir = ("C:/Users/jonat/OneDrive/Documents/Projects/" + 
                "Fantasy Premier League/Data/2023_24")
    raw_data_url = ("https://raw.githubusercontent.com/vaastav/" + 
                    "Fantasy-Premier-League/master/data/2023-24/players_raw.csv")
    raw_data = pd.read_csv(raw_data_url)
    
    gameweek = input("Enter the GW associated with the raw data: ")
    
    raw_data_filename = data_dir + "/Raw data/gw{}_raw_data.csv".format(gameweek)
    raw_data.to_csv(raw_data_filename, index=False)
    

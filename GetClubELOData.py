# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 10:04:13 2024

@author: jonat
"""

import GetUnderstatData as getus
from io import StringIO
import pandas as pd
import requests

# A collection of scipts used to get data from the ClubELO API.

# Create ClubELO id dict
def create_clubelo_id_df(write_dir, write=True, return_df=False):
    seasons = ["{}-{}".format(y, str(y + 1)[2:4]) for y in range(2016, 2023)]
    initial = True
    for season in seasons:
        if initial:
            id_df = getus.load_team_id_df(season)
        else:
            df1 = getus.load_team_id_df(season)
            id_df = pd.concat([id_df, df1])
        
        initial = False
        
    id_df.drop(['understat_id', 'fpl_id'], axis=1, inplace=True)    
    
    id_df.drop_duplicates(inplace=True)
    
    print(id_df.shape)
    
    id_df['clubelo_name'] = None
    
    for name_us, name_fpl in zip(id_df.understat_name, id_df.fpl_name):
        print("-------------\n{}\n".format(name_us))
        url_us = "http://api.clubelo.com/" + name_us
        url_fpl = "http://api.clubelo.com/" + name_fpl
        
        resp_us = requests.get(url_us)
        if len(resp_us.text) > 38:
            id_df.loc[id_df.understat_name == name_us, 'clubelo_name'] = name_us
            continue
        
        resp_fpl = requests.get(url_fpl)
        if len(resp_fpl.text) > 38:
            id_df.loc[id_df.fpl_name == name_fpl, 'clubelo_name'] = name_fpl
            continue
        
        clubelo_input = input("\nInput ClubELO name: ")
        
        id_df.loc[id_df.fpl_name == name_fpl, 'clubelo_name'] = clubelo_input
    
    if write:
        id_df_filename = write_dir + "/team_id_df.csv"
        id_df.to_csv(id_df_filename, index=False)
        
    if return_df:
        return id_df
    
# To create the id Dataframe with ClubELO names included run the following 
# code snippet
# write_dir = "C:/Users/jonat/OneDrive/Documents/Projects/Fantasy Premier League/Data"
# id_df = create_clubelo_id_df(write_dir, return_df=True)


# A function to load the main team id dataframe
def load_main_team_id_df(index_col='fpl_name'):
    id_df_filename = ("C:/Users/jonat/OneDrive/Documents/Projects/" + 
                      "Fantasy Premier League/Data/team_id_df.csv")
    id_df = pd.read_csv(id_df_filename, index_col=index_col)
    
    return id_df

# A function to add club ELO to the FPL data
def add_elo(fpl_df):
    id_df = load_main_team_id_df()
    
    fpl_df['team_elo'] = None
    fpl_df['opp_elo'] = None
    
    
    h_teams = fpl_df.team_x.unique().tolist()
    a_teams = fpl_df.opp_team_name.unique().tolist()
    team_names = h_teams + list(set(a_teams) - set(h_teams))
    for name_fpl in team_names:
        name_clubelo = id_df.at[name_fpl, "clubelo_name"]
        
        url = "http://api.clubelo.com/" + name_clubelo
        resp = requests.get(url)
        clubelo_df = pd.read_csv(StringIO(resp.text), sep=',')
        
        # Trim any rows containing data from before start of FPL data
        min_date = min(fpl_df.kickoff_date)
        clubelo_df = clubelo_df.loc[clubelo_df.To >= min_date]
        
        # Set "From" and "To" as datetime objects
        clubelo_df["From"] = pd.to_datetime(clubelo_df.From)
        clubelo_df["To"] = pd.to_datetime(clubelo_df.To)
        
        # Set clubelo_df index to be time intervals
        clubelo_df.index = pd.IntervalIndex.from_arrays(
            clubelo_df.From,
            clubelo_df.To,
            closed='both')
        
        home_df = fpl_df.loc[fpl_df.team_x == name_fpl]
        away_df = fpl_df.loc[fpl_df.opp_team_name == name_fpl]
        
        fpl_df.loc[fpl_df.team_x == name_fpl, "team_elo"] = home_df['kickoff_date'].apply(
            lambda x : clubelo_df.iloc[clubelo_df.index.get_loc(x)]['Elo'])
        fpl_df.loc[fpl_df.opp_team_name == name_fpl, "opp_elo"] = away_df['kickoff_date'].apply(
            lambda x : clubelo_df.iloc[clubelo_df.index.get_loc(x)]['Elo'])
        
    return fpl_df

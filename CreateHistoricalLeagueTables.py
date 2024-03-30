import datetime as dt
import pandas as pd

##############################################################################
##############################################################################
#
# A group of functions used to create at-the-moment league tables using results
# data downloaded from FBref.com
#
##############################################################################
##############################################################################

##############################################################################
# 
# load_results_data
# 
# Helper function to load results data
# 
##############################################################################
def load_results_data(season, data_dir):
    season_us = season.replace("-", "_")
    data_filename = '{}/premier_league_{}_results.csv'.format(data_dir, season_us)
    results_data = pd.read_csv(data_filename)
    return(results_data)


##############################################################################
# 
# clean_results_data
# 
# A function to clean results data downloaded from fbref.com
# 
##############################################################################
def clean_results_data(season, data_dir, return_data=False, write_data=True):
    season_us = season.replace("-", "_")

    data_filename = '{}/premier_league_{}_results.csv'.format(data_dir, season_us)
    
    # Load the results dataset
    results_df = pd.read_csv(data_filename)
    
    # Remove blank rows from the data frame
    results_df = results_df.loc[~results_df.isna().all(axis=1), :]
    
    # Write data to file if write_data == True
    if write_data:
        results_df.to_csv(data_filename, index=False)
    
    # Return cleaned data frame if required
    if return_data:
        return(results_df)
    else:
        return


##############################################################################
# 
# update_table_dict
# 
# A helper function used in create_historical_league_tables which updates the 
# a league table dict using results data.
# 
##############################################################################
def update_table_dict(table_dict, results): 
    for i in results.index:
        # Initialize dicts to store values
        home_dict = dict()
        away_dict = dict()
        
        # Assign home and away teams
        home_team = results.loc[i, 'Home']
        away_team = results.loc[i, 'Away']
        
        # Add 1 to the 'Played' stat for each team
        home_dict['Played'] = away_dict['Played'] = 1
        
        # Use the score to compute goals for and goals against for each team
        score = results.loc[i, 'Score']
        hyphen_pos = score.find(b'\xe2\x80\x93'.decode('utf-8'))
        
        home_dict['GF'] = away_dict['GA'] = int(score[:(hyphen_pos)])
        home_dict['GA'] = away_dict['GF'] = int(score[(hyphen_pos + 1):])
        home_dict['GD'] = home_dict['GF'] - home_dict['GA']
        away_dict['GD'] = away_dict['GF'] - away_dict['GA']
        
        # Record whether each team kept a clean sheet
        if home_dict['GA'] == 0:
            home_dict['Clean Sheets'] = 1
        else:
            home_dict['Clean Sheets'] = 0
        
        if away_dict['GA'] == 0:
            away_dict['Clean Sheets'] = 1
        else:
            away_dict['Clean Sheets'] = 0
        
        # Allocate points based on the scoreline
        if home_dict['GF'] > away_dict['GF']:
            home_dict['Points'] = 3
            away_dict['Points'] = 0
        elif home_dict['GF'] < away_dict['GF']:
            home_dict['Points'] = 0
            away_dict['Points'] = 3
        else:
            home_dict['Points'] = 1
            away_dict['Points'] = 1
            
        # Update table dict
        table_dict[home_team].update({key : table_dict[home_team][key] + home_dict[key] for key in home_dict})
        table_dict[away_team].update({key : table_dict[away_team][key] + away_dict[key] for key in away_dict})
    
    # Return the updated dict
    return(table_dict)


##############################################################################
# 
# create_historical_league_tables
# 
# A function to create gameday-by-gameday historical league tables for the EPL
# and write output dataframe to specified directory.
# 
##############################################################################
def create_historical_league_tables(season, results_df, write_data = True, save_dir = None):
    # Set start year
    start_year = season[0:4]
    
    # Turn 'Date' into a datetime object
    results_df.loc[:, 'Date'] = [dt.datetime.strptime(date, '%Y-%m-%d') for date in results_df['Date']]
    
    # Get unique values in datetime column
    dates_unique = results_df['Date'].sort_values().unique()
    
    # Get list of clubs
    home_clubs = set(results_df['Home'])
    away_clubs = set(results_df['Away'])
    clubs = sorted(list(home_clubs.union(away_clubs)))
    n_clubs = len(clubs)
    
    # Initialize league table
    league_table_columns = ['Date', 'Team', 'Played', 'Clean Sheets', 'GF', 'GA', 'GD', 'Points', 'Position']
    initial_date = dt.datetime(int(start_year), 1, 1)
    
    league_table_data = []
    for i, club in enumerate(clubs):
        init_values = [initial_date, club] + [0] * 6 + ['NA']
        league_table_data.append(dict(zip(league_table_columns, init_values)))
    
    # Update the league table using the fixtures at each kickoff time
    for date in dates_unique:
        results = results_df.loc[results_df['Date'] == date, :]
        
        # Change the format of the data from a list of dicts to a dict of dicts
        # suitable for passing to update_table_dict
        table_list = league_table_data[-n_clubs:]
        table_dict = dict()
        for i, club in enumerate(clubs):
            table_dict[club] = {key : table_list[i][key] for key in table_list[i] if key not in ['Team']}  
        
        table_dict_updated = update_table_dict(table_dict, results)
        
        # Change the format back to a list of dicts suitable for appending to
        # the league data table
        table_list_updated = []
        for i, club in enumerate(clubs):
            table_dict_updated[club].update({'Team' : club, 'Date' : date})
            table_list_updated.append(table_dict_updated[club])
        
        # Add position to the table
        table = pd.DataFrame(table_list_updated)
        table['Position'] = table.sort_values(['Points', 'GD', 'GF', 'Team'], ascending = [False, False, False, True]).reset_index().sort_values('Team').index + 1
        
        # Revert the table to a list of dictionaries
        table_list_updated = table.to_dict('records')
        
        # Append the new list to the primary list of league table data
        league_table_data.extend(table_list_updated)
    
    #Convert the historical league table to a DataFrame
    hist_league_table = pd.DataFrame(league_table_data)
    
    # Write the data to file if required
    if write_data:
        if save_dir == None:
            raise ValueError('Argument "save_dir" is None, should be valid file path.')
        
        hist_league_table.to_csv('{}/premier_league_{}_{}_hist_league_tables.csv'.format(save_dir, season[2:4], season[5:]))
    
    return 

results_data_dir = "C://Users/jonat/OneDrive/Documents/Projects/Fantasy Premier League/Data/results_data"
results_df = load_results_data("2022-23", results_data_dir)
hlt_save_dir = "C://Users/jonat/OneDrive/Documents/Projects/Fantasy Premier League/Data/historical_league_tables"
create_historical_league_tables("2022-23", results_df, save_dir=hlt_save_dir)

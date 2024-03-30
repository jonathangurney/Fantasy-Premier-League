# -*- coding: utf-8 -*-
"""
Created on Sat Oct 15 11:54:48 2022

@author: jonat
"""

import os
import pandas as pd

os.chdir('C://Users/jonat/OneDrive/Documents/Projects/Fantasy Premier League')
data_dir = './Data'

# Import FiveThirtyEight's SPI data
spi_data_filename = data_dir + '/spi_matches.csv'
spi_data = pd.read_csv(spi_data_filename)

#%%

# Select only the columns relevant to the EPL
pl_spi_data = spi_data.loc[spi_data['league'] == 'Barclays Premier League']

# Map the team names to the ones used elsewhere in the data
spi_team_names_map = {'AFC Bournemouth' : 'Bournemouth',
                      'Brighton and Hove Albion' : 'Brighton',
                      'Huddersfield Town' : 'Huddersfield',
                      'Manchester United' : 'Manchester Utd',
                      'Newcastle' : 'Newcastle Utd',
                      'Sheffield United' : 'Sheffield Utd',
                      'Tottenham Hotspur' : 'Tottenham',
                      'West Bromwich Albion' : 'West Brom',
                      'West Ham United' : 'West Ham',
                      'Wolverhampton' : 'Wolves'}

pl_spi_data['team1'] = [spi_team_names_map[name] if name in spi_team_names_map else name for name in pl_spi_data['team1']]
pl_spi_data['team2'] = [spi_team_names_map[name] if name in spi_team_names_map else name for name in pl_spi_data['team2']]

#%%

# Reset the index
pl_spi_data.reset_index(inplace=True)

#%%

# Drop the newly-formed index column
pl_spi_data.drop('index', inplace=True, axis=1)

#%%

# Save the resulting data frame
pl_spi_filename = data_dir + '/pl_spi_data.csv'
pl_spi_data.to_csv(pl_spi_filename)

# -*- coding: utf-8 -*-
"""
Created on Mon Jan 16 19:07:24 2023

@author: jonat
"""

# A helper function to shift a season by a given increment.
def increment_season(season, increment):
    season_y1 = int(season[0:4])
    output_season_y1 = season_y1 + increment
    output_season = str(output_season_y1) + "-" + str(output_season_y1 + 1)[2:4]
    return(output_season)
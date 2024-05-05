import pandas as pd
import numpy as np
import os

for i in os.listdir("./data/"):
    if "constructor" in i:
        constructor_standings = pd.read_csv(os.path.join("./path/", i))
    elif "driver" in i:
        driver_standings = pd.read_csv(os.path.join("./path/", i))
    elif "races" in i:
        races = pd.read_csv(os.path.join("./path/", i))
    elif "results" in i:
        results = pd.read_csv(os.path.join("./path/", i))
    elif "weather" in i:
        weather = pd.read_csv(os.path.join("./path/", i))
    elif "qualifying" in i:
        qualifying = pd.read_csv(os.path.join("./path/", i))
    else:
        print("Document {} not recognized".format(i))

# Rename grid position column
qualifying.rename(columns = {'grid_position': 'grid'}, inplace = True)

# Drop unnecessary data, you stay with: 
# season, round, driver, driver_points, driver_wins, driver_standings_pos
driver_standings.drop(['driver_points_after_race', 
                       'driver_wins_after_race', 
                       'driver_standings_pos_after_race'] ,axis = 1, inplace = True) 

# Drop unnecessary data, you stay with: 
# season round constructor constructor_points constructor_wins constructor_standings_pos
constructor_standings.drop(['constructor_points_after_race', 
                            'constructor_wins_after_race',
                            'constructor_standings_pos_after_race' ],axis = 1, inplace = True)

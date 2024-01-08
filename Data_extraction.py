import requests
import pandas as pd
import os

def lookup (df, team, points):
  """
  Calculate the points a team won after each round.

  Parameters:
  - df (DataFrame): Input DataFrame containing season, team, round, and points information.
  - team_column (str): Name of the column containing team information.
  - round_column (str): Name of the column containing round information.
  - points_column (str): Name of the column containing points information.

  Returns:
  - DataFrame: A new DataFrame with an additional column 'points_after_race'
                indicating the points a team won after each round.
  """

  # New column with the team specifying the season and the round, for example team: leclerc, round: 1 and season: 2022 -> 2022leclerc1
  df['lookup1'] = df.season.astype(str) + df[team] + df['round'].astype(str)
  # New column with the team specifying the season and the previous round, for example eam: leclerc, round: 1 and season: 2022 -> 2022leclerc0
  df['lookup2'] = df.season.astype(str) + df[team] + (df['round']-1).astype(str)
  # Calculate the points the team won after each round. For example; match in the right dataframe the lookup2 (after the round) and the lookup1 (before the round) and keep track of the team's points
  # In this case for example could be points_x after the round, and points_y before the round 
  new_df = df.merge(df[['lookup1', points]], how = 'left', left_on='lookup2',right_on='lookup1')
  # Drop the keywords we created by merging the dataframes except for the points of the left dataframe
  new_df.drop(['lookup1_x', 'lookup2', 'lookup1_y'], axis = 1, inplace = True)
  # Change the name of the points from the right dataframe to points_after_race and points from the left dataframe to only points
  new_df.rename(columns = {points+'_x': points+'_after_race', points+'_y': points}, inplace = True)
  # Fill NA with 0's, if 'points'_after_race is 0 means the season has just started or the team hasn't won any points yet
  new_df[points].fillna(0, inplace = True)
  return new_df

def save_data(data: pd.DataFrame, saving_path: str = "./data", name: str = "races") -> None:
    """
    Save a DataFrame to a CSV file.

    Parameters:
    - data (pd.DataFrame): DataFrame to be saved.
    - saving_path (str): Directory path where the file will be saved.
    - name (str): Name of the CSV file (without extension).
    """
    # Create the directory if it doesn't exist
    os.makedirs(saving_path, exist_ok=True)

    # Save the DataFrame to a CSV file
    data.to_csv(os.path.join(saving_path, name + ".csv"), index=False)

def get_rounds(races: pd.DataFrame = None, start_year: int = 2000, end_year: int = 2023) -> list:
    """
    Get a list of rounds for each season within the specified range.

    Parameters:
    - races (pd.DataFrame): DataFrame containing race information.
    - start_year (int): Starting year for data extraction.
    - end_year (int): Ending year for data extraction.

    Returns:
    - list: List of rounds for each season.
    """
    # If races DataFrame is not provided, extract it for the specified range
    if races is None:
        races = extract_race_rounds(start_year=start_year, end_year=end_year)

    rounds = []

    # Group races DataFrame by 'season' and iterate through groups
    for year, group in races.groupby('season'):
        # Append a list containing the year and rounds for that season
        rounds.append([year, group['round'].tolist()])

    return rounds


def extract_race_rounds(start_year: int = 1950, end_year: int = 2023) -> pd.DataFrame:
    """
    Extract F1 race data from ergast API for the specified range of years.

    Parameters:
    - year_start (int): Starting year for data extraction.
    - year_end (int): Ending year for data extraction.

    Returns:
    - pd.DataFrame: DataFrame containing F1 race data.
    """
    assert start_year < end_year, "Wrong configuration of the starting year and ending year, please make sure the configuration is correct"

    races_list = []

    for year in range(start_year, end_year + 1):
        # Construct the API URL for the specific year
        url = f'https://ergast.com/api/f1/{year}.json'
        # Make an HTTP request to get the JSON data
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Extract relevant race information from the JSON data
            json_data = response.json()
            races = json_data.get('MRData', {}).get('RaceTable', {}).get('Races', [])

            # Iterate through each race and extract desired attributes
            for item in races:
                race_dict = {
                    'season': int(item.get('season', None)),
                    'round': int(item.get('round', None)),
                    'circuit_id': item['Circuit'].get('circuitId', None),
                    'lat': float(item['Circuit']['Location'].get('lat', None)),
                    'long': float(item['Circuit']['Location'].get('long', None)),
                    'country': item['Circuit']['Location'].get('country', None),
                    'date': item.get('date', None),
                    'url': item.get('url', None)
                }

                # Append the race information to the list
                races_list.append(race_dict)

    # Convert the list of dictionaries to a DataFrame and return
    return pd.DataFrame(races_list)

def extract_driver_standings(rounds):
    """
    Extract driver standings data from the ergast API for the specified rounds.

    Parameters:
    - rounds (list): List of rounds, where each round is a tuple containing season and a list of round numbers.

    Returns:
    - pd.DataFrame: DataFrame containing driver standings data.
    """
    # Initialize a list to store driver standings data
    driver_standings_list = []

    # Iterate through the rounds and extract driver standings data
    for n in range(len(rounds)):
        for i in rounds[n][1]:
            # Construct the API URL for driver standings
            url = f'https://ergast.com/api/f1/{rounds[n][0]}/{i}/driverStandings.json'
            response = requests.get(url)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                json_data = response.json()

                # Extract driver standings information from the JSON data
                standings = json_data.get('MRData', {}).get('StandingsTable', {}).get('StandingsLists', [])[0].get('DriverStandings', [])

                for item in standings:
                    driver_standings_dict = {
                        'season': int(json_data['MRData']['StandingsTable']['StandingsLists'][0]['season']),
                        'round': int(json_data['MRData']['StandingsTable']['StandingsLists'][0]['round']),
                        'driver': item['Driver'].get('driverId', None),
                        'driver_points': int(item.get('points', None)),
                        'driver_wins': int(item.get('wins', None)),
                        'driver_standings_pos': int(item.get('position', None))
                    }

                    # Append the driver standings information to the list
                    driver_standings_list.append(driver_standings_dict)

    # Convert the list of dictionaries to a DataFrame and return
    return pd.DataFrame(driver_standings_list)

def extract_race_results(rounds):
    """
    Extract race results data from the ergast API for the specified rounds.

    Parameters:
    - rounds (list): List of rounds, where each round is a tuple containing season and a list of round numbers.

    Returns:
    - pd.DataFrame: DataFrame containing race results data.
    """
    # Initialize a list to store race results data
    results_list = []

    # Iterate through the rounds and extract race results data
    for n in range(len(rounds)):
        for i in rounds[n][1]:
            # Construct the API URL for race results
            url = f'http://ergast.com/api/f1/{rounds[n][0]}/{i}/results.json'
            response = requests.get(url)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                json_data = response.json()

                # Extract race results information from the JSON data
                races = json_data.get('MRData', {}).get('RaceTable', {}).get('Races', [])

                try:
                    for race in races:
                        for item in race.get('Results', []):
                            result_dict = {
                                'season': int(race.get('season', None)),
                                'round': int(race.get('round', None)),
                                'circuit_id': race['Circuit'].get('circuitId', None),
                                'driver': item['Driver'].get('driverId', None),
                                'date_of_birth': item['Driver'].get('dateOfBirth', None),
                                'nationality': item['Driver'].get('nationality', None),
                                'constructor': item['Constructor'].get('constructorId', None),
                                'grid': int(item.get('grid', None)),
                                'time': int(item['Time']['millis']) if item.get('Time') else None,
                                'status': item.get('status', None),
                                'points': int(item.get('points', None)),
                                'podium': int(item.get('position', None)),
                                'url': race.get('url', None)
                            }

                            # Append the race results information to the list
                            results_list.append(result_dict)

                except Exception as e:
                    print(f"Error extracting results for {rounds[n][0]}: {e}")

    # Convert the list of dictionaries to a DataFrame and return
    return pd.DataFrame(results_list)


def extract_constructor_standings(constructor_rounds):
    """
    Extract constructor standings data from the ergast API for the specified rounds.

    Parameters:
    - constructor_rounds (list): List of constructor rounds, where each round is a tuple containing season and a list of round numbers.

    Returns:
    - pd.DataFrame: DataFrame containing constructor standings data.
    """
    # Initialize a list to store constructor standings data
    constructor_standings_list = []

    # Iterate through the constructor rounds and extract constructor standings data
    for n in range(len(constructor_rounds)):
        for i in constructor_rounds[n][1]:
            # Construct the API URL for constructor standings
            url = f'https://ergast.com/api/f1/{constructor_rounds[n][0]}/{i}/constructorStandings.json'
            response = requests.get(url)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                json_data = response.json()

                # Extract constructor standings information from the JSON data
                standings = json_data.get('MRData', {}).get('StandingsTable', {}).get('StandingsLists', [])[0].get('ConstructorStandings', [])

                for item in standings:
                    constructor_standings_dict = {
                        'season': int(json_data['MRData']['StandingsTable']['StandingsLists'][0]['season']),
                        'round': int(json_data['MRData']['StandingsTable']['StandingsLists'][0]['round']),
                        'constructor': item['Constructor'].get('constructorId', None),
                        'constructor_points': int(item.get('points', None)),
                        'constructor_wins': int(item.get('wins', None)),
                        'constructor_standings_pos': int(item.get('position', None))
                    }

                    # Append the constructor standings information to the list
                    constructor_standings_list.append(constructor_standings_dict)

    # Convert the list of dictionaries to a DataFrame and return
    return pd.DataFrame(constructor_standings_list)


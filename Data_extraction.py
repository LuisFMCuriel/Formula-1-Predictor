import requests
import pandas as pd

def F1_data_extraction_general(year_start: int = 1950, year_end: int = 2023) -> pd.DataFrame:
    """
    Extract F1 race data from ergast API for the specified range of years.

    Parameters:
    - year_start (int): Starting year for data extraction.
    - year_end (int): Ending year for data extraction.

    Returns:
    - pd.DataFrame: DataFrame containing F1 race data.
    """
    assert year_start < year_end, "Wrong configuration of the starting year and ending year, please make sure the configuration is correct"

    races_list = []

    for year in range(year_start, year_end + 1):
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

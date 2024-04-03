import requests
import pandas as pd
import os
from selenium import webdriver
from bs4 import BeautifulSoup

def calculate_points_per_round (df, team, points) -> pd.DataFrame:
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

def get_rounds(races: pd.DataFrame = None, 
               start_year: int = 2000, 
               end_year: int = 2023, 
               save: bool = True) -> list:
    """
    Get a list of rounds for each season within the specified range.

    Parameters:
    - races (pd.DataFrame): DataFrame containing race information.
    - start_year (int): Starting year for data extraction.
    - end_year (int): Ending year for data extraction.
    - save (bool): Save races csv

    Returns:
    - list: List of rounds for each season.
    """
    # If races DataFrame is not provided, extract it for the specified range
    if races is None:
        try:
            races = pd.read_csv('./data/races_from{}to{}.csv'.format(start_year, end_year))
        except:
            races = extract_race_rounds(start_year=start_year, end_year=end_year, save = save)
        
    rounds = []

    # Group races DataFrame by 'season' and iterate through groups
    for year, group in races.groupby('season'):
        # Append a list containing the year and rounds for that season
        rounds.append([year, group['round'].tolist()])

    return rounds


def extract_race_rounds(start_year: int = 1950, end_year: int = 2023, save: bool = True) -> pd.DataFrame:
    """
    Extract F1 race data from ergast API for the specified range of years.

    Parameters:
    - year_start (int): Starting year for data extraction.
    - year_end (int): Ending year for data extraction.
    - save (bool): Save races csv.

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
    if save:
        save_data(pd.DataFrame(races_list), name='races_from{}to{}'.format(start_year, end_year))
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
                        'driver_points': float(item.get('points', None)),
                        'driver_wins': float(item.get('wins', None)),
                        'driver_standings_pos': float(item.get('position', None))
                    }

                    # Append the driver standings information to the list
                    driver_standings_list.append(driver_standings_dict)

    # Convert the list of dictionaries to a DataFrame and return
    return pd.DataFrame(driver_standings_list)

def extract_race_results(rounds: list):
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
                                'time': float(item['Time']['millis']) if item.get('Time') else None,
                                'status': item.get('status', None),
                                'points': float(item.get('points', None)),
                                'podium': int(item.get('position', None)),
                                'url': race.get('url', None)
                            }

                            # Append the race results information to the list
                            results_list.append(result_dict)

                except Exception as e:
                    print(f"Error extracting results for {rounds[n][0]}: {e}")

    # Convert the list of dictionaries to a DataFrame and return
    return pd.DataFrame(results_list)


def extract_constructor_standings(constructor_rounds: list):
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
                        'constructor_points': float(item.get('points', None)),
                        'constructor_wins': float(item.get('wins', None)),
                        'constructor_standings_pos': int(item.get('position', None))
                    }

                    # Append the constructor standings information to the list
                    constructor_standings_list.append(constructor_standings_dict)

    # Convert the list of dictionaries to a DataFrame and return
    return pd.DataFrame(constructor_standings_list)

def get_weather_info(df: pd.DataFrame = None, start_year: int = 1950, end_year: int = 2023, save: bool = True):
    """
    Extracts weather information from web pages linked in the DataFrame.

    Args:
    - df (DataFrame): A pandas DataFrame containing a column named 'url' which
                    contains URLs linking to web pages.
    - year_start (int): Starting year for data extraction.
    - year_end (int): Ending year for data extraction.
    - save (bool): Save races csv
    Returns:
    - list: A list containing weather information extracted from the web pages.
          If weather information is not found or an error occurs, 'not found'
          is appended to the list.
    """
    info = []
    # Use keywords to find out what was the weather that day
    weather_dict = {'weather_warm': ['soleggiato', 'clear', 'warm', 'hot', 'sunny', 'fine', 'mild', 'sereno'],
               'weather_cold': ['cold', 'fresh', 'chilly', 'cool'],
               'weather_dry': ['dry', 'asciutto'],
               'weather_wet': ['showers', 'wet', 'rain', 'pioggia', 'damp', 'thunderstorms', 'rainy'],
               'weather_cloudy': ['overcast', 'nuvoloso', 'clouds', 'cloudy', 'grey', 'coperto']}
    
    # One hot encode the weather, depending if was cold, dry, wet, cloudy or warm using the previous dict
    weather_df = pd.DataFrame(columns = weather_dict.keys())

    # If races DataFrame is not provided, extract it for the specified range
    if df is None:
        try:
            df = pd.read_csv('./data/races_from{}to{}.csv'.format(start_year, end_year))
        except:
            df = extract_race_rounds(start_year=start_year, end_year=end_year, save = save)

    # Create a dataframe for weather
    weather = df.iloc[:,[0,1,2]]
    # Iterate through each URL in the DataFrame
    for link in df['url']:
        try:
            # Try to read HTML tables from the webpage
            for i in range(4):
                df_table = pd.read_html(link)[i]
                # Look for the 'Weather' column in the table
                if 'Weather' in list(df_table.iloc[:, 0]):
                    # If found, append the weather information to the 'info' list
                    n = list(df_table.iloc[:, 0]).index('Weather')
                    info.append(df_table.iloc[n, 1])
                    break
            else:
                # If 'Weather' column not found in first 4 tables, use Selenium to fetch weather information
                driver = webdriver.Chrome()
                driver.get(link)
                # click language button
                button = driver.find_element_by_link_text('Italiano')
                button.click()
                # Extract weather information using XPath
                clima = driver.find_element_by_xpath('//*[@id="mw-content-text"]/div/table[1]/tbody/tr[9]/td').text
                info.append(clima)
                driver.quit()
        except Exception as e:
            # If an exception occurs, append 'not found' to the 'info' list
            info.append('not found')
            #print(f"Error occurred for {link}: {e}")
    # Add the found info into a new column called weather
    weather['weather'] = info
    # map the weather with keywords for one hot encoding
    for col in weather_df:
        weather_df[col] = weather['weather'].map(lambda x: 1 if any(i in weather_dict[col] for i in x.lower().split()) else 0)

    # Join info of the weather with the info of the races: season, round, circuit_id. A total of 9 columns
    weather_info = pd.concat([weather, weather_df], axis = 1)
    return weather_info

def get_qualifying_results(start_year=1983, end_year=2023):
    """
    Scrapes qualifying results data from the Formula 1 website for the specified range of years.

    Args:
        start_year (int): The start year for scraping qualifying results. Default is 1983.
        end_year (int): The end year for scraping qualifying results. Default is 2023.

    Returns:
        pd.DataFrame: A DataFrame containing the scraped qualifying results data.
    """
    qualifying_results = pd.DataFrame()
    
    # Loop through each year in the specified range
    for year in range(start_year, end_year):
        # Construct URL for the races page of the current year
        url = f'https://www.formula1.com/en/results.html/{year}/races.html'
        # Send GET request to fetch the webpage
        r = requests.get(url)
        # Parse the HTML content
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # Extract links for each race of the current year
        year_links = [link.get('href') for link in soup.find_all('a', class_="resultsarchive-filter-item-link FilterTrigger") 
                      if f'/en/results.html/{year}/races/' in link.get('href')]
        
        year_df = pd.DataFrame()
        new_url = 'https://www.formula1.com{}'
        
        # Loop through each race link of the current year
        for n, link in enumerate(year_links):
            # Modify the link to point to the starting grid page
            link = link.replace('race-result.html', 'starting-grid.html')
            # Print the URL for debugging purposes
            print(new_url.format(link))
            # Read HTML tables from the starting grid page
            df_list = pd.read_html(new_url.format(link))
            df = df_list[0]
            # Add columns for season and round number
            df['season'] = year
            df['round'] = n + 1
            # Remove columns with 'Unnamed' prefix
            df = df.loc[:, ~df.columns.str.startswith('Unnamed')]
            # Concatenate the current race data to the year DataFrame
            year_df = pd.concat([year_df, df])

        # Concatenate the year DataFrame to the overall qualifying results DataFrame
        qualifying_results = pd.concat([qualifying_results, year_df])
        
    # Rename columns for readability
    qualifying_results.rename(columns = {'Pos': 'grid_position', 'Driver': 'driver_name', 'Car': 'car',
                                     'Time': 'qualifying_time'}, inplace = True)
    # Remove row No
    qualifying_results.drop('No', axis = 1, inplace = True)
    
    return qualifying_results
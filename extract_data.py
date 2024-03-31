import argparse
from Data_extraction import *

def main():
    # Set up command-line argument parser
    parser = argparse.ArgumentParser(description="Extract and process Formula 1 data.")
    parser.add_argument("--start_year", type=int, help="Start year for data extraction")
    parser.add_argument("--end_year", type=int, help="End year for data extraction")

    # Parse command-line arguments
    args = parser.parse_args()

    # Check if both start_year and end_year are provided
    if not args.start_year or not args.end_year:
        parser.error("Please provide both start_year and end_year arguments.")

    # Extract round information based on provided start and end years
    rounds = get_rounds(start_year=args.start_year, end_year=args.end_year)

    # Extract and process driver standings data
    driver_standings = extract_driver_standings(rounds)
    driver_standings = calculate_points_per_round(driver_standings, 'driver', 'driver_points')
    driver_standings = calculate_points_per_round(driver_standings, 'driver', 'driver_wins')
    driver_standings = calculate_points_per_round(driver_standings, 'driver', 'driver_standings_pos')
    save_data(driver_standings, name='driver_standings_from{}to{}'.format(args.start_year, args.end_year))

    # Extract and process constructor standings data
    constructor_standings = extract_constructor_standings(rounds)
    constructor_standings = calculate_points_per_round(constructor_standings, 'constructor', 'constructor_points')
    constructor_standings = calculate_points_per_round(constructor_standings, 'constructor', 'constructor_wins')
    constructor_standings = calculate_points_per_round(constructor_standings, 'constructor', 'constructor_standings_pos')
    save_data(constructor_standings, name='constructor_standings_from{}to{}'.format(args.start_year, args.end_year))

    # Extract and save race results data
    results = extract_race_results(rounds)
    save_data(results, name='results_from{}to{}'.format(args.start_year, args.end_year))

    # Extract and save race weather data
    weather = get_weather_info(start_year = args.start_year, end_year = args.end_year)
    save_data(weather, name='weather_from{}to{}'.format(args.start_year, args.end_year))

if __name__ == "__main__":
    main()
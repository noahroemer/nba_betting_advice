import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
from lxml import etree
import csv
from collections import defaultdict
import sqlite3
from datetime import datetime

#Web scraping the data
url = 'https://www.teamrankings.com/nba/stat/points-per-game'

table = pd.read_html(url)
scoring_table = table[0]

scoring_table.to_csv('NBA Team Scoring.csv')

url_1 = 'https://www.teamrankings.com/nba/stat/opponent-points-per-game'

table_1 = pd.read_html(url_1)
defensive_table = table_1[0]

defensive_table.to_csv('NBA Team Defense.csv')

#Cleaning Scoring Data
file_path_s = 'NBA Team Scoring.csv'
df_s = pd.read_csv(file_path_s)

df_s_cleaned = df_s.iloc[:, 2:]

df_s_cleaned.to_csv("Cleaned Scoring Data.csv", index=False)

#Cleaning Defensive Data
file_path_d = 'NBA Team Defense.csv'
df_d = pd.read_csv(file_path_d)

df_d_cleaned = df_d.iloc[:, 2:]

df_d_cleaned.to_csv('Cleaned Defensive Data.csv', index=False)

#Opening csv files
file_s = open('Cleaned Scoring Data.csv', 'r')
file_d = open('Cleaned Defensive Data.csv', 'r')

#Reading Scoring Data
reader = csv.reader(file_s)
scoring = {}
for row in reader:
    scoring[row[0]] = {'PPG': row[1], 'Last 3': row[2], 'Last 1': row[3],'Home': row[4], 'Away': row[5]}

#Deletes top row of csv data
del scoring['Team']

#Reading Defensive Data
reader_d = csv.reader(file_d)
defense = {}
for row in reader_d:
    defense[row[0]] = {'OPP_PPG': row[1], 'Last 3': row[2], 'Last 1': row[3], 'Home': row[4], 'Away': row[5]}

#Deletes top row of csv data
del defense['Team']

#'scoring' is dictionary with scoring data of each team
#'defense' is dictionary with defensive data of each team

#Building the model
'''
#Get teams
home_team = input('Enter the home team (city): ')
away_team = input('Enter the away team (city): ')

#find team name in dictionary
if home_team in scoring and defense:
    home_team_ppg = float(scoring[home_team]['PPG'])
    home_team_home_ppg = float(scoring[home_team]['Home'])
    home_team_last3 = float(scoring[home_team]['Last 3'])
    home_team_def_ppg = float(defense[home_team]['OPP_PPG'])
    home_team_def_last3 = float(defense[home_team]['Last 3'])
    home_team_def_home =  float(defense[home_team]['Home'])
else:
    print('The home team you entered was not found.')

if away_team in scoring and defense:
    away_team_ppg = float(scoring[away_team]['PPG'])
    away_team_away_ppg = float(scoring[away_team]['Away'])
    away_team_last3 = float(scoring[away_team]['Last 3'])
    away_team_def_ppg = float(defense[away_team]['OPP_PPG'])
    away_team_def_last3 = float(defense[away_team]['Last 3'])
    away_team_def_away = float(defense[away_team]['Away'])
else:
    print('The away team you entered was not found.')

#First formulas, super basic
home_predicted_points = home_team_ppg/6 + home_team_home_ppg/6 + home_team_last3/6 + away_team_def_ppg/6 + away_team_def_last3/6 + away_team_def_away/6
away_predicted_points = away_team_ppg/6 + away_team_away_ppg/6 + away_team_last3/6 + home_team_def_ppg/6 + home_team_def_home/6 + home_team_def_last3/6

print('The predicted points for the', home_team, 'is', home_predicted_points)
print('The predicted points for the', away_team, 'is', away_predicted_points)

if home_predicted_points > away_predicted_points:
    print(home_team, 'should win by', home_predicted_points - away_predicted_points, 'points.')

elif away_predicted_points > home_predicted_points:
    print(away_team, 'should win by', away_predicted_points - home_predicted_points, 'points.')

print('The total number of points should be', home_predicted_points + away_predicted_points)
'''

#Gathering odds of the games
#Come back to it to make sure it knows how to work with the dates correctly
url_odds = 'https://www.teamrankings.com/nba/odds/'

web_odds = pd.read_html(url_odds)
lines_tables = web_odds

spread_odds = lines_tables[2]
spread_odds_cont = lines_tables[3]
total_line = lines_tables[4]
total_line_cont = lines_tables[5]

spread_odds_total = pd.concat([spread_odds, spread_odds_cont], ignore_index=True)
total_line_total = pd.concat([total_line, total_line_cont], ignore_index=True)

spread_odds_total.to_csv('Daily Spread Odds.csv')
total_line_total.to_csv('Daily Total Line.csv')

#Cleaning Odds Data (First team listed is home team)
df_spread = pd.read_csv('Daily Spread Odds.csv')
df_spread_cleaned = df_spread.iloc[0:, 1:]
df_spread_cleaned.to_csv('Cleaned Daily Spread Odds.csv', index=False, header=False)

df_total = pd.read_csv('Daily Total Line.csv')
df_total_cleaned = df_total.iloc[0:, 1:]
df_total_cleaned.to_csv('Cleaned Daily Total Line.csv', index=False, header=False)

#Adjusting csv files (getting rid of 'vs.' and 'at' so data is easier to work with)
df_spread_final = pd.read_csv('Cleaned Daily Spread Odds.csv', header=None, names=["Matchup", "Line"])
df_spread_final['Matchup'] = df_spread_final['Matchup'].str.replace(" vs. ", ",").str.replace(" at ", ",")
df_spread_final[['Team1', 'Team2']] = df_spread_final['Matchup'].str.split(',', expand=True)
df_spread_final = df_spread_final[['Team1', 'Team2', 'Line']]
df_spread_final.to_csv('Final Daily Spread Data.csv', index=False, header=False)

df_total_final = pd.read_csv('Cleaned Daily Total Line.csv', header=None, names=['Matchup', 'Line'])
df_total_final['Matchup'] = df_total_final['Matchup'].str.replace(" vs. ", ",").str.replace(" at ", ",")
df_total_final[['Team1', 'Team2']] = df_total_final['Matchup'].str.split(',', expand=True)
df_total_final = df_total_final[['Team1', 'Team2', 'Line']]
#df_total_final1 = df_total_final.iloc[1:]
df_total_final.to_csv('Final Daily Total Lines.csv', index=False, header=False)

matchups_spread = pd.read_csv('Final Daily Spread Data.csv', header=None, names=['Team1', 'Team2', 'Line'])
matchups_total = pd.read_csv('Final Daily Total Lines.csv', header=None, names=['Team1', 'Team2', 'Line'])

#Funtion to analyze single matchup for spread
advice_spread = []
def analyze_matchup_spread(team1, team2, line):
    stats1 = scoring.get(team1.strip(), None)  #is the combination of offensive stats for team 1
    stats2 = scoring.get(team2.strip(), None)  #is the combination of offensive stats for team 2
    stats3 = defense.get(team1.strip(), None)  #is the combination of defensive stats for team 1
    stats4 = defense.get(team2.strip(), None)  #is the combination of defensive stats for team 2
    if stats1 is None or stats2 is None or stats3 is None or stats4 is None:
        return f"Stats not found for matchup {team1} vs. {team2}"
    #Algorithm
    predicted_margin = (float(stats1['PPG'])/4 + float(stats1['Last 3'])/6 + float(stats1['Last 1'])/12 + float(stats4['OPP_PPG'])/4 + float(stats4["Last 3"])/6 + float(stats4['Last 1'])/12) - (float(stats2['PPG'])/4 + float(stats2['Last 3'])/6 + float(stats2['Last 1'])/12 + float(stats3['OPP_PPG'])/4 + float(stats3['Last 3'])/6 + float(stats3['Last 1'])/12)
    advice = f"Bet on {team1}" if predicted_margin > abs(float(line)) else f"Bet on {team2}"
    return f"{team1} vs. {team2}: {advice} (Predicted margin: {predicted_margin}, Line: {line})"

#Run analysis for each game
for _, row in matchups_spread.iterrows():
    team1, team2, line = row['Team1'], row['Team2'], row['Line']
    result = analyze_matchup_spread(team1, team2, line)
    advice_spread.append(result)

#Now do same for total line
advice_totals = []
def analyze_matchup_total(team1, team2, line):
    stats1 = scoring.get(team1.strip(), None)  # is the combination of offensive stats for team 1
    stats2 = scoring.get(team2.strip(), None)  # is the combination of offensive stats for team 2
    stats3 = defense.get(team1.strip(), None)  # is the combination of defensive stats for team 1
    stats4 = defense.get(team2.strip(), None)  # is the combination of defensive stats for team 2
    if stats1 is None or stats2 is None or stats3 is None or stats4 is None:
        return f"Stats not found for matchup {team1} vs. {team2}"
    #Algorithm
    predicted_total = (float(stats1['PPG']) / 4 + float(stats1['Last 3']) / 6 + float(stats1['Last 1']) / 12 + float(stats4['OPP_PPG']) / 4 + float(stats4["Last 3"]) / 6 + float(stats4['Last 1']) / 12) + (float(stats2['PPG']) / 4 + float(stats2['Last 3']) / 6 + float(stats2['Last 1']) / 12 + float(stats3['OPP_PPG']) / 4 + float(stats3['Last 3']) / 6 + float(stats3['Last 1']) / 12)
    advice_total = "Bet the over" if predicted_total > float(line) else "Bet the under"
    return f"{team1} vs. {team2}: {advice_total} (Projected pick: {predicted_total}, Line: {line})"

#Run analysis for each game
for _, row in matchups_total.iterrows():
    team1, team2, line = row['Team1'], row['Team2'], row['Line']
    result = analyze_matchup_total(team1, team2, line)
    advice_totals.append(result)

#Combine betting advice
combined_advice = {}
for entry in advice_spread:
    matchup, advice = entry.split(':', 1)
    combined_advice[matchup] = [advice]

for entry in advice_totals:
    matchup, advice = entry.split(':', 1)
    if matchup in combined_advice:
        combined_advice[matchup].append(advice)
    else:
        combined_advice[matchup] = [advice]

#combined lists
advice_formatted = []
for matchup, advices in combined_advice.items():
    combined_ad = '; '.join(advices)
    advice_formatted.append(f'{matchup}: {combined_ad}')

#for result in advice_formatted:
    #print(result)

#NBA game results
url_results = 'https://www.basketball-reference.com/leagues/NBA_2025_games-january.html'
table_results = pd.read_html(url_results)
results = table_results[0]
results.to_csv('NBA Game Results.csv')

#Change to dictionary (easier to input into database)
parsed_advice = []

for advice in advice_formatted:
    matchup, spread_info, total_info = advice.split(':', 1)[0], advice.split(';', 1)[0], advice.split(';', 1)[1]

    spread_advice = spread_info.split(' (')[0]
    spread_ad = spread_advice.split(':')[1]
    spread_details = spread_info.split(' (')[1].strip(')')
    predicted_margin = spread_details.split(', ')[0].split(': ')[1]
    spread_line = spread_details.split(', ')[1].split(': ')[1]

    total_advice = total_info.split(' (')[0]
    total_details = total_info.split(' (')[1].strip(')')
    total_lin = total_details.split(', ')[1].split(': ')[1]
    projected_total = total_details.split(',')[0],
    parsed_advice.append({
        'Matchup': matchup.strip(),
        'Spread Bet': spread_ad.strip(),
        'Predicted Margin': float(predicted_margin),
        'Spread Line': float(spread_line),
        'Total Advice': total_advice.strip(),
        'Projected Total': projected_total,
        'Total Line': float(total_lin)
    })


# Create an embedded dictionary
embedded_dict = {}

# Iterate over the list of dictionaries
for game in parsed_advice:
    matchup = game['Matchup']  # Access 'Matchup' key in the dictionary
    team1, team2 = matchup.split(' vs. ')  # Split into two teams
    embedded_dict[f"{team1} vs. {team2}"] = {
        "Spread Bet": game['Spread Bet'],
        "Predicted Margin": game['Predicted Margin'],
        "Spread Line": game['Spread Line'],
        "Total Advice": game['Total Advice'],
        "Projected Total": game['Projected Total'][0],  # Unpack the tuple
        "Total Line": game['Total Line']
    }

#Add date (Will be for the day code run)
for matchup, details in embedded_dict.items():
    details['Date'] = datetime.today().strftime('%Y-%m-%d')

matchup_keys = list(embedded_dict.keys())

#Get individual teams
for match in matchup_keys:
    teams = match.split(' vs. ')
    embedded_dict[match]['Team 1'] = teams[0]
    embedded_dict[match]['Team 2'] = teams[1]

print(embedded_dict.items())
print(matchup_keys)
print(embedded_dict[matchup_keys[0]]['Spread Bet'])

#Connect to SQL Database
conn = sqlite3.connect('nba_betting_project1.db')
c = conn.cursor()

#Create table

c.execute('''
    CREATE TABLE IF NOT EXISTS predictions4 (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_date DATE,
        team1 TEXT, 
        team2 TEXT, 
        spread_line NUMERIC, 
        spread_prediction TEXT,
        total_line NUMERIC, 
        total_prediction TEXT
    
    )
''')

for matchup, details in embedded_dict.items():
    c.execute('''
        INSERT OR REPLACE INTO predictions4 (
            game_date, team1, team2, spread_line, spread_prediction, total_line, total_prediction
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        details['Date'],
        details['Team 1'],
        details['Team 2'],
        details['Spread Line'],
        details['Spread Bet'],
        details['Total Line'],
        details['Total Advice']

    ))

#Create results table
c.execute('''
    CREATE TABLE IF NOT EXISTS Results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        team1 TEXT,
        team2 TEXT,
        spread_result TEXT,
        total_result TEXT
        
    )
''')


conn.commit()
conn.close()


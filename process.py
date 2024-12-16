import sqlite3
import csv
import plotly.graph_objects as go #type: ignore
import csv

conn = sqlite3.connect('./db/final_new.db')
cur = conn.cursor()

def process_db(years, teams):
    '''
    Process the data from the database to find how much each team spends
    and the amount of goals/points score per year.
    '''
    with open('processed_data.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Year', 'Team', 'Points/Expense', 'Goals/Expense'])
        for year in years:
            for team in teams:
                result = cur.execute(
                    '''
                    SELECT f.expenses, s.wins, s.points, s.goals_scored
                    FROM transfer_fee f
                    JOIN transfer_fee_teams tt ON f.tid = tt.id
                    JOIN Teams t ON tt.team = t.team_name
                    JOIN TeamStatistics s ON t.team_id = s.tid
                    WHERE tt.team = ? AND f.year = ? AND s.year = ?;
                    ''',
                    (team, year, year)
                ).fetchone()
                if result:
                    expense = result[0]
                    points = result[2]
                    goals = result[3]

                    pe = 0
                    ge = 0
                    if expense:
                        pe = round(points / expense * 1000000, 4)
                        ge = round(goals / expense * 1000000, 4)
                    
                    writer.writerow([team, year, pe, ge])


    teams = []
    years = []
    performance_metrics = {}

    with open('processed_data.csv', mode='r') as file:
        reader = csv.reader(file)
        next(reader) 
        for row in reader:
            team, year, pe, ge = row
            if team not in performance_metrics:
                performance_metrics[team] = {'years': [], 'pe': [], 'ge': []}
            performance_metrics[team]['years'].append(int(year))
            performance_metrics[team]['pe'].append(float(pe))
            performance_metrics[team]['ge'].append(float(ge))

    return performance_metrics

def visual_one(data):
    '''
    Produces a line graph for points vs expenses.
    '''
    fig = go.Figure()

    for team, data in data.items():
        fig.add_trace(go.Scatter(
            x=data['years'],
            y=data['pe'],
            mode='markers+lines',
            name=team,
            text=[f'({year}, {metric})' for year, metric in zip(data['years'], data['pe'])],
            hoverinfo='text',
        ))

    fig.update_layout(
        title='Points/Expense Ratio 2020-2022',
        xaxis_title='Year',
        yaxis_title='Points/Expense (scaled by 1 million)',
        xaxis=dict(tickvals=[2020, 2021, 2022]),
        yaxis=dict(tickmode='linear', tick0=0, dtick=1),
        showlegend=True
    )

    fig.show()

def visual_two(data):
    '''
    Produces a bar graph for goals vs expenses.
    '''
    fig = go.Figure()

    for team, data in data.items():
        fig.add_trace(go.Bar(
            x=data['years'],
            y=data['ge'],
            name=team,
            text=[f'({year}, {metric})' for year, metric in zip(data['years'], data['ge'])],
            hoverinfo='text',
        ))

    fig.update_layout(
        title='Goals/Expense Ratio 2020-2022',
        xaxis_title='Year',
        yaxis_title='Wins/Expense (scaled by 1 million)',
        xaxis=dict(tickvals=[2020, 2021, 2022]),
        yaxis=dict(tickmode='linear', tick0=0, dtick=1),
        showlegend=True
    )

    fig.show()

def main():
    years = [2020, 2021, 2022]
    teams = ["Manchester City", "Tottenham", "Liverpool", "Barcelona", "Real Madrid", "Manchester United", "Villarreal"] # Can always add more teams
    data = process_db(years, teams)
    visual_one(data)
    visual_two(data)

if __name__ == '__main__':
    main()


#!/usr/bin/env python3

### Copyright 2020 Robinson Sablons De Gelis

from google.cloud import bigquery
from google.oauth2 import service_account

import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

date_format = mdates.DateFormatter('%b, %d %Y')

# TODO(developer): Set key_path to the path to the service account key
#                  file.
key_path = "blacknut-analytics-7488e0e6d73d.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id,
)


###Time variables (YYYY, MM, DD)
since_date = datetime.date(2020, 6, 1)
end_date = datetime.date(2020, 7, 25)

print("Results between " + since_date.strftime("%Y-%m-%d") + " and " + end_date.strftime("%Y-%m-%d") + ": ")

###Query
query = "SELECT user, created_at, status, game__global_id FROM external_share.streams WHERE status = 'ended' AND created_at >= '"+ since_date.strftime("%Y-%m-%d") + " 00:00:00.000 UTC' AND created_at <='"+ end_date.strftime("%Y-%m-%d") + " 23:59:59.000 UTC'"
query_job = client.query(query)
query_daily = query_job.to_dataframe()


###Number of sessions
nb_sessions = len(query_daily)
print("number of sessions: ", nb_sessions)

###Map (User, (Date, Game))
map_users = {}

for i in range(0, len(query_daily)):
    if query_daily["user"][i] in map_users:
        if  query_daily["created_at"][i].strftime("%Y:%m:%d") in map_users[query_daily["user"][i]]:
            map_users[query_daily["user"][i]][query_daily["created_at"][i].strftime("%Y:%m:%d")].append(query_daily["game__global_id"][i])
        else:
            map_users[query_daily["user"][i]][query_daily["created_at"][i].strftime("%Y:%m:%d")] = []
            map_users[query_daily["user"][i]][query_daily["created_at"][i].strftime("%Y:%m:%d")].append(query_daily["game__global_id"][i])
    else:
        map_users[query_daily["user"][i]] = {}
        map_users[query_daily["user"][i]][query_daily["created_at"][i].strftime("%Y:%m:%d")] = []
        map_users[query_daily["user"][i]][query_daily["created_at"][i].strftime("%Y:%m:%d")].append(query_daily["game__global_id"][i])

###Map (date, number of sessions)
map_dates_nb = {}
###Map (date, name of users)
map_dates_users = {}
tab_dates = []
for i in range(0, len(query_daily)):
    if query_daily["created_at"][i].strftime("%Y:%m:%d") in map_dates_nb:
        map_dates_nb[query_daily["created_at"][i].strftime("%Y:%m:%d")] += 1
        if query_daily["user"][i] not in map_dates_users[query_daily["created_at"][i].strftime("%Y:%m:%d")]:
            map_dates_users[query_daily["created_at"][i].strftime("%Y:%m:%d")].append(query_daily["user"][i])
    else:
        tab_dates.append(query_daily["created_at"][i].year)
        map_dates_nb[query_daily["created_at"][i].strftime("%Y:%m:%d")] = 1
        map_dates_users[query_daily["created_at"][i].strftime("%Y:%m:%d")] = []
        map_dates_users[query_daily["created_at"][i].strftime("%Y:%m:%d")].append(query_daily["user"][i])

###Number of unique users
nb_unique_users = len(map_users)
print("number of unique users: ", nb_unique_users)

###Frequence of play by users
tab_frequence_of_play = []
for user in map_users:
    if(len(map_users[user]) > 7): #Only users with at least 8 days of play are taken into account, can be adjusted according to when you consider it significant.
        first_date = datetime.datetime.strptime(next(iter(sorted(map_users[user]))), "%Y:%m:%d").date()
        last_date = datetime.datetime.strptime(next(iter(sorted(map_users[user], reverse=True))), "%Y:%m:%d").date()
        diff_date = abs((last_date - first_date).days) + 1
        tab_frequence_of_play.append(len(map_users[user])/diff_date)

if(len(tab_frequence_of_play) > 0):
    print("mean of frequence of play by users: ", np.mean(tab_frequence_of_play))
    print("standart deviation: ", np.std(tab_frequence_of_play))


###Tabs in order to have the dates on the plots
dates = []
dates_week = []
dates_month = []

###mean of number of session
tab_times_session = []
for date in sorted (map_dates_nb) :
    tab_times_session.append(map_dates_nb[date])
    dates.append(datetime.datetime.strptime(date, "%Y:%m:%d").date())
    if datetime.datetime.strptime(date, "%Y:%m:%d").date().day == 1:
        dates_month.append(datetime.datetime.strptime(date, "%Y:%m:%d").date())
    if (datetime.datetime.strptime(date, "%Y:%m:%d").date() - since_date).days % 7 == 0:
        dates_week.append(datetime.datetime.strptime(date, "%Y:%m:%d").date())
print("mean of number of session: ", np.mean(tab_times_session))
print("standart deviation: ", np.std(tab_times_session))

print("nb weeks = ", len(dates_week))

#in order to make the plots more readable
if len(dates_month) <= 2:
    dates_month = dates

#plot
fig, ax = plt.subplots()
ax.plot(dates, tab_times_session)
plt.xticks(dates_month)
plt.gcf().autofmt_xdate() 
plt.gca().xaxis.set_major_formatter(date_format)
ax.set(xlabel='date', ylabel='number of sessions',
       title='number of sessions per day')
ax.grid()
plt.show()
#end

###mean of number of unique user
tab_times_user = []
tab_times_week = []
tab_times_start = []
tab_times_end = []
day = 1
tab_week_tmp = []

tab_times_bis_week = []
daybis = 1
tab_week_bis_tmp = []



nb_day = 0 ######
map_weeks = {}
for i in range(0, len(dates_week)):
        map_weeks[i] = [] 
map_weeks_recur = {}
for i in range(0, len(dates_week)):
        map_weeks_recur[i] = [] 
map_weeks_prop = {}
for i in range(0, len(dates_week)):
        map_weeks_prop[i] = [] 


for date in sorted (map_dates_users) :
    #weekly
    if day == 7:
        for user in map_dates_users[date]:
            if (user not in tab_week_tmp):
                tab_week_tmp.append(user)
        tab_times_week.append(len(tab_week_tmp))
        tab_week_tmp = []
        day = 1    
    else: 
        for user in map_dates_users[date]:
            if (user not in tab_week_tmp):
                tab_week_tmp.append(user)
        day += 1
    #bis weekly
    if daybis == 14:
        for user in map_dates_users[date]:
            if (user not in tab_week_bis_tmp):
                tab_week_tmp.append(user)
        tab_times_bis_week.append(len(tab_week_bis_tmp))
        tab_week_bis_tmp = []
        daybis = 1    
    else: 
        for user in map_dates_users[date]:
            if (user not in tab_week_bis_tmp):
                tab_week_bis_tmp.append(user)
        daybis += 1
    
    #user who come back
    for user in map_dates_users[date]:
        found = False
        nbfound = 0
        found_only_in_init = False
        for i in range(0, (nb_day//7) +1):
            if user in map_weeks[i]: 
                found = True
            if user in map_weeks_recur[i]:
                nbfound += 1
            if i == 0:
                if user in map_weeks_prop[i]:
                    found_only_in_init = True
            if i != 0:
                if user in map_weeks_prop[i]:
                    found_only_in_init = False

        if not found:
            map_weeks[nb_day//7].append(user)
        """
        if nbfound >= (nb_day//7)//2 and nbfound <= (nb_day//7):
            if user not in map_weeks_recur[nb_day//7]:
        """
        if nbfound == (nb_day//7):
            map_weeks_recur[nb_day//7].append(user)
        if found_only_in_init or nb_day < 7:
            if user not in map_weeks_prop[nb_day//7]:
                map_weeks_prop[nb_day//7].append(user)
    nb_day += 1
    

    #daily
    tab_times_user.append(len(map_dates_users[date]))
    if since_date.year != end_date.year:
        if(date.startswith(str(end_date.year))):
            tab_times_end.append(len(map_dates_users[date]))
        elif (date.startswith(str(since_date.year))):
            tab_times_start.append(len(map_dates_users[date]))

# tab of new user each weeks
user_each_weeks = []
for i in range(0, len(dates_week)):
    user_each_weeks.append(len(map_weeks[i]))

# tab of user that came back every weeks
user_recur_each_weeks = []
for i in range(0, len(dates_week)):
    user_recur_each_weeks.append(len(map_weeks_recur[i]))

# tab with cumulative proportions of user who came back
user_prop_each_weeks = []
for i in range(0, len(dates_week)):
    user_prop_each_weeks.append(0)
    if i >= 2:
        for j in range(1, i+1):
            user_prop_each_weeks[i] += len(map_weeks_prop[j])
    else:
        user_prop_each_weeks[i] += len(map_weeks_prop[i])
    user_prop_each_weeks[i] = round(user_prop_each_weeks[i] / len(map_weeks_prop[0]) * 100, 1)

print(user_prop_each_weeks)

#Plot of mean of number of user per day if multiple years
if since_date.year != end_date.year:
    print("mean of number of unique user in " + str(since_date.year) + ": ", np.mean(tab_times_start))
    print("mean of number of unique user in " + str(end_date.year) + ": ", np.mean(tab_times_end))
    fig, ax = plt.subplots()    
    ax.bar([str(since_date.year),str(end_date.year)],[np.mean(tab_times_start), np.mean(tab_times_end)])
    ax.set(xlabel='year', ylabel='number of user per day',
       title='mean of number of user')
    ax.grid()
    plt.show()

#Plots
#number of users per day
fig, ax = plt.subplots()
ax.plot(dates, tab_times_user) 
plt.xticks(dates_month)
plt.gcf().autofmt_xdate()
plt.gca().xaxis.set_major_formatter(date_format)
ax.set(xlabel='date', ylabel='number of users',
       title='number of users per day')
ax.grid()
plt.show()

print("mean of number of unique user per day since " + since_date.strftime("%Y-%m-%d") + ": ", np.mean(tab_times_user))
print("standart deviation: ", np.std(tab_times_user))
print("mean of number of unique user per week since " + since_date.strftime("%Y-%m-%d") + ": ", np.mean(tab_times_week))
print("standart deviation: ", np.std(tab_times_week))

#mean of number of unique users per day/week/two_week/period
if(len(dates_week) >= 2):
    fig, ax = plt.subplots()
    bars = ax.bar(["per day","per week", "per two weeks", "full period"],[np.mean(tab_times_user), np.mean(tab_times_week), np.mean(tab_times_bis_week), len(map_users)])
    ax.set(xlabel= since_date.strftime("%Y-%m-%d") + " - " + end_date.strftime("%Y-%m-%d"), ylabel='number of user per day', title='mean of number of unique users')
    ax.grid()
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x(), yval + .005, round(yval,1))
    plt.show()

#number of new users per week
fig, ax = plt.subplots()
bars = ax.bar(dates_week, user_each_weeks,6)
plt.xticks(dates_week)
ax.set(xlabel= since_date.strftime("%Y-%m-%d") + " - " + end_date.strftime("%Y-%m-%d"), ylabel='number of user', title='number of new users per week')
plt.gcf().autofmt_xdate()
plt.gca().xaxis.set_major_formatter(date_format)
ax.grid()
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x(), yval + .005, yval)
plt.show()

#number of user already present every past week
fig, ax = plt.subplots()    
bars = ax.bar(dates_week, user_recur_each_weeks,6)
plt.xticks(dates_week)
ax.set(xlabel= since_date.strftime("%Y-%m-%d") + " - " + end_date.strftime("%Y-%m-%d"), ylabel='number of user', title='number of user already present every past week')
plt.gcf().autofmt_xdate()
plt.gca().xaxis.set_major_formatter(date_format)
ax.grid()
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x(), yval + .005, yval)
plt.show()

# proportion of first week users who came back since the beginning
fig, ax = plt.subplots()    
bars = ax.bar(dates_week, user_prop_each_weeks,6)
plt.xticks(dates_week)
ax.set(xlabel= since_date.strftime("%Y-%m-%d") + " - " + end_date.strftime("%Y-%m-%d"), ylabel='number of user', title='proportion of first week users who came back since the beginning')
plt.gcf().autofmt_xdate()
plt.gca().xaxis.set_major_formatter(date_format)
ax.grid()
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x(), yval + .005, yval)
plt.show()
#end



###mean of number of sessions per day per user
game_freq = []
for user in map_users:
    freq = 0.0
    for date in map_users[user]:
        freq+= len(map_users[user][date])
    freq /= len(map_users[user])
    game_freq.append(freq)
mean_game_freq = np.mean(game_freq)
print("mean of number of sessions per day per user since " + since_date.strftime("%Y-%m-%d") + ": ", mean_game_freq)
print("standart deviation: ", np.std(game_freq))
#end

###mean of number of different game played every day by a user
number_diff_game = []
for user in map_users:
    tmp_number = []
    for date in map_users[user]:
        tmp = []
        for game in map_users[user][date]:
            if game not in tmp:
                tmp.append(game)
        tmp_number.append(len(tmp))
    number_diff_game.append(np.mean(tmp_number))
mean_diff_game = np.mean(number_diff_game)
print("mean of number of different games played per day per user since " + since_date.strftime("%Y-%m-%d") + ": ", mean_diff_game)
print("standart deviation: ", np.std(number_diff_game))
#end

###mean of number of different game played by users
number_diff_games = []
for user in map_users:
    tmp_number_games = []
    for date in map_users[user]:
        for game in map_users[user][date]:
            if game not in tmp_number_games:
                tmp_number_games.append(game)
    number_diff_games.append(len(tmp_number_games))
mean_diff_games = np.mean(number_diff_games)
print("mean of number of different games played per user since " + since_date.strftime("%Y-%m-%d") + ": ", mean_diff_games)
print("standart deviation: ", np.std(number_diff_games))
#end

###number of user who came only one time
only_one_time = 0
day_played = []
for user in map_users:
    day_played.append(len(map_users[user]))
    if len(map_users[user]) == 1:
        only_one_time += 1

print("proportion of users who only came once: ", round(only_one_time/len(map_users)*100, 1) , "%")
fig, ax = plt.subplots()
bars = ax.bar(["number of unique user","number of users who only came once"],[len(map_users), only_one_time])
ax.set(xlabel= since_date.strftime("%Y-%m-%d") + " - " + end_date.strftime("%Y-%m-%d"), ylabel='number of users', title='Proportion of users who only came once')
ax.grid()
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x(), yval + .005, yval)
plt.show()
print("Mean of number of day played by users since " + since_date.strftime("%Y-%m-%d") + ": ", np.mean(day_played))
print("standart deviation: ", np.std(day_played))
#end

#final end

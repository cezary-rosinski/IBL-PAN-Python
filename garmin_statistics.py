from garminconnect import (
    Garmin,
    GarminConnectConnectionError,
    GarminConnectTooManyRequestsError,
    GarminConnectAuthenticationError,
)

from datetime import date
import pandas as pd
import logging
from garmin_credentials import garmin_password, garmin_username
import datetime

logging.basicConfig(level=logging.DEBUG)

first_day = date(2020, 9, 22)
today = date.today()

try:
    client = Garmin(garmin_username, garmin_password)
except (
    GarminConnectConnectionError,
    GarminConnectAuthenticationError,
    GarminConnectTooManyRequestsError,
) as err:
    print("Error occurred during Garmin Connect Client init: %s" % err)
    quit()
except Exception:  # pylint: disable=broad-except
    print("Unknown error occurred during Garmin Connect Client init")
    quit()

try:
    client.login()
except (
    GarminConnectConnectionError,
    GarminConnectAuthenticationError,
    GarminConnectTooManyRequestsError,
) as err:
    print("Error occurred during Garmin Connect Client login: %s" % err)
    quit()
except Exception:  # pylint: disable=broad-except
    print("Unknown error occurred during Garmin Connect Client login")
    quit()

limit = (today - first_day).days

activities = client.get_activities(0, limit)
act = [[e['activityId'], e['startTimeLocal'], e['distance'], e['averageHR']] for e in activities if e['activityType']['typeKey'] == 'running']

for i, el in enumerate(act):
    hr_timezones = client.get_activity_hr_in_timezones(el[0])
    total_time = sum([e['secsInZone'] for e in hr_timezones])
    act[i].append(total_time)
    zones_1_2 = sum([e['secsInZone'] for e in hr_timezones if e['zoneNumber'] in [1,2]])/total_time
    zones_1_2_time = sum([e['secsInZone'] for e in hr_timezones if e['zoneNumber'] in [1,2]])
    act[i].append(zones_1_2)
    act[i].append(zones_1_2_time)
    
df = pd.DataFrame(act, columns=['activityId', 'date', 'distance', 'averageHR', 'total time', 'zones 1 and 2 %', 'zones 1 and 2 time'])

df['date'] = df['date'].apply(lambda x: x[:10])
df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

# df['date'] = pd.to_datetime(df['startTimeLocal'], format='%Y-%m-%d %H:%M:%S')
df['s/km'] = df['total time']*1000/df['distance']

df.loc[df['activityId'] == 7110865728, 'zones 1 and 2 %'] = 0.93

def rosinski_hubar_index(x):
    return ((1-(x['averageHR']/x['s/km']))+(x['total time']/x['averageHR']/300)+(x['zones 1 and 2 %']))*(x['distance']/10000)
    # return ((1-(x['averageHR']/x['s/km']))/(x['averageHR']/100)+(x['total time']/x['averageHR']/500)+(x['zones 1 and 2 %']))*(x['distance']/10000)
    
df['rosinski-hubar index'] = df.apply(lambda x: rosinski_hubar_index(x), axis=1)

# df.to_excel('running.xlsx', index=False)

fig = df.groupby('date').sum()['rosinski-hubar index'].plot(figsize = (40,10), title='running', legend=True, grid=True, lw=4).get_figure()
fig.savefig('running.jpg')


#%% linear regression
# import numpy as np
# import pandas as pd
# import datetime
# from sklearn import linear_model
# from matplotlib import pyplot as plt

# lr_df = df.copy()[['date', 'rosinski-hubar index']]
# lr_df['date'] = pd.to_datetime(lr_df['date'].apply(lambda x: x[:10]), format='%Y-%m-%d').dt.date
# lr_df = lr_df.iloc[::-1].reset_index(drop=True)
# lr_df = lr_df.iloc[56:,]
# lr_df = lr_df.set_index('date')
# lr_df.columns = ["value"]
# lr_df['days_from_start'] = (lr_df.index - lr_df.index[0]).days; lr_df

# x = lr_df['days_from_start'].values.reshape(-1, 1)
# y = lr_df['value'].values

# model = linear_model.LinearRegression().fit(x, y)
# linear_model.LinearRegression(copy_X=True, fit_intercept=True, n_jobs=1, normalize=False)
# pred = np.asarray(range(65,200)).reshape(-1, 1)
# prediction = model.predict(pred).reshape(-1, 1)
# model.predict([[65], [66], [67], [100]])

# plt.scatter(x, y,color='g')
# plt.plot(x, prediction,color='k')

# plt.savefig('test.png')


# prediction = model.predict(np.sort(x, axis=0))

# plt.scatter(x, y)
# plt.plot(np.sort(x, axis=0),prediction)














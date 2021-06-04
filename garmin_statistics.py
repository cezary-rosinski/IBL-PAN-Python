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

logging.basicConfig(level=logging.DEBUG)

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


activities = client.get_activities(0, 1000)
act = [[e['activityId'], e['startTimeLocal'], e['distance'], e['averageHR']] for e in activities if e['activityType']['typeKey'] == 'running']

for i, el in enumerate(act):
    hr_timezones = client.get_activity_hr_in_timezones(el[0])
    total_time = sum([e['secsInZone'] for e in hr_timezones])
    act[i].append(total_time)
    zones_1_2 = sum([e['secsInZone'] for e in hr_timezones if e['zoneNumber'] in [1,2]])/total_time
    act[i].append(zones_1_2)
    
df = pd.DataFrame(act, columns=['activityId', 'date', 'distance', 'averageHR', 'total time', 'zones 1 and 2 %'])

# df['date'] = df['startTimeLocal'].apply(lambda x: x[:10])
# df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

# df['date'] = pd.to_datetime(df['startTimeLocal'], format='%Y-%m-%d %H:%M:%S')
df['s/km'] = df['total time']*1000/df['distance']

def rosinski_hubar_index(x):
    return ((1-(x['averageHR']/x['s/km']))+(x['total time']/x['averageHR']/300)+(x['zones 1 and 2 %']))*(x['distance']/10000)
    
df['rosinski-hubar index'] = df.apply(lambda x: rosinski_hubar_index(x), axis=1)

df.to_excel('running.xlsx', index=False)

fig = df.groupby('date').sum()['rosinski-hubar index'].plot(figsize = (40,10), title='running', legend=True, grid=True, lw=4).get_figure()
fig.savefig('running.jpg')


























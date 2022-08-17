#%%import
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

#%% data cleaning
df = pd.read_csv(r"C:\Users\Cezary\Downloads\Activities.csv", decimal=',')
df = df[df['Activity Type'] == 'Running']
df = df[['Date', 'Distance','Calories','Avg HR','Avg Run Cadence', 'Avg Pace', 'Best Pace', 'Avg Stride Length', 'Elapsed Time']]

#convert 'Date', 'Avg Pace', 'Best Pace', 'Elapsed Time' objects to datetime format
df['Date'] = pd.to_datetime(df['Date'])
df['Avg Pace'] = pd.to_datetime(df['Avg Pace'], format='%M:%S')
df['Best Pace'] = pd.to_datetime(df['Best Pace'], format='%M:%S')
df['Elapsed Time'] = pd.to_datetime(df['Elapsed Time'])
#convert 'Avg Pace', 'Best Pace', 'Elapced Time' objects to the number of minutes
df['Avg Pace'] = df['Avg Pace'].dt.hour*60 + df['Avg Pace'].dt.minute + df['Avg Pace'].dt.second/60
df['Best Pace'] = df['Best Pace'].dt.hour*60 + df['Best Pace'].dt.minute + df['Best Pace'].dt.second/60
df['Elapsed Time'] = df['Elapsed Time'].dt.hour*60 + df['Elapsed Time'].dt.minute + df['Elapsed Time'].dt.second/60
#add 'Avg Speed' and 'Best Speed' columns
df['Avg Speed'] = 60 / df['Avg Pace']
df['Best Speed'] = 60 / df['Best Pace']
s = df.select_dtypes(include='object').columns
df[s] = df[s].astype('float')

#%% data analysis
plt.figure(figsize=(14,6))
sns.histplot(data = df, x='Avg HR').set(title = 'Average HR Distribution')

sns.jointplot(x='Avg Speed',y='Avg Run Cadence', data=df.dropna(),kind='scatter')
print("The correlation coefficient between cadence and speed:", df['Avg Speed'].corr(df['Avg Run Cadence']))

sns.jointplot(x='Avg Speed',y='Avg Stride Length', data=df.dropna(),kind='scatter')
print("The correlation coefficient between stride length and speed:", df['Avg Speed'].corr(df['Avg Stride Length']))

sns.jointplot(x='Avg Run Cadence',y='Avg Stride Length', data=df.dropna(),kind='scatter')
print("The correlation coefficient between stride length and run cadence:", df['Avg Run Cadence'].corr(df['Avg Stride Length']))

plt.figure(figsize=(14,6))
#add extra column with month for every running session
df['Month'] = df['Date'].dt.strftime('%b') + " " + df['Date'].dt.strftime('%Y')
#sort datataset by date in ascending order
df.sort_values(by='Date', inplace=True)
#plot boxplots grouped by month
sns.boxplot(x='Month',y='Avg Speed', palette=["m", "g"], data=df.dropna()).set(title = 'Avg Speed by Month')
sns.boxplot(x='Month',y='Avg Stride Length', palette=["m", "g"], data=df.dropna()).set(title = 'Avg Stride Length by Month')
sns.boxplot(x='Month',y='Avg Run Cadence', palette=["m", "g"], data=df.dropna()).set(title = 'Avg Run Cadence by Month')


df['Count'] = 1
#aggregate data by week
dfW = df.groupby(pd.Grouper(key='Date',freq='W')).agg({'Count':'sum','Distance':'sum', 'Calories':'sum','Avg HR':'mean','Avg Run Cadence':'mean', 'Avg Speed':'mean','Best Speed':'mean', 'Avg Pace':'mean', 'Best Pace':'mean', 'Avg Stride Length':'mean', 'Elapsed Time':'mean'}).reset_index()
dfW.head()

# create figure and axis objects with subplots()
fig,ax = plt.subplots(figsize=(14,6))
# make a barplot
count = ax.bar(dfW['Date'], dfW['Count'],width=10, color='red',label='Number of Runs')
ax.bar_label(count)
# set x-axis label
ax.set_xlabel('Date')
ax.legend(loc=2)
# set y-axis label
ax.set_ylabel('Number of Runs',color='red')
# twin object for two different y-axis on the sample plot
ax2=ax.twinx()
# make a plot with different y-axis using second axis object
ax2.plot(dfW['Date'],dfW['Avg Speed'],color='blue',marker='o',label='Average Speed')
ax2.set_ylabel('Average Speed',color='blue')
ax2.legend()
plt.show()

# create figure and axis objects with subplots()
fig,ax = plt.subplots(figsize=(14,6))
# make a barplot
count = ax.bar(dfW['Date'], dfW['Count'],width=10, color='red',label='Number of Runs')
ax.bar_label(count)
# set x-axis label
ax.set_xlabel('Date')
ax.legend(loc=2)
# set y-axis label
ax.set_ylabel('Number of Runs',color='red')
# twin object for two different y-axis on the sample plot
ax2=ax.twinx()
# make a plot with different y-axis using second axis object
ax2.plot(dfW['Date'],dfW['Avg Stride Length'],color='blue',marker='o',label='Avg Stride Length')
ax2.set_ylabel('Avg Stride Length',color='blue')
ax2.legend()
plt.show()

# create figure and axis objects with subplots()
fig,ax = plt.subplots(figsize=(14,6))
# make a barplot
count = ax.bar(dfW['Date'], dfW['Count'],width=10, color='red',label='Number of Runs')
ax.bar_label(count)
# set x-axis label
ax.set_xlabel('Date')
ax.legend(loc=2)
# set y-axis label
ax.set_ylabel('Number of Runs',color='red')
# twin object for two different y-axis on the sample plot
ax2=ax.twinx()
# make a plot with different y-axis using second axis object
ax2.plot(dfW['Date'],dfW['Avg Run Cadence'],color='blue',marker='o',label='Avg Run Cadence')
ax2.set_ylabel('Avg Run Cadence',color='blue')
ax2.legend()
plt.show()























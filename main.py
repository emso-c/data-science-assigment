import pandas as pd
import os


INPUT_PATH = 'data'
OUTPUT_PATH = 'output'
WEATHER_PRE_FILENAME = "weather_20180101_20211121.csv"
WEATHER_POST_FILENAME = "weather_20201004_20211222.csv"
TARGET_PRE_FILENAME = "target_20180101_20210930.csv"
TARGET_POST_FILENAME = "target_20211001_20211223.csv"

def getInput(filename):
    return os.path.join(INPUT_PATH, filename)
def getOutput(filename):
    return os.path.join(OUTPUT_PATH, filename)

def getNulls(df):
    return df[df.isnull().any(axis=1)]
def _getPreviousTemperatureOfRow(df, row, offset=-1):
    prev_rows = df[(df['metcities_id'] == row['metcities_id']) & (df['dt'] < row['dt'])]
    prev_temps = prev_rows['temperature']
    if prev_temps.size == 0:
        return None
    if not prev_temps.iloc[-offset]:
        return _getPreviousTemperatureOfRow(df, prev_rows.iloc[-1], offset+1)
    return prev_temps.iloc[-1]
def _getNextTemperatureOfRow(df, row, offset=1):
    next_rows = df[(df['metcities_id'] == row['metcities_id']) & (df['dt'] > row['dt'])]
    next_temps = next_rows['temperature']
    if next_temps.size == 0:
        return None
    if not next_temps.iloc[-offset]:
        return _getNextTemperatureOfRow(df, next_rows.iloc[-1], offset+1)
    return next_temps.iloc[-1]
def getAvgTemperatureOfRow(df, row):
    prev_temp = _getPreviousTemperatureOfRow(df, row)
    next_temp = _getNextTemperatureOfRow(df, row)
    if prev_temp is None and next_temp is None:
        return None
    if prev_temp is None:
        return next_temp
    if next_temp is None:
        return prev_temp
    return (prev_temp + next_temp) / 2


# read csv data
wpre_df = pd.read_csv(getInput(WEATHER_PRE_FILENAME))
wpost_df = pd.read_csv(getInput(WEATHER_POST_FILENAME))
tpre_df = pd.read_csv(getInput(TARGET_PRE_FILENAME))
tpost_df = pd.read_csv(getInput(TARGET_POST_FILENAME))


# merge pre and post data
w_df = pd.concat([wpre_df, wpost_df])
t_df = pd.concat([tpre_df, tpost_df])


# update date type from datetime to string
w_df['dt'] = w_df['dt'].astype(str)
t_df['dt'] = t_df['dt'].astype(str)

nulls = getNulls(w_df)
print(nulls.count()[0])
ctr = 0
for index, row in nulls.iterrows():
    avg_temp = getAvgTemperatureOfRow(w_df, row)
    if avg_temp is None:
        continue
    w_df.loc[index, 'temperature'] = avg_temp
    ctr += 1
    if ctr % 100 == 0: print(ctr)
print(getNulls(w_df).count()[0])

# floor times
w_df['dt'] = pd.Series(pd.to_datetime(w_df.dt), name='dt').dt.floor('H')
t_df['dt'] = pd.Series(pd.to_datetime(t_df.dt), name='dt').dt.floor('H')

# merge target data
df = w_df.merge(t_df, on='dt', how='left')


# group data
gw = df.groupby(['dt', 'target', 'metcities_id']).agg({'temperature':'mean'})


gw.to_csv(getOutput('gw_final.csv'), index=False)

#print(df.head(10))
#print(df.tail(10))
#print(df.isnull().sum())
print(df[df.isnull().any(axis=1)])
print(df.head(10))
print(df.tail(10))
print("=================================================")
print("=================================================")
print("=================================================")
print("=================================================")
print("=================================================")
print("=================================================")
print(gw[gw.isnull().any(axis=1)])
print(gw.head(10))
print(gw.tail(10))
#print(gw.isnull())


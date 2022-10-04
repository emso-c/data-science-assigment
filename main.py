import pandas as pd
import os
from tqdm import tqdm

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
def _getPreviousTemperatureOfRow(df, row):
    prev_rows = df[(df['metcities_id'] == row['metcities_id']) & (df['dt'] < row['dt'])]
    prev_temps = prev_rows['temperature']
    if prev_temps.size == 0:
        return None
    if not prev_temps.iloc[-1]:
        return _getPreviousTemperatureOfRow(df, prev_rows.iloc[-1])
    return prev_temps.iloc[-1]
def _getNextTemperatureOfRow(df, row):
    next_rows = df[(df['metcities_id'] == row['metcities_id']) & (df['dt'] > row['dt'])]
    next_temps = next_rows['temperature']
    if next_temps.size == 0:
        return None
    if not next_temps.iloc[-1]:
        return _getNextTemperatureOfRow(df, next_rows.iloc[-1])
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

# floor times
w_df['dt'] = pd.Series(pd.to_datetime(w_df.dt), name='dt').dt.floor('H')
t_df['dt'] = pd.Series(pd.to_datetime(t_df.dt), name='dt').dt.floor('H')

# merge target data
df = w_df.merge(t_df, on='dt', how='left')

# fill the null temperatures with average temperature of the neighbooring dates of the same city.
nulls = getNulls(df)
for index, row in tqdm(nulls.iterrows(), total=len(nulls), desc="Bosluklar dolduruluyor..."):
    avg_temp = getAvgTemperatureOfRow(df, row)
    if avg_temp is None:
        continue
    df.loc[index, 'temperature'] = avg_temp

# group data
gw = df.groupby(['dt', 'target', 'metcities_id']).agg({'temperature':'mean'})

# save data
gw.to_csv(getOutput('weather_target_merged.csv'))

print(gw[gw.isnull().any(axis=1)])
print(gw.head(10))
print(gw.tail(10))


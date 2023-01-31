import pandas as pd
import numpy as np
from datetime import date
import glob




dataframe = pd.read_csv(r"D:\test_development\DesigoCC\test_data_testing.csv", sep = ';')

dataframe = dataframe[dataframe.DateTime != "DateTime"]
dataframe = dataframe[dataframe.iloc[:, 1] != "Data Source"]
dataframe = dataframe.drop_duplicates(subset=['DateTime', 'Data Source'])

dataframe.at[dataframe['Value'] == 'ON', 'Value'] = 1
dataframe.at[dataframe['Value'] == 'OFF', "Value"] = 0
dataframe = dataframe[dataframe['Data Source'].notna()]

points = dataframe['Data Source'].unique()

point_dic = {'Point_' + str(i + 1) : point for i, point in enumerate(points)}


dataframe['DateTime'] = pd.to_datetime(dataframe.DateTime)

dataframe['Value'] = dataframe['Value'].astype(float)

datetimes = dataframe["DateTime"].unique()

datetimes_sorted = pd.to_datetime(datetimes, format="%Y%m%d %H:%M:%S").sort_values()

df = pd.DataFrame()
df['DateTime'] = datetimes_sorted

for key, point in point_dic.items():
    point_df = dataframe.loc[dataframe['Data Source'] == point][['DateTime', 'Value']]
    point_df.rename(columns = {'Value' : key}, inplace = True)
    df = pd.merge(df, point_df, how='left', left_on='DateTime', right_on = 'DateTime', validate='m:1')


df = df.sort_values("DateTime")

df = df.fillna(0)

df['<>Date'] = pd.to_datetime(df['DateTime']).dt.date
df['Time'] = pd.to_datetime(df['DateTime']).dt.time
df.insert(0, 'Time' ,df.pop('Time'))
df.insert(0, '<>Date', df.pop('<>Date'))
df = df.drop(columns='DateTime')

point_info_list = list(point_dic.items())
point_info_df = pd.DataFrame(point_info_list)


first_row_df = pd.DataFrame(['Key            Name:Suffix                                Trend Definitions Used'])
last_row_df = pd.DataFrame([' ******************************** End of Report *********************************'])

print(point_info_df)
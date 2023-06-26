import json
import datetime
import numpy
import pandas


def reshape_df(df):

    def _compare_dates(row):
        lc = datetime.datetime.strptime(row['last_contacted'], '%Y-%m-%dT%H:%M:%S')
        lu = datetime.datetime.fromtimestamp(row['last_updated']/1000)
        return lc > lu or row['ZIP'] == "NaN"

    df['update_needed'] = df.apply(_compare_dates, axis=1)
    return df.drop(columns = ['last_contacted', 'last_updated'])


def offer(df):                  # offer_expired checking method
    def _check(row):
        od = datetime.datetime.fromisoformat(row['offer_date'])
        today = datetime.datetime.now()
        data = False
        if (today-od).days > 180:
            data = True
        else:
            data = False
        return data
    df['offer_expired'] = df.apply(_check, axis=1)


source_1 = 'personal_entries.json'
source_2 = 'billing_entries.json'
source_3 = 'sales_entries.csv'

# load instant into DataFrames
df_1 = pandas.DataFrame(json.load(open(source_1,'r',encoding='utf8')))
df_1.drop('useless_info', axis='columns', inplace=True)                 # delete the usless column
df_2 = pandas.DataFrame(json.load(open(source_2,'r',encoding='utf8')))
df_3 = pandas.read_csv(source_3, delimiter=';', quotechar='"')
df_1['PID'] = df_1['PID'].astype(numpy.int64)               # cos it was an object, and it doesn't work with diff types

whole = df_1.merge(df_2, on='PID').merge(df_3, on='PID')

# address_info data and name change + address's datas like street, city put into new columns
zip_df = pandas.DataFrame(list(whole['address_info']))
print(zip_df)
whole['address_info'] = zip_df['ZIP']
whole.rename(columns = {'address_info':'ZIP'}, inplace = True)
whole = whole.merge(zip_df, on='ZIP')

# abnormal datas change to NaN
for column in whole:
    whole[column].mask(whole[column] == "", "NaN", inplace=True)
    whole[column].mask(whole[column] == -1, "NaN", inplace=True)


whole = reshape_df(whole)
print(whole['offer_date'])
print(whole)

offer(whole)
print(whole)
whole.to_excel('output.xlsx')

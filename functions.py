# -*- coding: utf-8 -*-
"""
Created on Mon Mar 11 14:40:01 2019

@author: yyzq135
"""
import pymysql
import pandas as pd
import datetime
from datetime import timedelta


def get_data_sql(db, sql):
    try:
        conn = pymysql.connect(host='rm-wz99k1ajr8yg63575o.mysql.rds.aliyuncs.com', port=3306, user='yy_system_v1',
                               password='rB9kIcF6bFj22zoT', db=db, charset='utf8',
                               cursorclass=pymysql.cursors.DictCursor)
        cursor = conn.cursor()
        cursor.execute(sql)
        dict = cursor.fetchall()
        df = pd.DataFrame(dict)
        return df
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()
        # print('%s connect success' % db)

def refine_commercial_excel(df):
    col_num = df.shape[1]
    df_1 = df[pd.isna(df['账号']) == 0]
    if col_num > 6:
        print('这是完整的表,有{}列'.format(col_num))
        df_copy = df_1.copy()
        if '金额.1' in df_copy.columns:
            df_copy.rename(columns={
    				'发号日期': '日期',
                    '金额.1': '金额',
                    '金额': '总金额'
                    }, inplace=True)
            print('两个金额')
            print(df_copy.columns)
            df_selected = df_copy.loc[:, ['账号', 'ID', '日期', '金额']]
            return df_selected
        else:
            print('没有两个金额')
            df_copy.rename(columns={
				'发号日期': '日期',
                '金额.1': '金额'
			}, inplace=True)
            df_selected = df_copy.loc[:, ['账号', 'ID', '日期', '金额']]
            return df_selected
    else:
        df_copy = df_1.copy()
        print('表只有{}列'.format(col_num))
        df_copy.rename(columns={
				'发号日期': '日期',
                '金额.1': '金额'
			}, inplace=True)
        df_selected = df_copy.loc[:, ['账号', 'ID', '日期', '金额']]
        return df_selected
            
def summary_day(data, col_name='金额', n=1):
    time_yest = datetime.datetime.now() - timedelta(n)
    date_yest = time_yest.date()
    month_yest = time_yest.month
    day_yest = time_yest.day
    if '日期' in data.columns:
        data['月份'] = data['日期'].apply(lambda x: x.month)
        sum_all = data[col_name].sum()
        sum_day = data[data['日期'] == date_yest][col_name].sum()
        sum_month = data[data['月份'] == month_yest][col_name].sum()
        avg_day = sum_month / (day_yest)
        #print([sum_day, sum_month, avg_day, sum_all])
        return [sum_all, sum_day, sum_month, avg_day]
    else:
        return None
    
def time_format(t):
    if isinstance(t, (int, float)):
        t = datetime.datetime(1900, 1, 1) + timedelta(int(t) - 2)
        t = t.date()
    elif isinstance(t, (datetime.datetime, pd._libs.tslibs.timestamps.Timestamp)):
        t = t.date()
    elif isinstance(t, (str, datetime.date)):
        t = t
    return t

def data_format(data):
    data.reset_index(drop=True, inplace=True)
    data['日期'] = data['日期'].map(time_format)
    data['ID'] = data['ID'].map(lambda x: x.lower())
    return data

def summary_money(data, income_featrue):
    time_now = pd.datetime.now()
    time_used = time_now - pd.Timedelta('1 days')
    income_all = data[income_featrue].sum()
    print('今天是%d月%d日' % (time_used.month, time_used.day))
    month_bool = data['日期'].map(lambda x: x.month == time_used.month if not isinstance(x, str) else False)#这里需要修改if not isinstance(x, str)
    data_month = data[month_bool]
    income_month = data_month[income_featrue].sum()
    income_day_average = income_month/(time_used.day)
    day_bool = data['日期'].map(lambda x: x == time_used.date())
    data_today = data[day_bool]
    income_today = data_today[income_featrue].sum()
    return income_all, income_month, income_day_average, income_today
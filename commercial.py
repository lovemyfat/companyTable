# -*- coding: utf-8 -*-
"""
Created on Mon Mar 11 14:37:31 2019

@author: yyzq135
"""


import pandas as pd
import os
import sys
import xlsxwriter
from imp import reload
sys.path.append('C:/Users/yyzq135/Desktop/refine') 
from functions import *
import time, timeit

def get_flowmaster_income(list):
    '''
    从公司数据库里获得老年组流量主数据
    '''
    originid_list = str(tuple(list))
    sql = '''SELECT x.originid, x.nick_name, x.alias, x.service_type_info, x.flowmaster_enable, y.originid, y.ref_date, y.cumulate_user, y.new_user, \
          y.cancel_user, y.a_view, y.a_click, y.a_income, y.b_view, y.b_click, y.b_income \
          FROM dw_wxmp.wxmp_info X \
          LEFT JOIN \
          (SELECT a.originid, a.ref_date, a.cumulate_user, a.new_user, a.cancel_user, b.a_view, b.a_click, b.a_income, b.b_view, b.b_click, b.b_income \
          FROM dw_wxmp.user_increase_stat a \
          LEFT JOIN ((SELECT a.originid, a.ref_date, a.view_count AS a_view, a. click_count AS a_click,  \
          a.income/100 AS a_income, b.view_count AS b_view, b. click_count AS b_click, b.income/100 AS b_income \
          FROM (SELECT * FROM dw_wxmp.flowmaster_data_stat WHERE pos_type = 1) AS a \
          LEFT JOIN  (SELECT * FROM dw_wxmp.flowmaster_data_stat WHERE pos_type = 4) AS b \
          ON a.originid = b.originid AND a.ref_date = b.ref_date) \
          UNION \
          (SELECT b.originid, b.ref_date, a.view_count AS a_view, a. click_count AS a_click,  \
          a.income/100 AS a_income, b.view_count AS b_view, b. click_count AS b_click, b.income/100 AS b_income \
          FROM (SELECT * FROM dw_wxmp.flowmaster_data_stat WHERE pos_type = 1) AS a \
          RIGHT JOIN  (SELECT * FROM dw_wxmp.flowmaster_data_stat WHERE pos_type = 4) AS b \
          ON a.originid = b.originid AND a.ref_date = b.ref_date)) b \
          ON a.originid = b.originid AND a.ref_date = b.ref_date) Y \
          ON x.originid = y.originid \
          WHERE x.alias IN {}\
          and y.ref_date >= "2018-09-01";
          '''.format(originid_list)
    df = get_data_sql('dw_wxmp', sql)
    df['a_income'] = df['a_income'].astype('float')
    df['b_income'] = df['b_income'].astype('float')
    df['all_income'] = df['a_income'] + df['b_income']
    df_selected = df.loc[:, ['originid','nick_name', 'ref_date', 'new_user',
                             'cancel_user', 'cumulate_user', 'a_view', 'a_click', 'a_income',
                             'b_view', 'b_click', 'b_income', 'all_income']]
    print('汇总流量主数据完成')
    df_selected.rename(columns={'originid':'原始id', 'nick_name':'昵称', 'ref_date':'日期', 
                       'new_user':'新增用户', 'cancel_user':'取关用户', 'cumulate_user':'总粉丝数', 
                       'a_view':'底部广告阅读量', 'a_click':'底部广告点击量', 'a_income':'底部广告收入', 
                       'b_view':'文中广告阅读量', 'b_click':'文中广告点击量', 'b_income':'文中广告收入', 
                       'all_income':'收入合计'}, inplace=True)
    # print('升级信息有%d行%d列' % (df.shape[0], df.shape[1]))
    return df_selected

def combine_flowmaster_income(list, file_path):
    '''
    汇总日报文件夹中的所有老年组流量主表格
    '''
    df = get_flowmaster_income(list)
    os.chdir(file_path)
    excel_list = os.listdir(file_path)
    excel_li_flowmaster = [i for i in excel_list if i.find('流量') >= 0]
    data = pd.read_excel(file_path+excel_li_flowmaster[0])
    data_all = data.append(df, sort=False)
    return data_all

def combine_warmwind_income(file_path):
    '''
    汇总日报文件夹中的所有派单充值表格， 并且进行初始化（主要是统一时间格式和排序，具体的函数在另一个文件中）
    '''
    os.chdir(file_path)
    excel_list = os.listdir(file_path)
    excel_li_warnwind = [i for i in excel_list if i.find('派单') >= 0]
    data_warmwind = pd.DataFrame()
    for excel in excel_li_warnwind:
        print('汇总暖风数据，正在导入', excel)
        data = pd.read_excel(file_path + excel, sheet_name='明细表')
        data['日期'] = data['日期'].map(time_format)
        data_warmwind = data_warmwind.append(data, sort=False)
    data_warmwind.sort_values(by=['日期'], ascending=True, axis=0, inplace=True)
    return data_warmwind

def combine_novel_income(file_path):
    '''
    汇总日报文件夹中的所有小说充值表格， 并且进行初始化（主要是统一时间格式和排序，具体的函数在另一个文件中）
    '''
    os.chdir(file_path)
    excel_list = os.listdir(file_path)
    excel_li_novel = [i for i in excel_list if i.find('回本') >= 0]
    data_novel = pd.DataFrame()
    for excel in excel_li_novel:
        print('汇总小说数据，正在导入', excel)
        exl = pd.ExcelFile(file_path + excel)
        sheet_list = [i for i in exl.sheet_names if i.find('充值') >= 0 or i.find('绑定平台服务号') >= 0]
        #print(sheet_list)
        for sheet in sheet_list:
            print('正在导入', excel)
            data = exl.parse(sheet)
            col_name_income = [_ for _ in data.columns if _.find('充值') >= 0]
            if col_name_income:
                data.rename(
                        columns={col_name_income[0]: '当日充值(分成后)'},
                        inplace=True)
                data.loc[:, '类型'] = excel + sheet
                data['日期'] = data['日期'].map(time_format)
                data_copy = data.loc[:, ['日期', '昵称', 'ID', '当日充值(分成后)', '类型']].copy()
                data_novel = data_novel.append(data_copy, sort=False)
    return data_novel    

def summary_data(data_novel, data_flowmaster, data_warmwind):
    '''
    生成之前确定好的日汇总表格中需要的数据
    '''
    data = [[16289909.00], [15239889.32], [721466.42], [25643592.94], [120720.18], [42054222.12], [], [20501681.6839257], [62555903.8039257]]
    # 选出小说第一批和第二批求和
    type1_bool = data_novel['类型'].map(lambda x: x.find('一') >=0)
    data_1 = data_novel.loc[type1_bool]
    type2_bool = data_novel['类型'].map(lambda x: x.find('二') >=0)
    data_2 = data_novel.loc[type2_bool]
    # 分别计算每一个部门的日月汇总
    print('开始计算合计收入')
    novel_li = summary_day(data_novel, col_name='当日充值(分成后)')
    data[0].extend(novel_li)
    li = summary_day(data_1, col_name='当日充值(分成后)')
    data[1].extend(li)
    li = summary_day(data_2, col_name='当日充值(分成后)')
    data[2].extend(li)
    flowmaster_li = summary_day(data_flowmaster, col_name='收入合计')
    data[3].extend(flowmaster_li)
    l = [x for x in zip(novel_li, flowmaster_li)]# 投放总计需要两部分合并
    li = [x for x in map(lambda x: x[0]+x[1], l)]
    data[5].extend(li)
    warmwind_li = summary_day(data_warmwind, col_name='当天充值（分成后）')
    data[7].extend(warmwind_li)
    l = [x for x in zip(li, warmwind_li)]
    li = [x for x in map(lambda x: x[0]+x[1], l)]
    data[8].extend(li)
    return data
    
def output_exl(data, file_path):
    '''
    得到的数据之后，生成对应的excel表
    '''
    #print(data)
    print('正在输出xlsx表')
    title = [u'',u'支出',u'总收入',u'日收入',u'本月收入',u'平均日收入',u'回本率']
    buname= [u'投放小说',u'其中：投放小说第一批',u'投放小说第二批',u'投放老年号',u'投放未运营',u'投放合计',u'',u'暖风派单',u'合计']
    workbook = xlsxwriter.Workbook(file_path)
    worksheet = workbook.add_worksheet()
    
    format=workbook.add_format()
    format.set_border(1)
    
    format_title=workbook.add_format()
    format_title.set_border(1)
    format_title.set_bg_color('#cccccc')
    format_title.set_align('center')
    format_title.set_bold()
    
    format_ave=workbook.add_format()
    format_ave.set_border(1)
    format_ave.set_num_format('0.00')
    
    worksheet.write_row('A1',title,format_title)
    worksheet.write_column('A2', buname,format_title)
    worksheet.write_row('B2', data[0],format)
    worksheet.write_row('B3', data[1],format)
    worksheet.write_row('B4', data[2],format)
    worksheet.write_row('B5', data[3],format)
    worksheet.write_row('B6', data[4],format)
    worksheet.write_row('B7', data[5],format)
    worksheet.write_row('B8', data[6],format)
    worksheet.write_row('B9', data[7],format)
    worksheet.write_row('B10', data[8],format)
    
    
    for row in range(2, 11):
        worksheet.write_formula('G'+str(row), '=iferror($C'+ str(row) + '/$B' + str(row)+',"")', format_ave)
        
    workbook.close()

    
    
def get_upgrade_info(alias_list):
    '''
    从数据库得到对应一些账号升级日期
    '''
    alias_list = str(tuple(alias_list))
    sql = 'SELECT b.nick_name, b.alias, a.update_time\
    FROM dw_wxmp.notification_center a\
    right JOIN dw_wxmp.wxmp_info b\
    ON a.originid = b.originid\
    WHERE a.title = "帐号成功转为服务号的通知"\
    AND b.alias IN {}'.format(alias_list)
    df = get_data_sql('dw_wxmp', sql)
    df.rename(columns={
        'nick_name': '账号',
        'alias': 'ID',
        'update_time': '日期'
    }, inplace=True)
    print('升级信息有%d行%d列' % (df.shape[0], df.shape[1]))
    '''
    出现了一个账号对应有两个升级时间
    '''
    df.sort_values(['ID', '日期'], inplace=True)
    df = df.drop_duplicates('ID', keep='first')
    return df

def get_unique_id(file_path):
    '''从历史的表格中得到相应的18年老年号和17年的账号列表'''
    excel_list = os.listdir(file_path)
    id_all_18 = []
    for excel in excel_list:
        print('正在读取'+excel)
        exl = pd.ExcelFile(file_path + excel)
        sheet_list = [i for i in exl.sheet_names if i.find('.') >= 0]
        # print(sheet_list)
        for sheet in sheet_list:
            data = exl.parse(sheet)
            for t in data['类型'].unique():
                if isinstance(t, str):
                    if t.find('18') >= 0:
                        data_subscribe_18 = data[data['类型'] == t]
            id_list_18 = list(data_subscribe_18['ID'].unique())
            id_all_18.extend(id_list_18)
    id_all_18 = list(set(id_all_18))
    return id_all_18
    
def combine_commercial_data(commercial_path):
    '''
    汇总日报文件夹中的所有商务收入表格
    '''
    name_list = os.listdir(commercial_path)
    df_all = pd.DataFrame()
    for name in name_list:
        print('正在整理' + name)
        df = pd.read_excel(commercial_path + name)
        df_refined = refine_commercial_excel(df)
        df_all = df_all.append(df_refined)
    #df_all.to_excel('C:/Users/yyzq135/Desktop/总表.xlsx', index=False)
    return df_all

def filter_commercial_data(df_all, data, n=1):#df_all所有商务记录，data升级记录, n今天记录的数据到前几天为止
    '''
    保证所有的收入都是在已升级服务号在升级后的收入
    '''
    df_all = data_format(df_all)
    data = data_format(data)
    data_all = pd.DataFrame()
    time_now = pd.datetime.now().date() - pd.Timedelta('{} days'.format(n))
    for i in range(data.shape[0]):
        data_select = df_all[df_all['ID'] == data.loc[i, 'ID']]
        str_bool = data_select['日期'].map(lambda x: isinstance(x, str))
        #data_select_str = data_select[str_bool]
        data_select_time = data_select[str_bool == 0]
        time_bool = data_select_time['日期'].map(lambda x: x >= data.loc[i, '日期'] and x <= time_now)
        data_all = data_all.append(data_select_time[time_bool])
        #data_all = data_all.append(data_select_str)
    data_all.sort_values(by=['ID', '日期'], ascending=True, axis=0, inplace=True)
    return data_all

def get_commercial_income(df_all, df_info, n=1):
    '''
    添加一些因为迁移或者封号原因的账号的升级信息
    '''
    while True:
        if len(id_all_18) > df_info.shape[0]:
            print([_ for _ in id_all_18 if _ not in list(df_info['ID'])])
            extra_info_path = 'C:/Users/yyzq135/Desktop/明细数据/补充信息/补充信息.xlsx'
            data_extra = pd.read_excel(extra_info_path)
            df_info = df_info.append(data_extra, sort=False)
            continue
        elif len(id_all_18) < df_info.shape[0]:
            print('dp9443 error')
            break
        else:
            print(len(id_all_18), df_info.shape[0])
            #print('流量主表中多的账号有',[_ for _ in id_all_18 if _.upper() not in list(df_info['ID'])])
            #print('升级账号表中多的账号有',[_ for _ in list(df_info['ID']) if _.lower() not in id_all_18])
            data_selected = filter_commercial_data(data_all, df_info)
            
            wb = pd.ExcelWriter('C:/Users/yyzq135/Desktop/commercial.xlsx', 
                                engine='openpyxl', date_format='yyyy/mm/dd')
            df_info.to_excel(wb, sheet_name='账号升级', index=False)
            data_selected.to_excel(wb, sheet_name='商务收入', index=False)
            wb.save()
            wb.close()
            break
    
if __name__ == "__main__":
    '''
    商务收入部分
    '''
    start = time.time()
    commercial_path = 'C:/Users/yyzq135/Desktop/明细数据/商务收入/'
    file_path = 'C:/Users/yyzq135/Desktop/明细数据/流量主id获取/'
    data_all = combine_commercial_data(commercial_path)
    # 得到所求的18年部分账号列表
    id_all_18 = get_unique_id(file_path)
    df_info = get_upgrade_info(id_all_18)
    #df_info.to_excel('C:/Users/yyzq135/Desktop/commercial.xlsx', 
                     #sheet_name='账号升级', index=False)
    get_commercial_income(data_all, df_info)
    print('商务数据输出完成')
    end = time.time()
    print((end-start)/60)
    '''
    wb = pd.ExcelWriter('C:/Users/yyzq135/Desktop/commercial.xlsx', 
                                engine='openpyxl', date_format='yyyy/mm/dd')
    data_all.to_excel(wb, sheet_name='所有收入', index=False)
    df_info.to_excel(wb, sheet_name='升级信息', index=False)
    wb.save()
    wb.close()
    '''
    
    daily_paper_path = 'C:/Users/yyzq135/Desktop/日报/'
    data_flowmaster = combine_flowmaster_income(id_all_18, daily_paper_path)
    #data_flowmaster = get_flowmaster_income(id_all_18)
    data_warmwind = combine_warmwind_income(daily_paper_path)
    data_novel = combine_novel_income(daily_paper_path)
    data = summary_data(data_novel, data_flowmaster, data_warmwind)
    output_path = 'C:/Users/yyzq135/Desktop/refinesummary.xlsx'
    output_exl(data, output_path)
    end_2 = time.time()
    print((end_2-end)/60)
    '''
    wb = pd.ExcelWriter('C:/Users/yyzq135/Desktop/detaildata.xlsx', 
                                engine='openpyxl', date_format='yyyy/mm/dd')
    data_flowmaster.to_excel(wb, sheet_name='流量主', index=False)
    print('流量主写入完成')
    data_warmwind.to_excel(wb, sheet_name='暖风', index=False)
    print('暖风写入完成')
    data_novel.to_excel(wb, sheet_name='小说', index=False)
    print('小说写入完成')
    wb.save()
    wb.close()
    '''


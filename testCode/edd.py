# coding: utf-8
import pandas as pd
import numpy as np
import cdecimal
from datetime import datetime
import datetime as dt

def edd(df,output_path = None,save_result = True):

    '''
    计算 df 的 edd
    edd_file 为结果存储路径，当save_result =TRUE时存储
    数值型的均值，分位数等是去掉-1之后统计的
    '''

    start_time = datetime.now()

    ########### define header ###############
    header = ['var',
              'type',
              'mean',
              'std',
              '# of obs',
              '# of uniq obs',
              'nmiss',
              'mod',
              'min',
              'max',
              'p1 or top1',
              'p5 or top2',
              'p25 or top3',
              'p50 or top4',
              'p75 or bottom3',
              'p95 or bottom2',
              'p99 or bottom1',
              'minus_one']

    edd_out = pd.DataFrame()


    ########### edd calculate ###############
    inidf = dict()
    for var in df.columns:
        # print(var)
        inidf['var'] = var
        edd = pd.DataFrame(inidf.values(),columns=inidf.keys())

        try:
            df[var] = df[var].astype(np.float64)
        except:
            pass

        try:
            edd['type'] = type(df.ix[df[var].notnull(),var].reset_index(drop = True).ix[0]).__name__
        except:
            edd['type'] = type(df[var].iat[0]).__name__

        if type(df[var].iat[0]) in [unicode]:
            try:
                df[var] = df.ix[df[var].notnull(), var].apply(lambda x: x.encode('utf8'))  # unicode to str
            except AttributeError as e:
                print var
                df[var] = df.ix[df[var].notnull(), var].apply(lambda x: str(x).encode('utf8'))

        if type(df[var].iat[0]) in [str,unicode]:
            edd['mean'] = ''
            edd['std'] = ''
            edd['# of obs'] = df[var].shape[0]
            edd['# of uniq obs'] = len(df[var].unique())
            edd['nmiss'] = df[(df[var].isnull()) | (df[var]=='')].shape[0]
            try:
                edd['mod'] = df[var].mode().values[0]
            except:
                edd['mod'] = ''
            edd['vmin'] = ''
            edd['vmax'] = ''
            edd['p1 or top1'] = str(df[var].value_counts().index[0]) + "::" + str(df[var].value_counts().values[0])
            try:
                edd['p5 or top2']  = str(df[var].value_counts().index[1]) + "::" + str(df[var].value_counts().values[1])
            except:
                edd['p5 or top2']  = ''
            try:
                edd['p25 or top3'] = str(df[var].value_counts().index[3]) + "::" + str(df[var].value_counts().values[3])
            except:
                edd['p25 or top3'] = ''
            try:
                edd['p50 or top4'] = str(df[var].value_counts().index[4]) + "::" + str(df[var].value_counts().values[4])
            except:
                edd['p50 or top4'] = ''
            try:
                edd['p75 or bottom3'] = str(df[var].value_counts().index[-3]) + "::" + str(df[var].value_counts().values[-3])
            except:
                edd['p75 or bottom3'] = ''
            try:
                edd['p95 or bottom2'] = str(df[var].value_counts().index[-2]) + "::" + str(df[var].value_counts().values[-2])
            except:
                edd['p95 or bottom2'] = ''
            try:
                edd['p99 or bottom1'] = str(df[var].value_counts().index[-1]) + "::" + str(df[var].value_counts().values[-1])
            except:
                edd['p99 or bottom1'] = ''

        elif type(df[var].iat[0]) in [np.float64, np.int64, float] or type(df[var].iat[0]).__name__ in ['int64','Decimal']:  # sun:bill模型更新时遇到np.int64无法判断
            df_var_tmp = df[var].ix[(df[var].isnull()==False)&(df[var]!=-1)]
            try:
                edd['mean'] = df_var_tmp.mean()
                edd['std'] = df_var_tmp.std()
            except:
                edd['mean'] = -9999
                edd['std'] =  -9999
            try:
                edd['# of obs'] = df[var].shape[0]
                edd['# of uniq obs'] = len(df[var].unique())
                edd['nmiss'] = df[df[var].isnull()].shape[0]
            except:
                edd['# of obs'] =  -9999
                edd['# of uniq obs'] =  -9999
                edd['nmiss'] =  -9999
            try:
                edd['mod'] = df_var_tmp.mode().values[0]
            except:
                edd['mod'] = -9999
            try:
                edd['vmin'] = df_var_tmp.min()
                edd['vmax'] = df_var_tmp.max()
                edd['p1 or top1'] = df_var_tmp.quantile(0.01)
                edd['p5 or top2'] = df_var_tmp.quantile(0.05)
                edd['p25 or top3'] = df_var_tmp.quantile(0.25)
                edd['p50 or top4'] = df_var_tmp.quantile(0.5)
                edd['p75 or bottom3'] = df_var_tmp.quantile(0.75)
                edd['p95 or bottom2'] = df_var_tmp.quantile(0.95)
                edd['p99 or bottom1'] = df_var_tmp.quantile(0.99)
            except:
                edd['vmin'] = -9999
                edd['vmax'] = -9999
                edd['p1 or top1'] = -9999
                edd['p5 or top2'] = -9999
                edd['p25 or top3'] = -9999
                edd['p50 or top4'] = -9999
                edd['p75 or bottom3'] = -9999
                edd['p95 or bottom2'] = -9999
                edd['p99 or bottom1'] = -9999
        else:
            edd['mean'] = ''
            edd['std'] =  ''
            edd['# of obs'] =  ''
            edd['# of uniq obs'] =  ''
            edd['nmiss'] =  ''
            edd['mod'] = ''
            edd['vmin'] = ''
            edd['vmax'] = ''
            edd['p1 or top1'] = ''
            edd['p5 or top2'] = ''
            edd['p25 or top3'] = ''
            edd['p50 or top4'] = ''
            edd['p75 or bottom3'] = ''
            edd['p95 or bottom2'] = ''
            edd['p99 or bottom1'] = ''

        #count -1/0
        try:
            edd['count_-1'] = df[var].isin([-1, '-1']).sum()
        except:
            edd['count_-1'] = ''
        try:
            edd['count_0'] = df[var].isin([0, '0']).sum()
        except:
            edd['count_0'] = ''

        edd_out = pd.concat([edd_out,edd],axis = 0,ignore_index=True)

    if save_result & (output_path != None):
        try:
            edd_out.to_csv(output_path,index = False,encoding = 'gbk')
        except:
            edd_out.to_csv(output_path,index = False)
    print ("#----------------use {}-----------------#".format(datetime.now()-start_time))
    return edd_out

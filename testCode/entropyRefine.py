# -*-coding:utf-8 -*-


import csv
# from numpy import array
import numpy as np
import pandas as pd

from math import log

from impala.dbapi import connect
from impala.util import as_pandas


def loadData(fileName):
    data =[]
    # print "call inner function loadData .............."
    # print "reading file", fileName ,"............"

    obj = pd.read_csv(fileName)
    # print "read file", fileName ,"ended\n file size is",obj.shape,".......................\n"
    return obj.fillna('nan')

def logRefine(vector,base=2) :

    if len(vector) == 1:
        return log(vector,base)
    else:
        return [log(i,base) for i in vector]


def shanon(binName, binCnt):

    # calculate the entropy of give list    # bin + freq
    # print " call inner function shanon .............."
    freq = []
    s = pd.DataFrame({"binCnt": binCnt,"binName": binName}).pivot_table(index = "binName",aggfunc=np.sum)
    feature = set(binName)
    freq = [s["binCnt"][i] for i in feature]
    p_k = np.divide(freq , float(sum(freq)))
    entropy = - np.dot(p_k,logRefine(p_k))
    return entropy



def EntropyRefine(dataSet, targetColumn, conditionColumn, cntColumn):

    # print " call inner function EntropyRefine .............."
    conditionColumnSet = set(dataSet[conditionColumn])


    impute_grps = dataSet.pivot_table(values=[cntColumn], index=[targetColumn], aggfunc=np.sum)

    # binValue = impute_grps[cntColumn].index
    # rawEnt = shanon([i for i in binValue], [impute_grps[cntColumn][i] for i in binValue])

    totalCnt = float(sum(impute_grps['cnt_uniq_user']))

    # print  "===============calculating target column entropy============================"
    # print "\n total sample is", totalCnt
    # print  " raw entropy of ",targetColumn," is " , rawEnt

    # print  "===============calculating conditional entropy============================"

    var_pv = dataSet.pivot_table(values=[cntColumn], index=[conditionColumn,targetColumn], aggfunc=np.sum)


    print "\n\n......................................calculating entropy under attribute ",conditionColumn,"..................."

    rawEntList = []
    w_List     = []
    cntList    = []
    enum_valueList =[]
    cnt        = 0
    for enum_value in conditionColumnSet:
        cnt +=1
        sample_i = var_pv[cntColumn][str(enum_value)]

        rawEnt_i = shanon([i for i in sample_i.index], sample_i[sample_i.index] )
        w_i      = sum(sample_i[sample_i.index])/totalCnt
        rawEntList.append(rawEnt_i)
        w_List.append(w_i)
        cntList.append(cnt)
        enum_valueList.append(enum_value)

        print "No.",cnt," value: ",enum_value,", \t\t entropy :", rawEnt_i, "\t\tcoeff:", w_i

    # print w_List,cntList,enum_valueList
    # s1 = pd.Series(enum_valueList,index = cntList)
    # s2 = pd.Series(w_List,index=cntList)
    print "......................................"
    # m =[ w_List,cntList,enum_valueList]

    df = pd.DataFrame({"w_List": w_List,"enum_valueList": enum_valueList})

    print df
    # print np.array(m)
    # print np.array(m).shape

    # # m2= np.array(m).reshape(4,3)
    # # print m2
    # # print m2.shape

    # m3 = np.array(w_List).reshape(4,1)
    # print m3
    # m4 = np.asarray(cntList).reshape(4,1)
    # print m4

    # M = [m3,m4]
    # m5= array(m3,m4)
    # df = pd.DataFrame(M,index=enum_valueList,columns=['w_List','cntList','value'])
    # print df
    # entropy =  np.dot(rawEntList,w_List)
    # print "......................................entropy of ",  conditionColumn, "under " , targetColumn ," is ", entropy, "\n\n"

    # return entropy






def run_sql(sql):
    conn = connect(host='10.2.8.91', auth_mechanism='PLAIN', port=21050, user='zhangminshu02', password='zhangminshu02')
    cursor = conn.cursor()


    cursor.execute(sql)
    # df = as_pandas(cursor)
    # print(df)
    return as_pandas(cursor) if cursor.description != None else 'null'



if __name__ == '__main__':
    print "hello world !"
    sql = '''select * from appzc.zms_customer_income_vars_Master_1  limit 10;'''


    # intital
    rawDataSet = loadData('C:\Users\zhangminshu02\Downloads\querryResult\query-impala-486745.csv')
    # condition = [u'phone_os_treat', u'ppd_tenure_m_bin', u'age_bin', u'cmstr_pho_pro', u'reg_resorce', u'cnt_rec', u'cnt_uniq_user'],

    # condition = rawDataSet.columns.difference(['cnt_uniq_user','salary_bin3','cnt_rec'])
    c = 'phone_os_treat'

    EntropyRefine(rawDataSet, targetColumn = 'salary_bin3', conditionColumn=c ,cntColumn='cnt_uniq_user')



    # for c in condition:
    #     # print c
    #     sql_grp = 'select salary_bin3 ,' + c +' , count(1) as cnt_rec , count(distinct userid) as cnt_uniq_user from appzc.zms_customer_income_vars_Master_1 group by salary_bin3, '+ c
    #     print sql_grp
    #     df = run_sql(sql_grp)
    #     rawDataSeti = df.fillna('nan')

    #     EntropyRefine(rawDataSeti, targetColumn = 'salary_bin3', conditionColumn=c ,cntColumn='cnt_uniq_user')

        # groupColumn = ['age_bin','cmstr_pho_pro']
        # groupColumnStr = ', '.join(groupColumn)











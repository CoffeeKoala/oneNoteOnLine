# -*- coding: utf-8 -*-
# @Author: zhangminshu
# @Date:   2018-03-26 10:39:06
# @Email:  zhangminshu02@ppdai.com
# @Last Modified by:   zhangminshu02
# @Last Modified time: 2019-03-13 11:10:25
################################################################################

from bs4 import BeautifulSoup, element
import time,re,urllib2,urllib,csv,re
import os
import pandas as pd

def loadData(otherC):
    # return mutiple files in each string

    folderpath = './rawData/'
    fileList = os.listdir(folderpath)
    print "*"*40
    print "loading data..."
    print "total",len(fileList),"files are found!"

    df = pd.DataFrame()
    for f in fileList:
        d = pd.read_csv(folderpath + f)
        df = pd.concat([df,d],axis=0)
    print "file size",df.shape
    print "*"*40
    return df[otherC]

def writeResultToCsv(lines,columns):
    # # # write data
    with open('result/result.csv', 'wb') as csvfile:
        # csvfile.write(codecs.BOM_UTF8)
        spamwriter = csv.writer(csvfile, delimiter='|')



        spamwriter.writerow(columns)
        rowCount=0
        for l in lines:
            spamwriter.writerow(l)
            rowCount +=1
        # print "...Total", rowCount , "rows are written!"
    return rowCount

def analysisString(textString, objString,columns):

    soup = BeautifulSoup(textString,'lxml')


    userinfo = soup.find_all(objString.lower())

    # # find data object

    resultList = []
    # print "...Total", len(resultList) ,"data object are found!"


    for i in xrange(len(userinfo)):
        result =[]
        for c in columns :

            temp1 =  userinfo[i].find_all(c.lower())
            # print "columns is ",c,"result is ", temp1, "length is ", len(temp1),type(temp1)

            # 没找到或者值为空
            if len(temp1) == 0  :

                temp2 = 'column not found'
            elif len(temp1) == 1 :
                temp2 = temp1[0].get_text().encode('utf-8')

            elif len(temp1) > 1 :
                # temp2 = [xx.get_text().string.decode('utf-8').encode('ascii') for xx in temp1]

                # temp2 = [xx.get_text().string.encode('utf-8') for xx in temp1]
                temp2 = [xx.get_text() for xx in temp1]


               # temp2 = userinfo[i].find_all(c.lower()).string.encode("utf-8")

            result.append(temp2)


        resultList.append(result)

    return resultList


def analysisString2(df,columns, objString = "execution-results"):

    # 按行处理
    print "*"*40
    print df.shape, "rows to be processed"
    print "*"*40
    cntlines = 0
    for i in df.index:

        lineString = df.loc[i,'Content']

        soup = BeautifulSoup(lineString,'lxml').find(objString.lower())

        for c in columns:
            text_c =  soup.find_all(c.lower())

            if isinstance(text_c,element.ResultSet):
                df.loc[i,c]  = "|".join([xx.get_text().encode('utf-8') for xx in text_c])



    return df


if __name__ == '__main__':



    columns = ['processFlag','name']

    otherC = ['Type', 'Content', 'InsertTime',  'UserId', 'listingId', 'bizId']

    t = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    print "-"*80
    print "hello world ! current time",t
    print "try to extract data object"
    print "-"*80



    df = loadData(otherC).head(100)

    df2 = analysisString2(df, columns).drop(['Content'],axis=1)

    df2.to_csv('result/result.csv',index=False)
    print "-"*80






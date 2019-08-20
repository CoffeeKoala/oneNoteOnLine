
def analysisString2(df,columns, objString = "execution-results"):

    # 按行处理
    print "*"*40
    print df.shape, "rows to be processed"
    print "*"*40
    cntlines = 0
    for i in df.index:
        if i %100 == 0:
            print "No ",i, "00 rows processing"
        lineString = df.loc[i,'Content']

        soup = BeautifulSoup(lineString,'lxml').find(objString.lower())

        for c in columns:
            text_c =  soup.find_all(c.lower())

            if isinstance(text_c,element.ResultSet):
                df.loc[i,c]  = "|".join([xx.get_text().encode('utf-8') for xx in text_c])
    return df.drop(['Content'],axis=1)


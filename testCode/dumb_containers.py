import pandas as pd
import numpy as np
import csv
from sklearn.metrics import roc_curve, auc
import random
import matplotlib.pyplot as plt
import pylab as plb
import pdb
import pickle
import os

# Load valuation maps
def load_valuation_rules(dr):
    values = dict()
    files = os.listdir(dr)
    for f in files:
        if f[-4:]=='.csv':
            rows = list(csv.reader(open(os.path.join(dr, f), 'r')))
            v = np.zeros((len(rows)-1, 91))
            for i in range(len(rows)-1):
                v[i, :] = np.interp(np.arange(91), list(map(float, rows[0])), list(map(float, rows[i+1])))
            values[f[0:-4]] = v

    return values

# compute valuation
def get_valuation(current_num, max_num, nextpaydays, rate, defaultdays, creditlevel,
                  product_type, valuation_maps, isdraw, ispay):

    #if type(nextpaydays) == pd.tslib.Timestamp:
    #    nextmonth = 1.0 * (datetime.datetime.today() - nextpaydays).days / 30.0
    #else:
    #    nextmonth = 1.0

    if nextpaydays >= 0:
        nextmonth = nextpaydays / 30.0
    else:
        nextmonth = 1.0

    rate = rate*1.0/100.00

    # 1) calculate npv
    remaining_num = max_num - current_num
    monthlypayment = rate/12.0*np.power(1.0+rate/12.0, max_num-current_num) / \
        (np.power(1.0+rate/12.0, max_num-current_num)-1)
    cs = np.array([0] + [monthlypayment]*(max_num-current_num))
    if remaining_num <= 6:
        #cs[1] = np.npv(0.1/12, cs[1:])
        cs[1] = np.npv(0.095/12, cs[1:])
    elif remaining_num > 6 and remaining_num < 12:
        #cs[5] = np.npv(0.11/12, cs[5:])
        #cs[1] = np.npv(0.1/12, cs[1:6])
        cs[5] = np.npv(0.105/12, cs[5:])
        cs[1] = np.npv(0.095/12, cs[1:6])
    else:
        #cs[10] = np.npv(0.12/12, cs[10:])
        #cs[5] = np.npv(0.11/12, cs[5:11])
        #cs[1] = np.npv(0.1/12, cs[1:6])
        cs[10] = np.npv(0.115/12, cs[10:])
        cs[5] = np.npv(0.105/12, cs[5:11])
        cs[1] = np.npv(0.095/12, cs[1:6])

    npv = cs[1] / np.power(1.0 + 0.095/12, nextmonth)
    if isdraw == 1 or ispay == 1:
        default = 0.0
    else:
        # 2) set credit level
        if product_type == 'guodu':
            if creditlevel[0] in ('A', 'B', 'C'):
                c = 'ABC'
            else:
                c = 'D'
        if product_type == 'yici':
            if creditlevel[0] in ('A', 'B', 'C'):
                c = 'ABC'
            elif creditlevel[0] == 'D':
                c = 'D'
            else:
                c = 'E'
        if product_type == 'dianshang':
            c = 'All'

        # 3) get months
        if max_num <=4 :
            m = 3
        elif max_num <= 8:
            m = 6
        else:
            m = 12

        try:
            valuation_map = valuation_maps[product_type+'_'+c+'_'+str(m)]
            default = np.interp(defaultdays, np.arange(valuation_map.shape[1]), \
                            valuation_map[min(valuation_map.shape[0]-1, current_num), :], right=1.0)
        except Exception:
            default = 2.0

        #if current_num == 0 and defaultdays==0:
        #    if creditlevel[0] in ('A', 'B', 'C'):
        #        default = 0.01
        #    elif creditlevel[0] == 'D':
        #        default = 0.04
        #    elif creditlevel[0] == 'E':
        #        default = 0.08
        #    elif creditlevel[0] == 'F':
        #        default = 0.14
        #    else:
        #        default = 0.2

    return npv*1.0*(1-default)

# prepare discount
def prepare_discount(filepath):
    ll = 200
    rows = list(csv.reader(open(filepath, 'r')))

    discount_map = np.zeros((len(rows)-1, ll))

    xs = np.array(list(map(float, rows[0])))
    for i in range(len(rows)-1):
        ys = np.array(list(map(float, rows[i+1])))
        discount_map[i,:] = np.interp(np.arange(ll), xs, ys)
    return discount_map

# get discount
def get_discount(discount_map, month, defaultdays):
    month = min(month, discount_map.shape[0])
    defaultdays = min(defaultdays, discount_map.shape[1]-1)
    return discount_map[month-1, defaultdays]


# get IRR
def compute_irr(start_principal, cashflow):
    tt = cashflow.copy()
    tt[0] = tt[0] - start_principal
    return np.irr(tt) * 12.0

# assign two-d woe
def assign_woe2(woes, bin1, bin2, values1, values2):
    results = np.zeros(values1.shape[0])
    for i in range(woes.shape[0]):
        for j in range(woes.shape[1]):
            ind = plb.find((values1>=bin1[i]) & (values1<bin1[i+1]) & \
                (values2>=bin2[j]) & (values2<bin2[j+1]))
            if ind.shape[0] > 0:
                results[ind] = woes[i]
    return results

# assign woe2
def assign_woe(woes, bins, values):
    results = np.zeros(values.shape[0])
    for i in range(woes.shape[0]):
        ind = plb.find((values>=bins[i]) & (values<bins[i+1]))
        if ind.shape[0] > 0:
            results[ind] = woes[i]
    return results

# compute 2-d woe
def woe2(x1, x2, y, bin1=[], bin2=[]):
    l1 = len(bin1)
    l2 = len(bin2)
    woes = np.zeros((l1-1, l2-1))
    counts = np.zeros((l1-1, l2-1))
    targets = np.zeros((l1-1, l2-1))
    for i in range(l1-1):
        for j in range(l2-1):
            inds = plb.find((x1>=bin1[i]) & (x1<bin1[i+1]) & \
                (x2>=bin2[j]) & (x2<bin2[j+1]))
            if inds.shape[0] > 0:
                p = np.mean(y[inds])
                woes[i,j] = np.log(p/(1.0-p))
                counts[i,j] = int(inds.shape[0])
                targets[i,j] = int(np.sum(y[inds]))
            else:
                woes[i,j] = 0
                counts[i,j] = int(0)
                targets[i,j] = int(0)
    return woes, counts, targets

# compute woe
def woe(x, y, bin=5, auto=True, toplot=True):

    if type(bin) != int:
        auto = False

    if auto:
        bins = np.zeros(bin+1)
        for i in range(bin+1):
            bins[i] = np.percentile(x, np.maximum(0, np.minimum(100, round(100.0/bin*i))))
    else:
        bins = np.array(bin)

    if type(bin) == int and np.unique(x).shape[0]<=10:
        temp = np.sort(np.unique(x))
        bin = temp.shape[0]
        bins = np.zeros(bin+1)
        bins[0:bin] = temp
        bins[-1] = temp[-1]+1

    bin = bins.shape[0]-1

    binx = np.zeros(bin)
    woes = np.zeros(bin)
    counts = np.zeros(bin)
    targets = np.zeros(bin)

    for i in range(bin):
        inds = (x >= bins[i]) & (x < bins[i+1])
        p1 = np.mean(y[inds])
        woes[i] = np.log(p1/(1.0-p1))
        counts[i] = np.sum(inds)
        targets[i] = np.sum(y[inds])
        binx[i] = np.mean(x[inds])

    if toplot:
        plt.bar(np.arange(1,len(binx)+1), counts)
        plt.bar(np.arange(1,len(binx)+1), targets, color='r')
        ax2 = plt.twinx()
        plt.plot(np.arange(1,len(binx)+1)+0.5, woes, '.-k', linewidth=2, markersize=10)

    return woes, bins, counts, targets

def grouptest( data,factor,target,moderatiolow,moderatiohigh ):
    #print 'begin testing'

    for f in factor:
        #print 'Testing factor',f
        # find the mode, special dealing
        mode=data[f].mode()
        data_mode=data.ix[data[f]==mode[0]]
        print('Mode=',mode[0],'Mode count=',data_mode[target].count())
        moderatio=float(data_mode[target].count())/float(len(data))
        print('moderatio',format(moderatio,'5f'))
        if moderatio>moderatiohigh :
            print('Data Mode Error')
            print(f,'test is over\n')
            continue
        elif ( moderatio>moderatiolow) :
            ind10=data_mode[target].ix[data_mode[target]==1].count()
            ind00=data_mode[target].ix[data_mode[target]==0].count()
            print('Mode Default=',ind10,'Mode NonDefault=',ind00)
            drate0=float(ind10)/float(data_mode[target].count())
            print('Mode DefaultRate=',format(drate0,'5f'))
            data_nonmode=data.ix[data[f]!=mode[0]]
            gt(data_nonmode,f,target)
        else:
            print('No special Mode')
            gt(data,f,target)


def gt(data,f,target):
        # grouptest nonmode data
        x0=data[f].quantile(0.01)
        x1=data[f].quantile(0.2)
        x2=data[f].quantile(0.4)
        x3=data[f].quantile(0.6)
        x4=data[f].quantile(0.8)
        x5=data[f].quantile(.99)

##        x0=float(data[f].quantile(0))
##        x1=float(data[f].quantile(0.2))
##        x2=float(data[f].quantile(0.4))
##        x3=float(data[f].quantile(0.6))
##        x4=float(data[f].quantile(0.8))
##        x5=float(data[f].quantile(1.0))

        print('Five Group range',x0,x1,x2,x3,x4,x5)


        c1=data[target].ix[(data[f]>=x0)&(data[f]<=x1)].count()
        c2=data[target].ix[(data[f]>x1)&(data[f]<=x2)].count()
        c3=data[target].ix[(data[f]>x2)&(data[f]<=x3)].count()
        c4=data[target].ix[(data[f]>x3)&(data[f]<=x4)].count()
        c5=data[target].ix[(data[f]-x4>0)&(data[f]-x5<=0)].count()
        print('Five Group Count',c1,c2,c3,c4,c5)

        data_default=data.ix[data[target]==1]
        data_nondefault=data.ix[data[target]==0]
        ind11=data_default[target].ix[(data_default[f]>=x0)&(data_default[f]<=x1)].count()
        ind12=data_default[target].ix[(data_default[f]>x1)&(data_default[f]<=x2)].count()
        ind13=data_default[target].ix[(data_default[f]>x2)&(data_default[f]<=x3)].count()
        ind14=data_default[target].ix[(data_default[f]>x3)&(data_default[f]<=x4)].count()
        ind15=data_default[target].ix[(data_default[f]>x4)&(data_default[f]<=x5)].count()
        print('Five Group Default Count',ind11,ind12,ind13,ind14,ind15)
        try:
            drate1=float(ind11)/float(data[target].ix[(data[f]>=x0)&(data[f]<=x1)].count())
        except ZeroDivisionError:
            print('group1 error')
            drate1=-1
        try:
            drate2=float(ind12)/float(data[target].ix[(data[f]>x1)&(data[f]<=x2)].count())
        except ZeroDivisionError:
            print('group2 error')
            drate2=-1
        try:
            drate3=float(ind13)/float(data[target].ix[(data[f]>x2)&(data[f]<=x3)].count())
        except ZeroDivisionError:
            print('group3 error')
            drate3=-1
        try:
            drate4=float(ind14)/float(data[target].ix[(data[f]>x3)&(data[f]<=x4)].count())
        except ZeroDivisionError:
            print('group4 error')
            drate4=-1
        try:
            drate5=float(ind15)/float(data[target].ix[(data[f]>x4)&(data[f]<=x5)].count())
        except ZeroDivisionError:
            print('group5 error')
            drate5=-1

        print('Five Group DefaultRate',format(drate1,'5f'),format(drate2,'5f'),format(drate3,'5f'),format(drate4,'5f'),format(drate5,'5f'))
        #print f,'test is over\n
        plt.subplot(2,1,2)
        plt.bar(np.array([x0, x1, x2, x3, x4]), np.log(np.maximum(0, np.array([drate1, drate2, drate3, drate4, drate5]))))

        print('Five Group DefaultRate',format(drate1,'5f'),format(drate2,'5f'),format(drate3,'5f'),format(drate4,'5f'),format(drate5,'5f'))
        print(f,'test is over\n\n')
        return


def factordivide(data,f,point):
    l=len(data[f])
    v1=data[f].ix[data[f]>point]
    v2=data[f].ix[data[f]<point]
    v3=np.zeros(l)

    data['v1']=v1
    data['v2']=v2
    data['v3']=v3
    return data

# split data
# df -- dataframe
# split -- ratio to split out float (0, 1)
# seed -- random seed to use
def split_data(df, split, seed=1):
    random.seed(seed)
    rows = random.sample(df.index, int(round(split*(df.shape[0]))))
    df_split = df.ix[rows]
    df_remaining = df.drop(rows)
    return df_split, df_remaining, rows

def hump_variable(df, variable, split_pt):
    inds1 = df[variable] <= split_pt
    inds2 = df[variable] > split_pt
    df[variable+'_1'] = np.zeros(df.shape[0])
    df[variable+'_2'] = np.zeros(df.shape[0])
    df[variable+'_3'] = np.zeros(df.shape[0])
    df[variable+'_1'][inds1] = split_pt - df[variable][inds1]
    df[variable+'_2'][inds2] = df[variable][inds2] - split_pt
    df[variable+'_3'][inds1] = 1.0
    return df

# Turn a categorical series to a few columns of dummy variables
# each unique value will be a separate column
#
# s - a data series
def get_dummies_column(s):
    vc = s.value_counts()
    names = vc.index
    length = vc.values.shape[0]

    #print names
    column_name = s.name;
    row_num = s.shape[0]
    #print row_num

    data = np.zeros((row_num, length))

    column_names = ['']*(length)
    for i in range(length):
        column_names [i] = column_name + '_' + names[i]
        data[:, i] = (s == names[i]).astype(int)

    return pd.DataFrame(data, s.index, columns=column_names)

# Turn a list of categorical series to dummy series, append them,
def process_dummies(data, columns):
    df = data;
    for i in range(len(columns)):
        column = columns[i]
        df[column] = df[column].astype(str)
        dummy_series = get_dummies_column(df[column])
        df = pd.concat([df, dummy_series], axis=1)
    return df

# clean up, floor values to 2*p99 by default
def treat_floor(df, names):
    for name in names:
        temp = df[name].quantile(0.99)
        if temp >= 0:
            df[name] = np.minimum(2.0 * temp, df[name])
        else:
            df[name] = np.minimum(0.5 * temp, df[name])
    return df

# clean up, ceiling values to p1*2 by default
def treat_ceiling(df, names):
    for name in names:
        temp = df[name].quantile(0.01)
        if temp > 0:
            df[name] = np.maximum(temp*0.5, df[name])
        else:
            df[name] = np.maximum(temp*2.0, df[name])
    return df

# Evaluate output of a logit
# Plot appropriate figures: KS/AUC, score distribution/average score
def evaluate_performance(all_target, predicted, toplot=True):
    fpr, tpr, thresholds = roc_curve(all_target, predicted)
    roc_auc = auc(fpr, tpr)
    ks = max(tpr-fpr)
    maxind = plb.find(tpr-fpr == ks)

    event_rate = sum(all_target) / 1.0 / all_target.shape[0]
    cum_total = tpr * event_rate + fpr * (1-event_rate)
    minind = plb.find(abs(cum_total - event_rate) == min(abs(cum_total - event_rate)))
    if minind.shape[0] > 0:
        minind = minind[0]

    print('KS=' + str(round(ks,2)) + ', AUC=' + str(round(roc_auc,2)) +', N='+str(predicted.shape[0]))

    if toplot:
        # KS plot
        plt.figure(figsize=(20,6))
        plt.subplot(1,3,1)
        plt.plot(fpr, tpr)
        plt.hold
        plt.plot([0,1],[0,1], color='k', linestyle='--', linewidth=2)
        plt.title('KS='+str(round(ks,2))+ ' AUC='+str(round(roc_auc,2)), fontsize=20)
        plt.plot([fpr[maxind], fpr[maxind]], [fpr[maxind], tpr[maxind]], linewidth=4, color='r')
        plt.plot([fpr[minind]], [tpr[minind]], 'k.', markersize=10)

        plt.xlim([0,1])
        plt.ylim([0,1])
        plt.xlabel('False positive', fontsize=20); plt.ylabel('True positive', fontsize=20);
        print('KS=' + str(round(ks,2)) + ', AUC=' + str(round(roc_auc,2)) +', N='+str(predicted.shape[0]))
        print('At threshold=' + str(round(event_rate, 3)) + ', TPR=' + str(round(tpr[minind],2)) + ', ' + str(int(round(tpr[minind]*event_rate*all_target.shape[0]))) + ' out of ' + str(int(round(event_rate*all_target.shape[0]))))

        #print 'At threshold=' + str(round(event_rate, 3))
        #print str(round(fpr[minind],2))
        #print str(int(round(fpr[minind]*(1.0-event_rate)*all_target.shape[0])))
        #print str(int(round((1.0-event_rate)*all_target.shape[0])))
        print('At threshold=' + str(round(event_rate, 3)) + ', TPR=' + str(round(fpr[minind],2)) + ', ' + str(int(round(fpr[minind]*(1.0-event_rate)*all_target.shape[0]))) + ' out of ' + str(int(round((1.0-event_rate)*all_target.shape[0]))))

        # Score distribution score
        plt.subplot(1,3,2)
        #print predicted.columns
        plt.hist(predicted, bins=20)
        plt.hold
        plt.axvline(x=np.mean(predicted), linestyle='--')
        plt.axvline(x=np.mean(all_target), linestyle='--', color='g')
        plt.title('N='+str(all_target.shape[0])+' Tru='+str(round(np.mean(all_target),3))+' Pred='+str(round(np.mean(predicted),3)), fontsize=20)
        plt.xlabel('Target rate', fontsize=20)
        plt.ylabel('Count', fontsize=20)

        # Score average by percentile
        binnum = 10
        ave_predict = np.zeros((binnum))
        ave_target = np.zeros((binnum))
        indices = np.argsort(predicted)
        binsize = int(round(predicted.shape[0]/1.0/binnum))
        for i in range(binnum):
            startind = i*binsize
            endind = min(predicted.shape[0], (i+1)*binsize)
            ave_predict[i] = np.mean(predicted[indices[startind:endind]])
            ave_target[i] = np.mean(all_target[indices[startind:endind]])

        plt.subplot(1,3,3)
        plt.plot(ave_predict, 'b.-', label='Prediction', markersize=5)
        plt.hold
        plt.plot(ave_target, 'r.-', label='Truth', markersize=5)
        plt.legend(loc='lower right')
        plt.xlabel('Percentile', fontsize=20)
        plt.ylabel('Target rate', fontsize=20)
        print('Ave_target: ' + str(ave_target))
        print('Ave_predicted: ' + str(ave_predict))
        plt.show()

    return ks

# Get header row of a file
def get_header(fi):
    f = open(fi, 'r')
    g = csv.reader(f)
    head = next(g)
    head = [x.replace('\xef\xbb\xbf', '') for x in head]
    f.close()
    return head

# Get string for columns to keep to pass to awk
def get_column_string(header, columns, marker='$'):
    ss = marker + str(header.index(columns[0])+1)
    for i in range(1, len(columns)):
        ss = ss + ',' + marker + str(header.index(columns[i])+1)
    return ss

# get dataframe that correspond to a unique field
def get_data(g, currentrow, header, fieldtomatch, tomatch):
    if len(currentrow) == 0:
        return [], []
    index = header.index(fieldtomatch)
    if currentrow[index] > tomatch:
        return [], currentrow
    elif currentrow[index] < tomatch:
        while True:
            try:
                row = next(g)
                currentrow = row
                if row[index] > tomatch:
                    return [], currentrow
                elif row[index] == tomatch:
                    break
            except StopIteration:
                return [], []

    rows = [currentrow]
    while True:
        try:
            row = next(g)
            if row[index] == tomatch:
               rows.append(row)
            else:
                return pd.DataFrame(rows, columns=header), row
        except StopIteration:
            return pd.DataFrame(rows, columns=header), []

# save an object to a file
def save_object(obj, filename):
    with open(filename, 'wb') as output:
        pickle.dump(obj, output, -1)

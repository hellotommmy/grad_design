import pandas as pd

import time



def split(splitter,splittee):
    chunksize = 500000
    len_of_string = 81
    cols_dex = pd.read_csv(splitter, delim_whitespace=True)
    header = []
    for cols in cols_dex['Variable']:
        header.append(cols)
    # print big_data
    how_to_split = []
    for i in cols_dex['Columns']:
        how_to_split.append(int(i.split('-')[0]) - 1)
    # for i in how_to_split:
    how_to_split.append(len_of_string)
    table = []
    df2 = pd.DataFrame(table,
                       columns=header)
    df2.to_csv('split.csv', index=None, sep=' ', mode='a')
    for big_data in pd.read_csv(splittee, header = None, chunksize=chunksize):
        table = []
        for row in big_data.itertuples():
            snew = []
            for i in range(1,len(how_to_split)):
                snew.append(row[1][how_to_split[i-1]:how_to_split[i]])
            table.append(snew)

        df2 = pd.DataFrame(table,
                           columns=header)
        #df2.to_csv('split.csv',index = None,sep = ' ',mode = 'a')
    #print how_to_split
        with open('split.csv', 'a') as f:
            df2.to_csv(f, header=False)

t1 = time.clock()
split('l.txt','usa_00001.dat')
print time.clock()-t1
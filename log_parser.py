import pandas as pd
import numpy as np
from time import time
import argparse

if __name__ == '__main__':
    
    # Command-line format: python3 filename --target=log.csv
    parser=argparse.ArgumentParser()
    parser.add_argument('--target', help='Path to the logfile to be analyzed. If filename only is provided, then the script will look within current directory.')

    args=parser.parse_args()

    start = time()

    # get filename from command line
    filename = args.target

    # read the file into list
    with open(filename, 'r') as myfile:
        data=myfile.read()
        lst = data.split('\n')

    conn_list = list()
    disconn_list = list()
    # else_list = list()

    # take only connection-related data
    for i in range(0,len(lst)-1):
        if 'disconnection' in lst[i]:
            disconn_list.append(lst[i])
        elif 'connection' in lst[i]:
            conn_list.append(lst[i])
        # else:
        #     else_list.append(lst[i])

    # parse both lists and dataframe them together
    conn_df = list()

    for i in conn_list:
        parts  = i.split(' UTC:')
        parts2 = parts[1].split(':LOG:')
        req_type = parts2[1].strip().split(' ')[1]
        conn_df.append([parts[0], parts2[0], req_type.replace(':',''), np.nan])

    disconn_df = list()

    for i in disconn_list:
        parts  = i.split(' UTC:')
        parts2 = parts[1].split(':LOG:')
        session_time = parts2[1].strip().split(' ')[3]
        disconn_df.append([parts[0], parts2[0], 'disconnected', session_time])

    df = pd.DataFrame(conn_df + disconn_df, columns=['timestamp', 'line', 'type', 'session_time'])

    # extract origin/destination
    df['origin'] = df.line.apply(lambda x: x.split(':')[0])
    df['user_database'] = df.line.apply(lambda x: x.split(':')[1] + x.split(':')[2])

    # drop redudancy
    df.drop('line', axis=1, inplace=True)

    # write to file
    df.to_csv(filename+'.csv', index=False)

    end = time()
    print('Finished in', (end - start)/60, 'min.')

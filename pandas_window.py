import pandas as pd
import os
import sys
import argparse 
import csv





def getpanda(filepath): 

    filepath.replace(" ", "^ ")
    data = pd.read_csv(filepath, header=0, delim_whitespace=True, skip_blank_lines=True)
    data["index_column"] = range(len(data))
    data.set_index("index_column", inplace=True)
    return data 


def process(data, windowsize): 

    data["POSITION"] = data["POSITION"].astype(int) 
    startval = data["POSITION"][0]

    windows = []

    for row, value in enumerate(data["POSITION"]): 

        if value - startval >= windowsize: 
            windows.append(row)
            startval = value

    return windows





def cluster(data, windowsize, pattern): 

    heads = data.columns[4:8]
    data[f'CLUSTERS1{pattern}'] = ["" for _ in range(0,len(data["POSITION"]))]
    data[f'CLUSTERS2{pattern}'] = ["" for _ in range(0,len(data["POSITION"]))]
    data[f'CLUSTERS3{pattern}'] = ["" for _ in range(0,len(data["POSITION"]))]
    data[f'CLUSTERS4{pattern}'] = ["" for _ in range(0,len(data["POSITION"]))]

    #start the window
    rear = 0
    front   = 0
    clustering = [False, False, False, False]
    trackerstart = [0,0,0,0]
    recent = [0,0,0,0]


        
    for index in range(0, len(data["POSITION"])-1): 
        
        front = index

        for i in range(0,4):
            if data.loc[index, heads[i]]== pattern: 
                recent[i] = index + 1
                #pop back of list

        while data["POSITION"][front] - data["POSITION"][rear] > windowsize: 
            rear += 1

        for i in range(0,4):

            if clustering[i]: 

                if pattern not in data.loc[rear: front+1, heads[i]].values:
                    clustering[i] = False
                    clustering[i] = False

                    for m in range(trackerstart[i], recent[i]): 
                        data.loc[m,[f"CLUSTERS{i+1}{pattern}"]] = "PRESENT"
            else: 

                if pattern in data.loc[rear: front+1, heads[i]].values:

                    clustering[i] = True
                    trackerstart[i] = front + 1


    for i in range(0,4):
        if clustering: 
            for k in range(trackerstart[i], recent[i]): 
                data.loc[k,f"CLUSTERS{i+1}{pattern}"] = "PRESENT"
            if data.loc[len(data["POSITION"])-1, heads[i]] == pattern: 
                data.loc[len(data["POSITION"])-1,f"CLUSTERS{i+1}{pattern}"] = "PRESENT"



    return data



'''
def toall_seperately(data, source_cols, dest_col_titles, window, fn):
            
            
            
            data.loc[index+1, "dest_col_titles1"] = fn(data, heads[0], index+1, windows[0])
            data.loc[index+1, "dest_col_titles2"] = fn(data, heads[1], index+1, windows[0])
            data.loc[index+1, "dest_col_titles3"] = fn(data, heads[2], index+1, windows[0])
            data.loc[index+1, "dest_col_titles4"] = fn(data, heads[3], index+1, windows[0])


'''

def visit_window(data, windows, fn): 

    initpos = 0

    j =  ["" for _ in range(len(data["POSITION"]))]
    data["COUNT1"] = j
    data["COUNT2"] = j
    data["COUNT3"] = j
    data["COUNT4"] = j

    heads = data.columns[4:8]

    
    if "count0" in fn.lower(): 
        k = lambda dat, col, strt, nd : (dat[col][strt:nd].astype(float) == 0).sum()
    elif "prod" in fn.lower(): 
        k = lambda dat, col, strt, nd : dat[col][strt:nd].astype(float).prod()
    elif "sum" in fn.lower(): 
        k = lambda dat, col, strt, nd : dat[col][strt:nd].astype(float).sum()


    for i in range(1,5): 
        data.loc[0, f"COUNT{i}"] = k(data, heads[i-1], 0, windows[0])


        index = 0
    while windows != []:
        index = windows.pop(0)
        if windows != []:
            data.loc[index+1, "COUNT1"] = k(data, heads[0], index+1, windows[0])
            data.loc[index+1, "COUNT2"] = k(data, heads[1], index+1, windows[0])
            data.loc[index+1, "COUNT3"] = k(data, heads[2], index+1, windows[0])
            data.loc[index+1, "COUNT4"] = k(data, heads[3], index+1, windows[0])
        else:
            pass
            data.loc[index+1, "COUNT1"] = k(data, heads[0], index+1, -1)
            data.loc[index+1, "COUNT2"] = k(data, heads[1], index+1, -1)
            data.loc[index+1, "COUNT3"] = k(data, heads[2], index+1, -1)
            data.loc[index+1, "COUNT4"] = k(data, heads[3], index+1, -1)

    return data

def cluster_summary(data, pattern): 

    count = 0
    tracking = False
    log = {"ERRORLOCS" : [], 'ERRORCOUNT' : []}
    for ind, val in enumerate(data[f"CLUSTERSOF{pattern}"]):

        if  val == "PRESENT": 
            count += 1
            tracking = True

        elif tracking == True: 
            tracking = False
            log['ERRORLOCS'].append(ind-count)
            log["ERRORCOUNT"].append(count)
            count = 0

    return pd.DataFrame(log)






if __name__ == "__main__": 
    parser = argparse.ArgumentParser(description='Run Cleaning Tool')
    parser.add_argument("filepath",help="relative filepath for file to check, required")

    parser.add_argument("filedest",help="destination filepath of augmented file, required" )

    
    parser.add_argument('--pattern', dest='pattern', action='store', type=float,
                        default=0.0, help='pattern to search in sliding clustering algorithm')
    
    parser.add_argument('--partitionsize', dest='partsize', action='store', type=int,
                        default=1000000, help='windowpartition when calculating in non-sliding style default=1000000')
    
    parser.add_argument("--function", type=str, dest="func", choices=["count0", "prod", "sum"], default="count0", help="option selection for non-sliding window choose from: count0 prod sum")

    parser.add_argument('--slidesize', dest='slidesize', action='store', type=int,
                    default=1000000, help='windowsizeslide when calculating sliding style defalut=1000000')


    args = parser.parse_args()


    data = getpanda(args.filepath)
    windows = process(data, args.partsize)
   
    data = visit_window(data, windows, args.func)
    data = cluster(data, args.slidesize, args.pattern)
    data.to_csv(args.filedest, sep='\t', index=False)

    #data = cluster_summary(data, args.pattern)
    #data.to_csv(f"{args.filedest}_summary.csv", sep='\t', index=False)
    








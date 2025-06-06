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




def apply_seperately(data, source_cols: list[str], dest_col_prefix: str, start_index: int, end_index: int, fn):         
    
    for i in range(0,4): 

        data.loc[start_index, f"{dest_col_prefix}{i}"] = fn(data, source_cols[i], start_index, end_index)


def norm(data) :

    heads = data.columns[4:8]
    newcols = [f"Norm{i}" for i in range(0,4)] 
    data[newcols] = data[heads].apply(lambda row: row/max(row) if max(row) != 0 else [0,0,0,0], axis =1)


def visit_window(data, windows, fn): 

    heads = data.columns[4:8]
    aggfn = apply_seperately
    if "count0" in fn.lower(): 
        k = lambda dat, col, strt, nd : (dat[col][strt:nd].astype(float) == 0).sum()
        prefix = "count0_"
    elif "prod" in fn.lower(): 
        prefix = "prod_"
        k = lambda dat, col, strt, nd : dat[col][strt:nd].astype(float).prod()
    elif "sum" in fn.lower(): 
        prefix = "sum_ "
        k = lambda dat, col, strt, nd : dat[col][strt:nd].astype(float).sum()
    elif "prod_of_norm":
        prefix = "norm_prod_" 
        norm(data)
        heads = [f"Norm{i}" for i in range(0,4)]
        k = lambda dat, col, strt, nd : dat[col][strt:nd].astype(float).prod()
    else:
        raise SyntaxError("No Aggregating Function Specified")


    

    aggfn(data, heads, prefix, 0, windows[0]+1, k)


    index = 0
    while windows != []:
        index = windows.pop(0)
        if windows != []:
            aggfn(data, heads, prefix, index+1, windows[0]+1, k)
        else:
            aggfn(data, heads, prefix, index+1, -1, k)

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
    
    parser.add_argument("--function", type=str, dest="func", choices=["count0", "prod", "sum", "p_of_n"], 
                        default="count0", help="option selection for non-sliding window choose from: count0 prod sum")

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
    








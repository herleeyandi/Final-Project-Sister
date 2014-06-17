import numpy as np
from scipy.spatial import distance
#import pandas as pd
import dispy,random
import pickle
import pdb
import sys
import csv
import pdb
def compute(testdata,centroids):
    import math
    from scipy.spatial import distance 
    min_ed=10000
    prediction=0
    for idx,j in enumerate(centroids): 
        temp_ed=0
        t_ed=distance.euclidean(testdata,j);
        if t_ed < min_ed:                        # find minimal eucledian distance
              min_ed=t_ed
              prediction=idx
    return prediction


if __name__=='__main__':

    print '....initialize manager'
    cluster= dispy.JobCluster(compute,nodes=['192.168.56.*',],ip_addr='192.168.56.101')   # set worker and manager ip address 
    filePath="kddcup_newtestdata.csv"
    print '....reading data'
    csv_file=open("dataResult.txt","wb")                # for writing result to file 
    writer=csv.writer(csv_file,delimiter=",")
    
    dataSet=np.loadtxt(filePath,dtype=float,delimiter=',')
    dataIdx=0                                           # index of datatest
    WorkerN=100                                           # number of job that given to worker

    centroids=pickle.load(open("centroid_pickle.p","rb"))

 #   print("hasil" + str(compute(dataSet[0],centroids)))
    
    # iterate over dataSet and set worker parameter
    print '...assign data to every worker'
    while(dataIdx!=len(dataSet)):
        try:
            jobs=list()
            dataIdx2 = dataIdx
            for n in range(WorkerN):
                jobt=cluster.submit(dataSet[dataIdx],centroids)    # set it to worker funciton
                jobt.id=dataIdx
                dataIdx+=1                              # increase data index
                jobs.append(jobt)    
            for job in jobs:
                result=job()                            # running job in worker
                temp_l=dataSet[dataIdx2].tolist()      # change one row of data to list
                print '>>>',temp_l,' -> ',result
                temp_l.append(result)                   # append prediction 
                writer.writerow(temp_l)
                dataIdx2+=1
            cluster.stats()
        except KeyboardInterrupt:
            sys.exit(0)

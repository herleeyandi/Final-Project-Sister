import numpy as np
import scipy as sc
import pandas as pd
from scipy.cluster.vq import kmeans,vq
import pickle

file="kddcup_testdata.csv"
broken=pd.read_csv(file,sep=",",header=None)
dataset=broken.as_matrix()
centroid,_ = kmeans(dataset,23)
print centroid
print
print len(centroid)
pickle.dump( centroid, open("centroid_pickle.p","wb"))


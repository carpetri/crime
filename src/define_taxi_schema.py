import pandas as pd
import numpy as np
import csv
import os
headers_folder = '../data/taxi_headers/yellow'
files = os.listdir(headers_folder)


headers = {}
for f_name in files:
	ff= os.path.join(headers_folder,f_name)
	with open(ff,'r') as f:
		h = csv.reader(f,delimiter=',')
		for l in h:
			headers[f_name]= np.array(l,dtype='string')
		

head_df = pd.DataFrame(dict( [ (k,pd.Series(v)) for k,v in headers.items() ] ))

head_df.to_csv('../data/taxi_headers/yellow/yellow_columns.csv')
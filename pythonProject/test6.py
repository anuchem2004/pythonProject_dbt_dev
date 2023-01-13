import pandas as pd
import csv
from bs4 import BeautifulSoup
import json
import os

#get path
path=os.getcwd()

# files=['bulk_load_email_dim.kjb','dim_email_job.kjb']
def hrst_master_automation_job():

  if not os.path.isdir(path): os.mkdir(path)
  filename1 = os.path.join(path,'bulk_load_email_dim.kjb')

  print(filename1)
  # df = pd.read_xml("bulk_load_email_dim.kjb")

  with open("bulk_load_email_dim.kjb", 'r') as f:
    data = f.read()
  # print(df)
  Bs_data = BeautifulSoup(data, "xml")

  ###pass specific parameter
  b_parameter = Bs_data.find_all('filename')
  name_parameter = Bs_data.find_all('name')

  # print(b_parameter,name_parameter)
  l=[]
  j=[]
  for r in b_parameter:
    l.append(b_parameter)
  for i in name_parameter.pop():
    j.append(name_parameter)
  # print(l)
  # print(j)
  with open("anu5.csv",'w',newline= "\n" ) as f:
    fo=csv.writer(f)
    fo.writerows([l,j])


  # with open(filename1, 'r') as f:
  #   data = f.read()
  #
  # Bs_data = BeautifulSoup(data, "xml")
  #
  # ### get filename
  #
  # var = Path(filename1).stem
  # print("the filename is:", var)
  # df=pd.data()
  # print(df)

if __name__=="__main__":
  hrst_master_automation_job()
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
import os
import xmltodict

#get path
path=os.getcwd()

def hrst_master_automation_job():

  filename1 = os.path.join(path, 'hrst_master_automation_job.kjb')
  if not os.path.isdir(path): os.mkdir(path)

  ### get filename

  var = Path(filename1).stem
  print("the filename is:", var)

  ### do transformation->get filename
  with open('hrst_master_automation_job.kjb', 'r') as f:
    data = f.read()

  data_dict = xmltodict.parse(data)
  # print(data_dict)

  for i in data_dict:
    print(i[0])


  Bs_data = BeautifulSoup(data, "xml")

  ###read through pandas library
  df= pd.read_xml("hrst_master_automation_job.kjb")
  df1=df.to_csv("anu.csv")
  # print("anu.csv")
  # print("dataframe",df)

  for row in df.iterrows():
    print(row)

  ###pass specific parameter
  b_parameter = Bs_data.find_all('filename')

  for items in b_parameter:
    print(items)


def move_pc_files_to_wip():
  filename1 = os.path.join(path, 'move_pc_files_to_wip.kjb')
  if not os.path.isdir(path): os.mkdir(path)
  # print(path)

 ### do transformation
  with open('move_pc_files_to_wip.kjb', 'r') as f:
    data = f.read()

  Bs_data = BeautifulSoup(data, "xml")
  df = pd.read_xml("move_pc_files_to_wip.kjb")
  ###read the main parameters
  # print("dataframe", df)
  for rows in df.iterrows():
    # print(rows)
    job_parameter= Bs_data.find_all('jobs')
    for all in job_parameter:
      name_tag=Bs_data.find_all("names")
      print(name_tag)

 ###get items by id->parameter:{value,name}
  b_parameter= Bs_data.find_all('parameter')

  for items in b_parameter:
    b_items= Bs_data.find_all('value')
    n_items = Bs_data.find_all('name')

    for ind in n_items:

      for row in b_items:
        # print(ind,row)
        pass
    print(n_items[0], b_items[0])
    print(n_items[1], b_items[1])
    print(n_items[5], b_items[5])

    return ' '

if __name__=="__main__":

  hrst_master_automation_job()
  move_pc_files_to_wip()


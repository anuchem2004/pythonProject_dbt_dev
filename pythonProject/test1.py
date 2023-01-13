import pandas as pd
import csv
from csv import DictWriter
from bs4 import BeautifulSoup
from pathlib import Path
import os

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

  Bs_data = BeautifulSoup(data, "xml")

  ###read through pandas library
  df= pd.read_xml("hrst_master_automation_job.kjb")

  ###pass specific parameter
  b_parameter = Bs_data.find_all('filename')
  name_parameter=Bs_data.find_all('name')

  e_parameter=Bs_data.find_all('entry')
  for x in e_parameter:
    print(str(x[0]))
  print(b_parameter)


  fields=['name','filepath']
  filename="anu1.csv"

  data={}
  i=0

  for name in name_parameter:
    for ame in name:
      if( ame!='condition'):
        data[i]=ame
        i+=1
  # print(data.items())

  ### write to csvfile

  with open(filename, 'w') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow((str(fields).replace(',',"").replace("''","").split(",")))

    a=csvwriter.writerow(str(data).replace(',','').replace("'","").replace("',',","").replace('{','[').replace('}',']').splitlines())
    # Dictwriter.writerow(a,b_parameter)
###validate with reading csvfile
  with open("anu1.csv",'r') as csvfile:
    csvReader=csvfile.read()
    print(csvReader)

# def move_pc_files_to_wip():
#   filename1 = os.path.join(path, 'move_pc_files_to_wip.kjb')
#   if not os.path.isdir(path): os.mkdir(path)
#   # print(path)
#
#  ### do transformation
#   with open('move_pc_files_to_wip.kjb', 'r') as f:
#     data = f.read()
#
#   Bs_data = BeautifulSoup(data, "xml")
#   df = pd.read_xml("move_pc_files_to_wip.kjb")
#   ###read the main parameters
#
#   for rows in df.iterrows():
#     # print(rows)
#     job_parameter= Bs_data.find_all('jobs')
#     e_parameter = Bs_data.find_all('entry')
#     name_parameter = Bs_data.find_all('name')
#
#     for ind in e_parameter:
#       for name in name_parameter:
#         print("name_parameter", name_parameter[1])
#         type_step = Bs_data.find_all('type')
#         print(type_step)
#
#
#     for all in job_parameter:
#       name_tag=Bs_data.find_all("names")
#       print(name_tag[0])
#
#  ###get items by id->parameter:{value,name}
#   b_parameter= Bs_data.find_all('parameter')
#
#   for items in b_parameter:
#     b_items= Bs_data.find_all('value')
#     n_items = Bs_data.find_all('name')
#
#     for ind in n_items:
#
#       for row in b_items:
#         # print(ind,row)
#         pass
#     print(n_items[0], b_items[0])
#     print(n_items[1], b_items[1])
#     print(n_items[5], b_items[5])
#
#     return ' '
#
#
# def move_earlier_processed_files():
#   filename2 = os.path.join(path, 'move_earlier_processed_files.ktr')
#   if not os.path.isdir(path): os.mkdir(path)
#   # print(path)
#
#   ### do transformation
#   with open('move_earlier_processed_files.ktr', 'r') as f:
#     data = f.read()
#
#   ###read the whole file
#   Bs_data = BeautifulSoup(data, "xml")
#
#   ###get items by {transformation,info,notepad}
#   b_parameter = Bs_data.find_all('transformation')
#   df = pd.read_xml("move_earlier_processed_files.ktr")
#   ###read the main parameters
#
#   for items in b_parameter:
#     info_items = Bs_data.find_all('info')
#     print(info_items)
#
#     order_items = Bs_data.find('order')
#
#     ### get the steps and name, type
#     step_items = Bs_data.find_all('step')
#
#     for count in step_items:
#       name_step = Bs_data.find_all('name')
#       print(name_step)
#
#       filemask_step = Bs_data.find('step').find_all('filemask')
#       print(name_step,filemask_step)
#
#       n= Bs_data.find('type')
#
#       print(n)
#   return ' '


if __name__=="__main__":
  hrst_master_automation_job()
  # move_pc_files_to_wip()
  # move_earlier_processed_files()






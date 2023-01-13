from bs4 import BeautifulSoup
import pandas as pd
import os
import csv
from pathlib import Path

def trans_ext_OLAP():

  path=os.getcwd()
  if not os.path.isdir(path): os.mkdir(path)
  filename1 = os.path.join(path, 'get_maint_dt_flag.ktr')

  with open('get_maint_dt_flag.ktr', 'r') as f:
      data = f.read()

  Bs_data = BeautifulSoup(data, "xml")

  y=[]
  for value in (Bs_data.find_all('name')):
    y.append(value.text,"\n")

  t=[]
  for type in (Bs_data.find_all('type')):

    t.append(type.text,"\n")
  h=[]
  for hop in (Bs_data.find_all('hop')):
    h.append(hop.text)

  with open("anu6.csv", 'w',encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["hop","name","type"])
    w.writerow([h])
    w.writerow([y])
    w.writerow([t])

if __name__=="__main__":
  trans_ext_OLAP()




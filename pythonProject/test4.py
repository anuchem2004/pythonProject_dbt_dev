import pandas as pd
import csv
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

  ###pass specific parameter
  # b_parameter = Bs_data.find_all('filename')
  n=[]
  # name_parameter=Bs_data.find_all('name')
  for range in  (Bs_data.find_all('name')):
    job=range.find('type')
    n.append(range.text)
  y=[]
  for value in (Bs_data.find_all('filename')):
    y.append(value.text)
  # e_parameter=Bs_data.find_all('entry')
  e_parameter = Bs_data.find_all('entry')
  t=[]
  for type in (Bs_data.find_all('type')):

    t.append(type.text)

  with open("anu4.csv", 'w') as f:
    w = csv.writer(f)
    w.writerow(["filename","name","type"])
    w.writerows([ n])
    w.writerows([y])
    w.writerows([t])
  df=pd.read_csv("anu2.csv")
  file=[],name=[],type=[]
  output=[item for item in (Bs_data.find_all('filename'))]
  print(output,"\n")
  file.append(output)



  ### write to csvfile
  # for x in  range(0,16):
  #   with open("anu2.csv", 'w') as csvfile:
  #     csvwriter = csv.writer(csvfile)
  #     csvwriter.writerow(str(fields).replace(',',"").replace("''","").split(","))
  #     csvwriter.writerow(str(data).replace(',','').replace("'","").replace("',',","").replace('{','[').replace('}',']').strip().splitlines())
  #   with open("anu1.csv",'w') as csvfile:
  #     csvwriter=csv.writer(csvfile)
  #     csvwriter.writerow(str('filepath'))
  #     csvwriter.writerow(str(b_parameter).strip().splitlines())


##validate with reading csvfile
  with open("anu1.csv",'r') as csvfile:
    csvReader=csvfile.read()


if __name__=="__main__":
  hrst_master_automation_job()
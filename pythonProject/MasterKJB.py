import KettleParser
import pandas as pd
import csv

main_job = KettleParser.Parse("hrst_master_automation_job.kjb")
print(f"Parent Job Name: ", main_job.name)
name = main_job.name

# Entries
jobs_lst = []
type_lst = []
filepath = []
for entry in main_job.steps:
    jobs_lst.append(entry.get_attribute("name"))
    type_lst.append(entry.get_attribute("type"))
    if entry.get_attribute("type") == "JOB":
        filepath.append(entry.get_attribute("filename"))
    elif entry.get_attribute("type") == "SQL":
        filepath.append(entry.get_attribute("sql"))
    else :
        filepath.append('')
print(f"Nested Jobs: ", jobs_lst)
print(f"Type: ", type_lst)
print(f"Filepath :", filepath)


# Hops
from_lst = []
to_lst = []
for hop in main_job.hops:
    from_lst.append(hop.get_attribute("from"))
    to_lst.append(hop.get_attribute("to"))
print(f"From: ", from_lst)
print(f"To : ", to_lst)


df = pd.DataFrame(list(zip(*[jobs_lst, type_lst, filepath, from_lst, to_lst])), columns=['Nested Jobs', 'Type', 'file path', 'From', 'To'])
df.to_csv('hrst_master.csv', index=False)

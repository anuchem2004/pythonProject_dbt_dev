import KettleParser
import pandas as pd
import csv
import xml.etree.ElementTree as ET

mytree=ET.parse('test_DimShippingMethod_Transform.ktr')
myroot=mytree.getroot()

main_job = KettleParser.Parse("C:\\Users\\anuch\\Downloads\\pentaho_jobs_and_transformations-20220928T124912Z-001\\pentaho_jobs_and_transformations\\transformations\\test_DimShippingMethod_Transform.ktr")
print(f"Parent Job Name: ", main_job.name)
name = main_job.name

# Entries
jobs_lst = []
type_lst = []
filepath = []

for entry in main_job.steps:
    jobs_lst.append(entry.get_attribute("name"))
    type_lst.append(entry.get_attribute("type"))

s = []
for x in myroot.iter('sql'):
    print(x.text)
    s.append(str(x.text.replace(",", "")))

# print(f"Nested Jobs: ", jobs_lst)
print(f"SQL List ", s)
print(f"Type: ", type_lst)



# Hops
from_lst = []
to_lst = []
for hop in main_job.hops:
    from_lst.append(hop.get_attribute("from"))
    to_lst.append(hop.get_attribute("to"))
print(f"From: ", from_lst)
print(f"To : ", to_lst)


df = pd.DataFrame(list(zip(*[jobs_lst, type_lst, s, from_lst, to_lst])), columns=['Nested Jobs', 'Type','Filepath', 'From', 'To'])
df.to_csv('C:\\Users\\anuch\\PycharmProjects\\pythonProject\\transf.csv', index=False)

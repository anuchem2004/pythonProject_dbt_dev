import csv
import xml.etree.ElementTree as ET

mytree=ET.parse('test_DimShippingMethod_Transform.ktr')
myroot=mytree.getroot()
name=[]
for x in myroot.iter('step'):
    print(x.text)
    if (x in myroot.findall("name")):
        name.append(str(x.text))
        name.append("\n")
t=[]
for x in myroot.iter('type'):
    print(x.text)
    t.append(str(x.text.replace(",","")))
s=[]
for x in myroot.iter('sql'):
    print(x.text)
    s.append(str(x.text.replace(",","")))
f1=[]
for x in myroot.iter('from'):
    print(x.text)
    f1.append(str(x.text.replace(",","")))
to1=[]
for x in myroot.iter('to'):
    print(x.text)
    to1.append(str(x.text))

with open("anu7.csv", 'w') as f:
    w = csv.writer(f)
    w.writerow(["name","type","sql","from","to"])
    w.writerows([name,t,s,f1,to1])
#     # w.writerows([t])
#     # w.writerows([s])
#     # w.writerows([f1])
#     # w.writerows([to1])
# df = pd.DataFrame(list(zip(*[name,t,s,f1,to1])), columns=["name","type","sql","from","to"])
# df.to_csv('C:\\Users\\anuch\\PycharmProjects\\pythonProject\\anu8.csv', index=False)
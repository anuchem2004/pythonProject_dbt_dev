import csv
import os
import xml.etree.ElementTree as ET

trans_dir=r'Documents'
path=os.getcwd()
filepath=os.path.join(path,'test.ktr')
print(filepath)
mytree=ET.parse('test.ktr')
myroot=mytree.getroot()
name=[]
all_files=[]
output_list=[]

for dirpath,dirs,files in os.walk(trans_dir):
    for i in files:
        if i.split('.')[-1] == 'ktr':
            all_files.append(str(dirpath + "/" + i).replace('\\', '/'))

for xml_data in all_files:
    parsed_tree = ET.parse(xml_data)
    root = parsed_tree.getroot()
    main_job = root.findall('name')
    print(main_job)

    with open("output_jobs.csv", 'w', newline='\n') as fwrite:
        write = csv.writer(fwrite)
        output_list.append(main_job)
        output_list.append(xml_data)
        write.writerow(output_list)
        output_list = []

        for each in root.findall('.//step'):
            name = each.find('name').text
            type = each.find('type').text
            output_list.append(name + "," + type)
            # write.writerow(output_list)
            print(output_list)
            output_list = []
#
# for x in myroot.iter('step'):
#     print(x.text)
#     if (x in myroot.findall("name")):
#         name.append(str(x.text))
#         name.append("\n")
# t=[]
# for x in myroot.iter('type'):
#     print(x.text)
#     if x.tag=='file':
#         t.append(str(x.text))
#     t.append(str(x.text.replace(",","")))
# # files=[]
# # for x in myroot.iter('file'):
# #     print(x.text)
# #     files.append(str(x.text.replace(",","")))
# s=[]
# for x in myroot.iter('sql'):
#     # print(x.text)
#     s.append(str(x.text.replace(",","")))
# f1=[]
# for x in myroot.iter('from'):
#     # print(x.text)
#     f1.append(str(x.text.replace(",","")))
# to1=[]
# for x in myroot.iter('to'):
#     # print(x.text)
#     to1.append(str(x.text))
#
# for each in myroot.findall('.//entry'):
#     name = each.find('name').text
#     type = each.find('type').text
#     file1 = each.find('file').text
#
#     print(file1)
#     if each.find('file').text == 'name':
#         filename = each.find('name').text
#         print(filename)
# with open("anu7.csv", 'w') as f:
#     w = csv.writer(f)
#     w.writerow(["name","type","sql","from","to"])
#     w.writerows([name,t,s,f1,to1])


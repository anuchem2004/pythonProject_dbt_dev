import xml.etree.ElementTree as ETree
from xml.etree.ElementTree import Element
import csv
import os

import pandas as pd

trans_dir = r'C:/Users/anuch/Downloads/pentaho_jobs_and_transformations/pentaho_jobs_and_transformations/transformations/merge_purge'

def extract_trans():
    all_files = []
    n = []
    t = []
    s1=[]
    f = []
    t1 = []
    d=[]
    e=[]
    jscript=[]
    news={}
    all_items=[]
    for dirpath, dirs, files in os.walk(trans_dir):

        for i in files:
            if i.endswith('.ktr'):
                # print(i)
                all_files.append(str(dirpath + "/" + i).replace('\\', '/'))

    for xml_data in all_files:

        parsed_tree = ETree.parse(xml_data)
        root = parsed_tree.getroot()
        print(list(root))

        for child in root.findall('.//step/name'):
            a = child.findtext('name')
            n.append(a)

            b = child.findtext('sql')
            s1.append(b)
            print(b)
            c = child.findtext('type')
            print(c)
            t.append(c)
            # print(list(tuple((child))))

        # for each in pd.DataFrame.iterrows():
            li=(child.findall('filemask'))
            ln=child.findall('name')
            # lj =each.findall('jsScript_script')
            js=list(root.iter('jsScript_script'))
            js1=(child.findtext('./step/name/file/name'))
            jscript.append(js)
            # print(js)
            # print(lj)

            for l in li:
                # print(l.iter())
                d.append(l.iter())
            for l in ln:

                e.append(l.text)
        # for each1 in root.findall('.//step//jsScripts//jsScript_script'):
        #     print(each1.('jsScript_script'))
            # print(root.findall('jsScript_script'))
            # for js in lj:
            #     # print(jname.append(js.find('jsScript_name')))
            #     jscript.append(js)
            #     jname.append(js)
            #     # print(jscript.append(js.find('jsScript_script')))
        for each in root.findall('.//hop'):
            fro= each.find('from').text.replace('[[',"").replace(']',"")
            f.append([fro])
            to = each.find('to').text.replace('[[',"").replace(']',"")
            t1.append([to])
        items=[n,t,s1,f,t1]
        all_items.append(items)


    xmltoDF=pd.DataFrame(all_items,columns=['name','type','sql','from','to'])
    # print(xmltoDF)
    df=pd.DataFrame(list(zip(*[n,t,s1,f,t1])),columns=['name','type','sql','from','to'])
    df.to_csv("anu_trans_test.csv",index=False)

if __name__ == '__main__':
    extract_trans()
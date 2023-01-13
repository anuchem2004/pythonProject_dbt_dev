import os
import re
from joblib import Joblib
from Elem import Elem
import xml.etree.cElementTree as ET

from privateobject import Private_Object

# def loadXML():
# def parseXML(xmlfile):
parent_path = "C:\\Users\\anuch\\Downloads\\pentaho_jobs_and_transformations"

masterparams ={  "hrst_automation_files": 'C:\\Users\\anuch\\Downloads\\pentaho_jobs_and_transformations\\hrst_automation_files',
            "automation_files": 'C:\\Users\\anuch\\Downloads\\pentaho_jobs_and_transformations\\automation_files',
            "Sailthru_API_ETL": 'C:\\Users\\anuch\\Downloads\\pentaho_jobs_and_transformations\\Sailthru_API_ETL'}
mainParams = {
    "sailthru_dir_path":"C:\\Users\\anuch\\Downloads\\pentaho_jobs_and_transformations\\Sailthru_API_ETL",
    "marketingdb_dir_path":"C:\\Users\\anuch\\Downloads\\pentaho_jobs_and_transformations\\\hrst_automation_files",
    "Internal.Job.Filename.Directory":"",
    # "Internal.Entry.Current.Directory":""
}
def removePathvariable(path):
    path=path.replace('/','\\')
    finish =path.find('}')
    return path[finish+1:len(path)]

def removeVariable(path):
    print(f"remoce",{path})
    return path

def loadParameters(job,root,params):
    for param in root.findall("./parameters/parameter"):
        job.parameters[param.find('name').text] = param.find('default_value').text
        params[param.find('name').text] =param.find('default_value').text
    return params


def parseJob(file,path,params,mainParams):
    params = params | mainParams
    job=Joblib()
    try:
        tree = ET.parse(file)
        print(tree)

    except:
        "File not found"
    else:
        print(path)
    finally:
        print("file found")
    root =tree.getroot()
    job.name = root.find("./name").text
    job.filename = file

    try:
        params =loadParameters(job,root,params)
    except Exception as e:
        print(e)

    params = params|mainParams

    for element in root.iter('entry'):
        name = element.find('name').text
        elementType = element.find('type').text
        if elementType == 'JOB':
            filename = element.find('filename').text
            tempFilename = filename
            filename = parent_path + removePathvariable(filename)

            try:
                subJob = parseJob(filename,params)
                subJob.name = name
                subJob.filename = filename
                job.appendjobList(subJob)
            except OSError as e:
                print(f"tempFilename {tempFilename}")
                print(f'error while reading a job {filename} parent {file}')
    return job
# for key,value in params.items():
#
#     # new_file_top = os.path.join(path,(os.path.join(x)))
#     print(f"{key} {value}")

# print(file1)


jobList =[]
job= Joblib()
def getfileNames(path):

    name= os.listdir(path)
    fileNames = []
    for file in name:
        if file.endswith(".kjb"):

            fileNames.append(file)

    return fileNames

# for f in params:
#     print(params[f])
#     new_path= os.listdir(params[f])
#     valid_choices.append(new_path)
#     # print(valid_choices)
#     current_choice=''
#     computer_parts=[]
    # for ind in range(0,len(new_path)):
    #     valid_choices.append(str(ind))
    # print(valid_choices)
        # if current_choice<= len(new_path):


    #     if ind == 'jobs':
    #         valid_jobnames.append(str(ind))
    #         print(valid_jobnames)
    #     elif ind =='transformations':
    #         valid_transnames.append(str(ind))
    #     else:
    #         print("filename",str(ind))


    # print(f.lstrip())
    # if f.lstrip() == "jobs" :
    #     file2 = os.listdir(Pr_ob.filename +"\\"+ f.lstrip())
    #     # print("Parent Jobs folder----------->",file2)
    #     path = Pr_ob.filename+"\\"+f
    #     # print(path)
    #     file5 = os.listdir(path)
        # sub=Pr_ob.subfoldersappend(file2)
        # sub_child=Pr_ob.subfileappend(file2)
        # # (file5)
        # for file6 in file5:
        #     if file6.endswith('.kjb'):
        #         # print(file6,"with .kjb")
        #         ChildList.append(file6)

job=Joblib()
def printjobs(job,tab):
    print(f'{job.name}')
    for jobs in job.jobList:
        print(f'{jobs.name} || {jobs.name}')
        print(jobs,(" -----")+ tab)

path = "C:\\Users\\anuch\\Downloads\\pentaho_jobs_and_transformations"

jobList =[]
# for filename in getfileNames(path):
for filename in ["\\hrst_automation_files\\jobs\\hrst_master_incremental_job.kjb"]:
    try:
        print("parent job"+path + "\\"+filename)
        folderName= path.split('\\')[1]
        parent_path = path + "\\"+ folderName
        job = parseJob(path+"\\" + filename,filename,{},mainParams)
        print(params.keys())
        for val in params.keys():
            print(params.get(val))
        jobList.append(job)
    except OSError as e:
        print(e)

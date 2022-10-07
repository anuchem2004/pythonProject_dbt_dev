import xml.etree.cElementTree as et
from Element import Element
from Job_test import Job
import csv
import os

marketingdb_dir_path = 'C:\\Users\\anuch\\Downloads\\pentaho_jobs_and_transformations\\pentaho_jobs_and_transformations\\jobs'

def removePathVariable(path):
    finish = path.find('}')
    # print(finish)
    # print(path[finish+1:len(path)])
    return (path[finish+1:len(path)])

def parseJob(file):
    print(file)
    job=Job()
    tree=et.parse(file)
    root =tree.getroot()
    job.name= root.find("./name").text
    for element in root.iter('entry'):
        name =element.find('name').text
        elementType= element.find('type').text
        if elementType == 'JOB':
            filename =element.find('filename').text
            filename = marketingdb_dir_path + removePathVariable(filename)
            subJob= parseJob(filename)
            subJob.name = name
            subJob.filename =filename
            job.appendJobList(subJob)
        else:
            element = Element(name, elementType)
            job.appendTaskList(element)
    return job

def printJob(job,tab):
    print(f"{job.name}")
    for task in job.tasklist:
        print(tab + str(task))
    for  subJob in job.jobList:
        print(f"{subJob.name} || { subJob.name}")
        printJob(subJob , ("----"+tab))


def writeCSV(job, jobType,parent, mainParent, writer):
    #print(f"{job.name}")
    writer.writerow([jobType, job.name, parent, mainParent])
    for task in job.taskList:
        writer.writerow([task.type, task.name, job.name, mainParent])
    for subJob in job.jobList:
        writeCSV(subJob, "sub-job",job.name, mainParent, writer)

def getFileNames(path):
    dir_list = os.listdir(path)
    fileNames = []
    for name in dir_list:
        if name.endswith(".kjb"):
            fileNames.append(name)
    return fileNames

path = "C:\\Users\\anuch\\Downloads\\pentaho_jobs_and_transformations"
jobList = []
for filename in getFileNames(path):
    try:
        job = parseJob(path + "\\" + filename)

        jobList.append(job)
    except Exception as e:
        print (e)

with open('job_list_with_hierarchy_trans.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["Type", "JobName", "Parent","MainParentProcess"])
    for job in jobList:
        writeCSV(job, "ParentJob", "",job.name, writer)





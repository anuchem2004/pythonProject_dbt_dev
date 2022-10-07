def removePathVariable(path):
    finish = path.find('}')
    return path[finish+1:len(path)]


import os
# Get the list of all files and directories
path = "C:\\Users\\anuch\\Downloads\\pentaho_jobs_and_transformations\\pentaho_jobs_and_transformations\\jobs"
dir_list = os.listdir(path)

print("Files and directories in '", path, "' :")
fileNames = []
for name in dir_list:
    if name.endswith(".kjb"):
        fileNames.append(name)
        print(name)

def removeVariable(path, dict):
    startIndex = path.find('{')
    if startIndex != -1:
        print(startIndex)
        finishIndex = path.find('}')
        key = path[startIndex+1:finishIndex]
        value = dict.get(key)
        path = path.replace("{"+key+"}",value)
    return path


hash = {}
path = "C:\\Users\\anuch\\Downloads\\pentaho_jobs_and_transformations\\pentaho_jobs_and_transformations\\{database}"
key = "database"
value = "taut"
hash[key] = value
print(removeVariable(path,hash))

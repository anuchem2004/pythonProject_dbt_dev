from itertools import compress

import xml.etree.cElementTree as et

def removeVariable(path, dict):

    startIndex = path.find('$')
    if startIndex != -1:
        #print(startIndex)
        finishIndex = path.find('}')
        key = path[startIndex + 2:finishIndex]
        print(f"key {key}")
        value = dict.get(key)
        print(f"value {value}")
        if value is not None:
            path = path.replace("${" + key + "}", value)
        else:
            return path
        if path.find('$') != -1:
            path = removeVariable(path, dict)
    return path




dist ={"mdb.output.path":"C://",
       "database":"hrst"}
# print(dist)
print(removeVariable("${alterian_folder}",dist))
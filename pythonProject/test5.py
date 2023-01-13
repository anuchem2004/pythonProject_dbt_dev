import json
import os
import pandas as pd

path=os.getcwd()
filename1 = os.path.join(path, 'pythontest.json')
if not os.path.isdir(path): os.mkdir(path)

df=pd.read_json(filename1)
df1=df.to_json(orient='index')
print(df1)
# ro={}
# c=0
# for rows in df.iterrows():
#     ro[c]=rows
#     c+=1
# # print(str(ro[10]))
data={}
jsonDict=json.loads(df1)
# print(type(jsonDict))
# print(jsonDict['hop'])

# print(data)
output=[majorkey for majorkey in jsonDict]
# print(output)

# for output in jsonDict():
#     print(output)
#     # data=json.dumps(jsonDict[majorkey])
#     # print(data[0],data[1],data[2],data[3],data[5],data[6])
#     # print(str(data))
#     # output= [item for item in data]
#     # print(tuple(output))
# print()


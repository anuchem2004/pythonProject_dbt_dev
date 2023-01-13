import KettleParser
import pandas as pd
import os

# assign directory
directory = 'E:\Exeliq\Pentaho_migration\cheetah'

# iterate over files in directory
for subdir, dirs, files in os.walk(directory):
    for file in files:
        print(os.path.join(subdir, file))
        main_tranformation = KettleParser.Parse(os.path.join(subdir, file))
        #print(f"Parent Job Name: ", main_tranformation.name)

        # Steps
        step_lst = []
        type_lst = []
        for step in main_tranformation.steps:
            step_lst.append(step.get_attribute("name"))
            type_lst.append(step.get_attribute("type"))
        #print(f"Steps: ", jobs_lst)
        #print(f"Type: ", type_lst)

        # Hops
        from_lst = []
        to_lst = []
        for hop in main_tranformation.hops:
            from_lst.append(hop.get_attribute("from"))
            to_lst.append(hop.get_attribute("to"))
       # print(f"From: ", from_lst)
       # print(f"To : ", to_lst)

# CSV
df = pd.DataFrame(list(zip(*[step_lst, type_lst, from_lst, to_lst])), columns=['Transformations', 'Type', 'From', 'To'])
#df.to_csv('E:\Exeliq\Pentaho_migration\cheetah\cheetah_trsanformtions.csv', index=False)

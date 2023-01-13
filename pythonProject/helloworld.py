from bs4 import BeautifulSoup
import pandas as pd
with open('for_each_exp_eml_meta_file.kjb', 'r') as f:
    data = f.read()

Bs_data = BeautifulSoup(data, "xml")

###read through pandas library
df = pd.read_xml("for_each_exp_eml_meta_file.kjb")

print("dataframe", df)

for row in df.iterrows():
    print(row)
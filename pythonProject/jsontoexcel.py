import pandas as pd
# df_json = pd.read_json("move_earlier_processed_files.json")
# df_json.to_excel("move_earlier_processed_files.xlsx")
df_json1 = pd.read_json("move_pc_files_to_wip.json")
df_json1.to_excel("move_pc_files_to_wip.xlsx")

# print(df_json.to_excel)
print(df_json1.to_excel)
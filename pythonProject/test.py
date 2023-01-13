from bs4 import BeautifulSoup

with open('move_earlier_processed_files.ktr', 'r') as f:
  data = f.read()

Bs_data= BeautifulSoup(data,"xml")
# print(Bs_data)

#
# directory_name    prefcenter_api
# directory_path    prefcenter_api/pc_pref
# source_file_name  userPref

def directory_name():
  b_parameter= Bs_data.find_all('parameter')

  for items in b_parameter:
    b_items= Bs_data.find('value')
    if (items):
      name= items.find("name")

      for ame in name:
        print(ame)

      for al in b_items:
         print(al)

  return ''

if __name__=="__main__":
  directory_name()

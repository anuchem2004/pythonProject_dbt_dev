import sys

import pandas as pd
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element,ElementTree
from pathlib import Path
import os

# get path
path = os.getcwd()


def move_earlier_processed_files():


    filename2 = os.path.join(path, 'move_earlier_processed_files.ktr')
    file="move_earlier_processed_files.ktr"
    if not os.path.isdir(path): os.mkdir(path)
    tree=ET.parse(file)
        # print(path)
    # except:
    #     print("File not found")
    #     sys.exit()

    ### do transformation
    with open('move_earlier_processed_files.ktr', 'r') as f:
        data = f.read()

    ###read the whole file
    Bs_data = BeautifulSoup(data, "xml")

    data_list=[]
    ###get items by {transformation,info,notepad}
    b_parameter = Bs_data.find_all('transformation')

    for items in b_parameter:
        info_items = Bs_data.find_all('info')
        print(info_items)
        notepad_items = Bs_data.find('notepad')
        print(notepad_items)
        data_list.append(info_items)
    # for book in tree.findall('transformation'):
    #     info = book.findtext('info')
    #     notepad=book.findall('notepad')
    #     order= book.findtext('hops')
    #     print(order)





    return data_list


if __name__=="__main__":
  a=move_earlier_processed_files()
  print(a)
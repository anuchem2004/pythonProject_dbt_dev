import xml.etree.ElementTree as ETree
import csv
import os

output_jobs = r'C:/Users/anuch/Downloads/test_hrst_master_automation_job.csv'
output_trans = r'C:/Users/anuch/Downloads/test_hrst_master_automation_transformations.csv'
output_hops = r'C:/Users/anuch/Downloads/test_hrst_master_automation_hops.csv'
# jobs_dir = r'C:/Users/anuch/OneDrive/Documents/cheetah_job'
jobs_dir = r'C:\Users\anuch\Downloads\pentaho_jobs_and_transformations\pentaho_jobs_and_transformations\jobs'
# trans_dir = r'C:\Users\anuch\OneDrive\Documents\cheetah_transformation'
trans_dir = r'C:/Users/anuch/Downloads/pentaho_jobs_and_transformations/pentaho_jobs_and_transformations/transformations/alterian/model_scores/common_transformation'



def extract_jobs():
    output_list = []
    all_files = []
    for dirpath, dirs, files in os.walk(jobs_dir):
        print(os.walk(jobs_dir))
        for i in files:
            all_files.append(str(dirpath + "/" + i).replace('\\', '/'))
            print(all_files)

    for xml_data in all_files:
        parsed_tree = ETree.parse(xml_data)
        root = parsed_tree.getroot()
        main_job = root.find('name').text
        with open(output_jobs, 'a', newline='\n') as fwrite:
            write = csv.writer(fwrite)
            output_list.append(main_job)
            output_list.append(xml_data)
            write.writerow(output_list)
            output_list = []

            for each in root.findall('.//entry'):
                name = each.find('name').text
                type = each.find('type').text
                file1= each.find('file').text
                print(file1)
                if each.find('type').text == 'JOB':
                    filename = each.find('filename').text
                    new_filename = filename.replace("${marketingdb_dir_path}",
                                                    "C:/Users/swprabha/Documents/pentaho_jobs_and_transformations").replace(
                        "\\", "/")
                    # print(name,",",type,",",new_filename)
                    output_list.append(name + "," + type + "," + new_filename)
                    write.writerow(output_list)
                    print(output_list)
                    output_list = []
                elif each.find('type').text == 'SQL':
                    sql = each.find('sql').text
                    # print(name,",",type,",",sql)
                    output_list.append(name + "," + type + "," + sql)
                    write.writerow(output_list)
                    print(output_list)
                    output_list = []
                else:
                    output_list.append(name + "," + type)
                    write.writerow(output_list)
                    print(output_list)
                    output_list = []


def extract_trans():
    output_list = []
    all_files = []
    for dirpath, dirs, files in os.walk(trans_dir):
        for i in files:
            all_files.append(str(dirpath + "/" + i).replace('\\', '/'))

    for xml_data in all_files:
        print(xml_data)
        parsed_tree = ETree.parse(xml_data)
        root = parsed_tree.getroot()
        #main_job = root.findall('name').text
        with open('output_trans1.csv', 'a', newline='\n') as fwrite:
            write = csv.writer(fwrite)
            #output_list.append(main_job)
            output_list.append(xml_data)
            write.writerow(output_list)
            output_list = []

            for each in root.findall('.//step'):
                name = each.find('name').text
                type = each.find('type').text
                output_list.append(name + "," + type)
                #write.writerow(output_list)
                print(output_list)
                output_list = []


def extract_hops():
    output_list = []
    all_files = []
    for dir_path, dirs, files in os.walk(jobs_dir):
        for i in files:
            all_files.append(str(dir_path + "/" + i).replace('\\', '/'))
    for xml_data in all_files:
        parsed_tree = ETree.parse(xml_data)
        root = parsed_tree.getroot()
        main_job = root.find('name').text
        with open(output_hops, 'a', newline='\n') as fwrite:
            for each in root.findall('.//hop'):
                src = each.find('from').text
                target = each.find('to').text
                output_list.append(main_job)
                output_list.append(src + " ---> " + target)
                write = csv.writer(fwrite)
                write.writerow(output_list)
                print(output_list)
                output_list = []


if __name__ == '__main__':
    extract_jobs()
    # extract_hops()
    # extract_trans()
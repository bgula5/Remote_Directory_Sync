import datetime
import os
import time
import pandas as pd
#import pyodbc
import shutil

'''
if you want to store record remotely use this method
conn = pyodbc.connect(r'Driver={SQL Server};'
                      r'Server=SERVERNAME;'
                      r'Database=DB_NAME;'
                      r'UID=USERNAME'
                      r'PWD=PASSWORD'
                      )
'''


def main():
    dir_name = r'LOCAL_FOLDER'
    recovery_dir = r'D:\recovery'
    current_files = put_into_df(get_list_of_files(dir_name))
    recovery_files = put_into_df(get_list_of_files(recovery_dir))
    new_files = check_record(current_files, recovery_dir)
    if not new_files:
        print("No Changes")
    else:
        update_files(new_files, current_files, recovery_dir)
        print('done')

def get_list_of_files(dir_name):
    # create a list of file and subdirectories
    # names in the given directory
    list_of_files = os.listdir(dir_name)
    all_files = list()
    # Iterate over all the entries
    for entry in list_of_files:
        # Create full path
        full_path = os.path.join(dir_name, entry)
        # If entry is a directory then get the list of files in this directory
        if os.path.isdir(full_path) and not entry.startswith('.'):
            all_files = all_files + get_list_of_files(full_path)
        else:
            all_files.append(full_path)

    return all_files

def put_into_df(list_of_files):
    df = pd.DataFrame(columns=["File_Path", "File_Name", "Date_Modified"])
    for elem in list_of_files:
        # grab the date modified for each file and then grab the current time. Putting everything into a df
        name = os.path.basename(elem).split('/')[-1]
        date_mod = datetime.datetime.strptime(time.ctime(os.path.getmtime(elem)), "%a %b %d %H:%M:%S %Y")
        df = df.append({"File_Path": [elem], "File_Name": [name],"Date_Modified": [date_mod]}, ignore_index=True)
    return df

def check_record(df, recovery_dir):
    empty = []
    diff = []
    if not os.path.exists(r'C:\temp\record.csv'):
        df.to_csv(r'C:\temp\record.csv', mode='w', index=False)
        for index, row in df.iterrows():
            file = str(row['File_Path']).replace("'", '').replace('"', '').replace("\\\\", "\\").replace("[", '').replace("]", '')
            shutil.copy(file, recovery_dir)
        print('initial upload complete')
        return empty
    else:
        record_df = pd.read_csv(r'C:\temp\record.csv')
        compare_df = df

        convert_type = {
            'File_Path': str,
            'Date_Modified': str,
            'File_Name': str
        }
        compare_df = compare_df.astype(convert_type)
        compare_df = compare_df.sort_values('File_Name').reset_index(drop=True)
        record_df = record_df.sort_values('File_Name').reset_index(drop=True)

        compare_df.set_index("File_Path", inplace=True)
        record_df.set_index("File_Path", inplace=True)

        #check for new files/ updated files
        for i, row in compare_df.iterrows():
            if i in set(record_df.index.values):
                if not compare_df.loc[i]['Date_Modified'] == record_df.loc[i]['Date_Modified']:
                    diff.append(i)
                #print(compare_df.loc[i]['Date_Modified'])
                #print(record_df.loc[i]['Date_Modified'])
            else:
                diff.append(i)

        #check for deleted files. if deleted remove from our remote directory
        for x, row in record_df.iterrows():
            if not x in set(compare_df.index.values):
                if os.path.exists(recovery_dir+ "\\" + str(record_df.loc[x]['File_Name']).replace("'", '').replace('"', '').replace("\\\\", "\\").replace("[", '').replace("]", '')):
                    os.remove(recovery_dir+ "\\" + str(record_df.loc[x]['File_Name']).replace("'", '').replace('"', '').replace("\\\\", "\\").replace("[", '').replace("]", ''))
                else:
                    print("already deleted from remote drive")

                print(str(record_df.loc[x]['File_Name']).replace("'", '').replace('"', '').replace("\\\\", "\\").replace("[", '').replace("]", '') + ' deleted')

        return diff


#take the new files/ recently updated files and upload them to the remote database.
def update_files(new_files, current_files, recovery_dir):
    current_files.to_csv(r'C:\temp\record.csv', mode='w', index=False)
    for file in new_files:
        shutil.copy(str(file).replace(r"\\\\", "\\").replace("'", '').replace('"', '').replace("[", '').replace("]", ''), recovery_dir)



if __name__ == '__main__':
    main()
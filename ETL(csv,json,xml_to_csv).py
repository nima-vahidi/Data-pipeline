import glob
import pandas as pd
import xml.etree.ElementTree as et
import datetime as datetime

tempfile="temp.tmp" # file used to store all extracted data
logfile="log.txt" # all event logs will be stored in this file
targetfile="transformed_data.csv" # file where transformed data is stored

def extract_from_csv(file_to_process):
    dataframe=pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe=pd.read_json(file_to_process)

def extract_from_xml(file_to_process):
    dataframe=pd.DataFrame(columns=["name", "age" ,"job_title"])
    tree=et.parse(file_to_process)
    root=tree.getroot()
    for tablename in root:
        #write columns and types for example:
        name= tablename.find("name").text
        age=int(tablename.find("age").text)
        job_title=tablename.find("job_title").text
        dataframe=dataframe.append({"name":name, "age":age, "job_title":job_title}, ignore_index=True)

def extract():
    extracted_data=pd.DataFrame(['name','age','job_title']) # create an empty data frame to hold extracted data
    for csvfile in glob.glob("*.csv"):
        extracted_data = extracted_data.append(extract_from_csv(csvfile), ignore_index=True)

    for jsonfile in glob.glob("*.json"):
        extracted_data = extracted_data.append(extract_from_json(jsonfile), ignoe_index=True)

    for xmlfile in glob.glob("*.xml"):
        extracted_data = extracted_data.append(extract_from_xml(xmlfile), ignore_index=True)

# TRANSFORM

def tranform(data):

    data['age'] = round(data.age)


# Load

def load(targetfile, data_to_load):
    data_to_load.to_csv(targetfile)

# Log file

def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')





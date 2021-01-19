import json
from lxml import etree
import pandas as pd
import urllib.request
import tempfile
import boto3
from datetime import datetime
# import urllib2
import requests
import zipfile
from extract import XMLParser
from os import listdir
from os.path import isfile, join
import os
import shutil
import glob


def clear_tmp():
    files = glob.glob('/tmp/*')
    for f in files:
        os.remove(f)

def lambda_handler(event, context):
    clear_tmp()
    url1=   "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2020-01-08T00:00:00Z+TO+2020-01-08T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
    bucket_name = "dataxmldumps"
    s3folder    = "input/"
    if check_file_status(bucket_name,s3folder)== False:
        dump_input_xml_file(url1,bucket_name,s3folder)
        xml_dump_nm = (read_xml(bucket_name,s3folder,0))
    else:
        print("File Exists")
        xml_dump_nm = (read_xml(bucket_name,s3folder,1))
    
    print(read_schema(bucket_name,"schema/","auth.036.001.02_ESMAUG_DLTINS_1.1.0.xsd"))

    data_pull = XMLParser(xml_file="/tmp/" + xml_dump_nm,
        python_callable=convert_to_csv,
        callable_kwargs={'csv_file': "/tmp/output.csv"},
        schema=read_schema(bucket_name,"schema/","auth.036.001.02_ESMAUG_DLTINS_1.1.0.xsd"))
        

def read_schema(bucket_name,s3folder,filenm):
    pfx = datetime.now().strftime('%Y%m%d')
    zip_dump = '/tmp/' + filenm
    s3 = boto3.client('s3')
    s3.download_file(bucket_name,s3folder + filenm,zip_dump)
    with open( '/tmp/' + filenm, mode='rb') as schema_file:
        schema_xml = schema_file.read() 
    return schema_xml    
    
    
def read_xml(bucket_name,s3folder,ind):
    
    pfx = datetime.now().strftime('%Y%m%d')
    zip_dump = '/tmp/' + pfx + "_" + "data.zip"
    if ind==1:
        s3 = boto3.client('s3')
        s3.download_file(bucket_name,s3folder + pfx + "_" + "data.zip",zip_dump)

    filename = zipfile.ZipFile(zip_dump).namelist()[0]
    with zipfile.ZipFile(zip_dump) as zip_ref:
        zip_ref.extractall("/tmp/")
    return filename
            
            
def check_file_status(bucket_name,s3folder):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    key = "input/"
    pfx = datetime.now().strftime('%Y%m%d')
    objs = list(bucket.objects.filter(Prefix=key))
    if any([pfx in w.key  for w in objs]):
        return True
    else:
        return False
        
def dump_input_xml_file(url1,bucket_name,s3folder):
    pfx = datetime.now().strftime('%Y%m%d')
    with urllib.request.urlopen(url1) as f:
        xml = f.read()
    parser = etree.XMLParser(ns_clean=True, recover=True, encoding='utf-8')
    root = etree.fromstring(xml)
    input_file = []
    for appt in root.getchildren():
        if "result" in appt.tag:
            for elem in appt.getchildren():
                input_file.append(elem.xpath("str[@name='download_link']")[0].text)
    
    url = input_file[0]
    
    zip_dump = '/tmp/'+ pfx + "_" + "data.zip"
    urllib.request.urlretrieve(url, zip_dump)
    # filename = zipfile.ZipFile(zip_dump).namelist()[0]
    
    s3_path =  s3folder  + pfx + "_" + "data.zip"
    
    # with zipfile.ZipFile(zip_dump) as zip_ref:
    #     zip_ref.extractall("/tmp/")
    s3 = boto3.resource("s3")
    s3.Bucket(bucket_name).put_object(Key= s3_path, Body=open(zip_dump, 'rb'))
    
    
def convert_to_csv(element: etree.Element, **kwargs) -> None:
    row = []
    csv_file = kwargs.get('csv_file')
    namespaces = kwargs.get('namespaces')
    # print(element)
    # print(namespaces)
    key_dtc = {}
    key_lst = ["Issr","Id","FullNm","ClssfctnTp","CmmdtyDerivInd","NtnlCcy"]

    if "Issr" in element.tag:
        for e in element.xpath("//*"):
            if e.tag.replace("{" +namespaces["ns"] + "}","") in key_lst:
                key_dtc[e.tag.replace("{" +namespaces["ns"] + "}","")] = e.text
    if "FinInstrmGnlAttrbts" in element.tag:
        for e in element.xpath("//*"):
            if e.tag.replace("{" +namespaces["ns"] + "}","") in key_lst:
                key_dtc[e.tag.replace("{" +namespaces["ns"] + "}","")] = e.text

        for j in set(key_lst) - set(key_dtc.keys()):
          key_dtc[j] = ""

        key_df = pd.DataFrame(columns=key_lst)
        # print(key_df)
        # print(pd.DataFrame([key_dtc]).reset_index())
        key_df = key_df.append(pd.DataFrame([key_dtc]).reset_index(),ignore_index=True)
        # print(key_df)
        
        key_df.to_csv(csv_file, mode='a', header=True)
        print(key_df)
        exit()
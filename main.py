import base64
import csv
import datetime
import gzip
import hashlib
import ntpath
import os
import re
import time
from threading import Event

import pandas as pd
import requests
import simplejson as json
from fsplit.filesplit import Filesplit
from requests.packages.urllib3.exceptions import InsecureRequestWarning

start_time = time.time()


def ail_publish(apikey, manifest_file, file_name, data=None):
    try:
        ail_url = "https://192.168.179.128:7000/api/v1/import/json/item"
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        ail_response = requests.post(ail_url, headers={'Content-Type': 'application/json', 'Authorization': apikey},
                                     data=data, verify=False)
        data = ail_response.json()
        Event().wait(0.5)
        if "status" in ail_response.text:
            if data.get("status") == "success":
                print(file_name + ": Successfully Pushed to Ail")
                filepath = ntpath.join(ntpath.dirname(ntpath.realpath(manifest_file)), file_name)
                if os.path.exists(filepath):
                    previous_file = re.findall(r"[-+]?\d*\.\d+|\d+", file_name.split('_')[1])
                    file_to_del = file_name.split('_')[0] + "_" + str(int(previous_file[0]) - 1) + ".txt"
                    os.unlink(ntpath.join(ntpath.dirname(ntpath.realpath(manifest_file)), file_to_del))
                    remove_split_manifest(manifest_file, "filename", file_name)
                    return True
            if data.get("status") == "error":
                print(data.get("status"))
    except Exception as e:
        print(e)


def check_ail(apikey):
    try:
        ail_ping = "https://192.168.179.128:7000/api/v1/ping"
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        ail_response = requests.get(ail_ping, headers={'Content-Type': 'application/json', 'Authorization': apikey},
                                    verify=False)
        data = ail_response.json()
        if "status" in ail_response.text:
            if data.get("status") == "pong":
                return True
            if data.get("status") == "error":
                print(data.get("status"))
    except Exception as e:
        print(e)


def jsonclean(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


def ail(leak_name, file_name, file_sha256, file_content, manifest_file):
    ail_feeder_type = "Leak_feeder"
    uuid = "17450648-9581-42a6-b7c4-28c13f4664bf"
    ail_api = "cpJOIjpuUMdOu69r3vdJCofeS9On5iRMZC350H5ol"
    print("Checking AIL API...")
    if check_ail(ail_api):
        print("AIL API is GOOD!")
        print("Starting to process content of: " + file_name)
        print("The sha256 of " + file_name + " content is : " + file_sha256)
        list2str = ''.join(str(e) for e in file_content)
        compressed_base64 = base64.b64encode(gzip.compress(list2str.encode('utf-8'))).decode()
        output = {}
        output['source'] = ail_feeder_type
        output['source-uuid'] = uuid
        output['default-encoding'] = 'UTF-8'
        output['meta'] = {}
        output['meta']['Leaked:File Name'] = ntpath.basename(leak_name)
        output['meta']['Leaked:Chunked'] = file_name
        output['data-sha256'] = file_sha256
        output['data'] = compressed_base64
        Event().wait(0.5)
        ail_publish(ail_api, manifest_file, file_name, data=json.dumps(output, indent=4, sort_keys=True, default=str))
    else:
        print("Connection error !")


def split(leak_name, chunk_size, split_only=False):
    dir_path = ntpath.dirname(ntpath.realpath(leak_name))
    manifest_file = dir_path + r"\fs_manifest.csv"
    if ntpath.exists(manifest_file):
        df = pd.read_csv(manifest_file)
        if df.empty:
            print("You need to clean the folder before starting again!")
        else:
            if split_only:
                print("*** You can't 'split only' while the old task didnt finish ***")
            print("Resuming From the last task !!!")
            file_worker(leak_name, dir_path)
    else:
        print("Splitting the File Now")
        Filesplit().split(file=leak_name, split_size=chunk_size, output_dir=dir_path, newline=True)
        print("File split successfully")
        if not split_only:
            file_worker(leak_name, dir_path)
            print("Unable to locate manifest file")


def remove_split_manifest(file, column_name, *args):
    row_to_remove = []
    for row_name in args:
        row_to_remove.append(row_name)
    try:
        df = pd.read_csv(file)
        for row in row_to_remove:
            df = df[eval("df.{}".format(column_name)) != row]
        df.to_csv(file, index=False)
    except Exception as e:
        print(e)


def file_worker(leak_name, dir_path):
    print("Starting to process splits")
    manifest_file = dir_path + r"\fs_manifest.csv"
    if not ntpath.isdir(dir_path):
        print("Input directory is not a valid directory")

    if not ntpath.exists(manifest_file):
        print("Unable to locate manifest file")

    print("Processing Data from splits")
    with open(file=manifest_file, mode="r", encoding="utf-8") as reader:
        manifest_reader = csv.DictReader(f=reader)
        for manifest_files in manifest_reader:
            file_name = manifest_files.get("filename")
            file_content = ntpath.join(dir_path, manifest_files.get("filename"))
            file_size = int(manifest_files.get("filesize"))
            with open(file_content, encoding="utf8", errors='ignore') as f:
                Event().wait(0.5)
                file_lines = f.readlines()
                with open(file_content, "rb") as f:
                    file_sha256 = hashlib.sha256(f.read(file_size)).hexdigest()
                ail(leak_name, file_name, file_sha256, file_lines, manifest_file)
                Event().wait(0.5)
                break
    run_time = (time.time() - start_time)
    print(f"Run time(s): {run_time}")


if __name__ == "__main__":
    Chunks = 1000000
    dir_path = ntpath.dirname(ntpath.realpath(__file__))
    # do not name the leak with numbers or underscore
    split(dir_path + r"\test\France-Hostpital.txt", Chunks, split_only=False)

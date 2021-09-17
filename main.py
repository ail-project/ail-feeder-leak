##################################
# Import External packages
##################################
import base64
import csv
import datetime
import gzip
import hashlib
import os
import re
import shutil
import sys
import threading
import time
from pathlib import Path
from threading import Event

import configargparse
import pandas as pd
import requests
import simplejson as json
from fsplit.filesplit import Filesplit
from requests.packages.urllib3.exceptions import InsecureRequestWarning

##################################
# Import Project packages
##################################
from service.utils import get_list_of_files

start_time = time.time()

sys.setrecursionlimit(10 ** 7)  # max depth of recursion
threading.stack_size(2 ** 27)  # new thread will get stack of such size

# Feeder configuration dict
CONFIG = None

current_leak_filename = "current_leak.txt"
manifest_filename = "fs_manifest.csv"
leak_list_filename = "leak_list.csv"

def ail_publish(apikey, manifest_file, file_name, data=None):
    """
    publish the chunked file to ail and remove it from folder and manifest
    """
    try:
        ail_url = f"{CONFIG.ail_url}/import/json/item"
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        ail_response = requests.post(ail_url, headers={'Content-Type': 'application/json', 'Authorization': apikey},
                                     data=data, verify=False)
        data = ail_response.json()
        Event().wait(CONFIG.wait)
        if "status" in ail_response.text:
            if data.get("status") == "success":
                print(file_name + ": Successfully Pushed to Ail")
                file_by_number = re.findall(r"[-+]?\d*\.\d+|\d+", file_name.split('_')[1])
                file_to_del = file_name.split('_')[0] + "_" + str(int(file_by_number[0]) - 1)
                filepath = os.path.join(os.path.dirname(os.path.realpath(manifest_file)), file_to_del)
                if os.path.exists(filepath):
                    os.unlink(os.path.join(os.path.dirname(os.path.realpath(manifest_file)), file_to_del))
                remove_split_manifest(manifest_file, "filename", file_name)
                return True
            if data.get("status") == "error":
                print(data.get("reason"))
    except Exception as e:
        print(e)


def check_ail(apikey):
    """
    check if AIL instance is available
    """
    try:
        ail_ping = f"{CONFIG.ail_url}/ping"
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        ail_response = requests.get(ail_ping, headers={'Content-Type': 'application/json', 'Authorization': apikey},
                                    verify=False)
        data = ail_response.json()
        if "status" in ail_response.text:
            if data.get("status") == "pong":
                return True
            if data.get("status") == "error":
                return data.get("reason")
    except Exception as e:
        print(e)


def json_clean(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


def ail(leak_name, file_name, file_sha256, file_content, manifest_file):
    """
    prepare the data before sending them to AIL API
    """
    ail_feeder_type = CONFIG.name
    uuid = CONFIG.uuid
    ail_api = CONFIG.api_key
    print("Checking AIL API...")
    check_ail_resp = check_ail(ail_api)
    if check_ail_resp:
        print(f"Starting to process content of: {file_name}")
        print(f"The sha256 of {file_name} content is : {file_sha256}")
        li2str = ''.join(str(e) for e in file_content)
        comp_b64 = base64.b64encode(gzip.compress(li2str.encode('utf-8'))).decode()
        output = {}
        output['source'] = ail_feeder_type
        output['source-uuid'] = uuid
        output['default-encoding'] = 'UTF-8'
        output['meta'] = {}
        output['meta']['Leaked:FileName'] = os.path.basename(leak_name)
        output['meta']['Leaked:Chunked'] = file_name
        output['data-sha256'] = file_sha256
        output['data'] = comp_b64
        Event().wait(CONFIG.wait)
        ail_pub_res = ail_publish(ail_api, manifest_file, file_name,
                                  data=json.dumps(output, indent=4, sort_keys=True, default=str))
        if not ail_pub_res:
            return ail_pub_res
    else:
        return check_ail_resp


def split(leak_name, chunk_size):
    """
    will split the leak into chunks of files based on the config file
    """
    dir_path = os.path.dirname(os.path.realpath(leak_name))
    manifest_file = os.path.join(dir_path, manifest_filename)
    if os.path.exists(manifest_file):
        print("Resuming from the last task")
        file_worker(leak_name, dir_path)
    else:
        print("Splitting the file now")
        Filesplit().split(file=leak_name, split_size=chunk_size, output_dir=dir_path, newline=True)
        print("File split successfully")
        file_worker(leak_name, dir_path)


def remove_split_manifest(file, column_name, *args):
    """
    remove a specific raw from the manifest file
    """
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
    """
    will iterate based on the manifest file line by line and prepare them as a
    list and send them to Ail function
    :param leak_name: input of the file
    :param dir_path: where to process the files
    """
    print("Starting to process splits")
    manifest_file = os.path.join(dir_path, manifest_filename)
    if not os.path.isdir(dir_path):
        print("Input directory is not a valid directory")

    if not os.path.exists(manifest_file):
        print("Unable to locate manifest file")

    print("Processing data from splits")
    with open(file=manifest_file, mode="r", encoding="utf-8") as reader:
        manifest_reader = csv.DictReader(f=reader)
        for manifest_files in manifest_reader:
            file_name = manifest_files.get("filename")
            file_content = os.path.join(dir_path, manifest_files.get("filename"))
            file_size = int(manifest_files.get("filesize"))
            # TODO add a try: except UnicodeDecodeError: to bypass binary files
            with open(file_content, encoding="utf8", errors='ignore') as f:
                Event().wait(CONFIG.wait)
                file_lines = f.readlines()
                with open(file_content, "rb") as f:
                    file_sha256 = hashlib.sha256(f.read(file_size)).hexdigest()
                ail(leak_name, file_name, file_sha256, file_lines, manifest_file)
                Event().wait(CONFIG.wait)
    run()


def folder_cleaner(path):
    """
    Will clean all folders and files in given path
    """
    for root, dirs, files in os.walk(path):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))


def update_leak_list():
    """
    it will update and save the list of leaks inside
    Leaks_Folder dir and return bool, checks for the last task
    :rtype: bool
    """
    dirname = Path(os.path.realpath(__file__))
    cur_dir = os.path.join(dirname.resolve().parent, CONFIG.leaks_folder)
    unprocessed_folder = os.path.join(dirname.resolve().parent, CONFIG.out_folder)

    if not os.listdir(cur_dir):
        cur_dir = os.path.dirname(os.path.realpath(__file__))
        manifest_file = os.path.join(cur_dir, CONFIG.out_folder, manifest_filename)
        if os.path.exists(manifest_file):
            df = pd.read_csv(manifest_file)
            if df.empty:
                return False
            else:
                return True

    list_of_files = get_list_of_files(cur_dir, unprocessed_folder)
    print(f"list_of_files: {list_of_files}")

    df = pd.DataFrame(list_of_files, columns=["Leaks"])
    df.to_csv(leak_list_filename, index=False)
    return True


def end_time():
    """
    will indicate the runtime in seconds when the module finishes
    """
    run_time = (time.time() - start_time)
    print(f"Run time(s): {run_time}")


def move_new_leak():
    """
    this function will move a new leak for work
     if Unprocessed_Leaks is empty else it will continue the previous work
    :rtype: bool
    """
    result = False

    if update_leak_list():
        cur_dir = os.path.dirname(os.path.realpath(__file__))
        leak_list = os.path.join(cur_dir, leak_list_filename)
        file_name = ((pd.read_csv(leak_list).values[0]).tolist())[0]
        leak_source_path = os.path.join(cur_dir, CONFIG.leaks_folder, file_name)
        leak_destination_path = os.path.join(cur_dir, CONFIG.out_folder)
        if os.path.exists(leak_source_path):
            new_location = shutil.move(leak_source_path, leak_destination_path)
            with open(current_leak_filename, "w") as file:
                file.write(new_location)
                file.close()
            result = True
    
    return result


def run():
    """
    Run the feeder
    """
    leaks_folder = CONFIG.leaks_folder
    unprocessed_leaks = CONFIG.out_folder
    unprocessed_folder = CONFIG.unprocessed_folder
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    manifest_file = os.path.join(cur_dir, unprocessed_leaks, manifest_filename)
    chunk_size = CONFIG.chunks
    if not os.path.isdir(leaks_folder):
        os.makedirs(leaks_folder)

    if not os.path.isdir(unprocessed_leaks):
        os.makedirs(unprocessed_leaks)

    if not os.path.isdir(unprocessed_folder):
        os.makedirs(unprocessed_folder)

    if update_leak_list():
        if not os.path.exists(os.path.join(cur_dir, current_leak_filename)):
            print("Starting a new process")
            move_new_leak()
            leak_name = open(current_leak_filename, "r+").read()
            split(leak_name, chunk_size)
        else:
            if os.path.exists(manifest_file):
                df = pd.read_csv(manifest_file)
                if df.empty:
                    print("Cleaning from the last task")
                    folder_cleaner(os.path.join(cur_dir, unprocessed_leaks))
                    run()
                else:
                    print("Processing from the last task")
                    leak_name = open(current_leak_filename, "r+").read()
                    split(leak_name, chunk_size)
            else:
                if move_new_leak():
                    print("Processing new task")
                    leak_name = open(current_leak_filename, "r+").read()
                    split(leak_name, chunk_size)
                else:
                    if os.path.exists(os.path.join(cur_dir, current_leak_filename)):
                        os.remove(os.path.join(cur_dir, current_leak_filename))
                    if os.path.exists(os.path.join(cur_dir, "leak_list.txt")):
                        os.remove(os.path.join(cur_dir, "leak_list.txt"))
                    print("No more leaks to process")
                    end_time()
    else:
        print("Leaks folder is empty !")
        end_time()


if __name__ == "__main__":
    args_parser = configargparse.ArgParser(default_config_files=['config.yaml'])
    args_parser.add('-g', '--config', is_config_file=True, help='Configuration file path.')
    args_parser.add('-n', '--name', help='Name of the feeder.')
    args_parser.add('-l', '--leaks_folder', help='Leaks Folder to parse and send to AIL.')
    args_parser.add('-r', '--out_folder', help='Output Folder of unprocessed split files.')
    args_parser.add('-a', '--unprocessed_folder', help='Output Folder of file that cannot be processed.')
    args_parser.add('-c', '--chunks', type=int, required=True, env_var='FEEDER_LEAKS_CHUNKS',
                    help='Chunks size of split files.')
    args_parser.add('-k', '--api_key', help="API key for AIL authentication.")
    args_parser.add('-u', '--ail_url', help='AIL API URL.')
    args_parser.add('-i', '--uuid', help='Uniq identifier of the feeder.')
    args_parser.add('-w', '--wait', type=float, help='Time sleep between API calls in seconds.')

    options = args_parser.parse_args()
    CONFIG = options

    run()

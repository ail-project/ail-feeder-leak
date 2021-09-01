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
import configargparse
from pathlib import Path
from threading import Event

import string
import unicodedata
import pandas as pd
import requests
import simplejson as json
from fsplit.filesplit import Filesplit
from requests.packages.urllib3.exceptions import InsecureRequestWarning

start_time = time.time()

sys.setrecursionlimit(10 ** 7)  # max depth of recursion
threading.stack_size(2 ** 27)  # new thread will get stack of such size


# Feeder configuration dict
CONFIG = None

valid_filename_chars = f"-_ {string.ascii_letters}{string.digits}"


def ail_publish(apikey, manifest_file, file_name, data=None):
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
                previous_file_by_number = re.findall(r"[-+]?\d*\.\d+|\d+", file_name.split('_')[1])
                file_to_del = file_name.split('_')[0] + "_" + str(int(previous_file_by_number[0]) - 1) + ".txt"
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


def jsonclean(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


def ail(leak_name, file_name, file_sha256, file_content, manifest_file):
    ail_feeder_type = CONFIG.name
    uuid = CONFIG.uuid
    ail_api = CONFIG.api_key
    print("Checking AIL API...")
    check_ail_resp = check_ail(ail_api)
    if check_ail_resp:
        print(f"Starting to process content of: {file_name}")
        print(f"The sha256 of {file_name} content is : {file_sha256}")
        list2str = ''.join(str(e) for e in file_content)
        compressed_base64 = base64.b64encode(gzip.compress(list2str.encode('utf-8'))).decode()
        output = {}
        output['source'] = ail_feeder_type
        output['source-uuid'] = uuid
        output['default-encoding'] = 'UTF-8'
        output['meta'] = {}
        output['meta']['Leaked:FileName'] = os.path.basename(leak_name)
        output['meta']['Leaked:Chunked'] = file_name
        output['data-sha256'] = file_sha256
        output['data'] = compressed_base64
        Event().wait(CONFIG.wait)
        ail_pub_res = ail_publish(ail_api, manifest_file, file_name,
                                  data=json.dumps(output, indent=4, sort_keys=True, default=str))
        if not ail_pub_res:
            return ail_pub_res
    else:
        return check_ail_resp


def split(leak_name, chunk_size):
    dir_path = os.path.dirname(os.path.realpath(leak_name))
    manifest_file = os.path.join(dir_path, "fs_manifest.csv")
    if os.path.exists(manifest_file):
        print("Resuming From the last task")
        file_worker(leak_name, dir_path, chunk_size)
    else:
        print("Splitting the File Now")
        Filesplit().split(file=leak_name, split_size=chunk_size, output_dir=dir_path, newline=True)
        print("File split successfully")
        file_worker(leak_name, dir_path, chunk_size)


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


def file_worker(leak_name, dir_path, chunk_size):
    print("Starting to process splits")
    manifest_file = os.path.join(dir_path, "fs_manifest.csv")
    if not os.path.isdir(dir_path):
        print("Input directory is not a valid directory")

    if not os.path.exists(manifest_file):
        print("Unable to locate manifest file")

    print("Processing Data from splits")
    with open(file=manifest_file, mode="r", encoding="utf-8") as reader:
        manifest_reader = csv.DictReader(f=reader)
        for manifest_files in manifest_reader:
            file_name = manifest_files.get("filename")
            file_content = os.path.join(dir_path, manifest_files.get("filename"))
            file_size = int(manifest_files.get("filesize"))
            with open(file_content, encoding="utf8", errors='ignore') as f:
                Event().wait(CONFIG.wait)
                file_lines = f.readlines()
                with open(file_content, "rb") as f:
                    file_sha256 = hashlib.sha256(f.read(file_size)).hexdigest()
                ail(leak_name, file_name, file_sha256, file_lines, manifest_file)
                Event().wait(CONFIG.wait)
    run()


def folder_cleaner(path):
    for root, dirs, files in os.walk(path):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))


def update_leak_list():
    dirname = Path(os.path.realpath(__file__))
    cur_dir = os.path.join(dirname.resolve().parent, CONFIG.leaks_folder)

    if not os.listdir(cur_dir):
        return False
    list_of_files = sorted(filter(lambda x: os.path.isfile(os.path.join(cur_dir, x)), os.listdir(cur_dir)))

    df = pd.DataFrame(list_of_files, columns=["Leaks"])
    df.to_csv('leak_list.csv', index=False)
    return True


def end_time():
    run_time = (time.time() - start_time)
    print(f"Run time(s): {run_time}")


def move_new_leak():
    if update_leak_list():
        cur_dir = os.path.dirname(os.path.realpath(__file__))
        leak_list = os.path.join(cur_dir, 'leak_list.csv')
        file_name = ((pd.read_csv(leak_list).values[0]).tolist())[0]
        leak_source_path = os.path.join(cur_dir, CONFIG.leaks_folder, file_name)
        leak_destination_path = os.path.join(cur_dir, CONFIG.err_folder)
        if os.path.exists(leak_source_path):
            new_location = shutil.move(leak_source_path, leak_destination_path)
            file = open("current_leak.txt", "w")
            file.write(new_location)
            file.close()
            return True
    else:
        return False


def run():
    leaks_folder = CONFIG.leaks_folder
    unprocessed_leaks = CONFIG.err_folder
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    manifest_file = os.path.join(cur_dir, unprocessed_leaks, "fs_manifest.csv")
    chunk_size = CONFIG.chunks
    if not os.path.isdir(leaks_folder):
        os.makedirs(leaks_folder)

    if not os.path.isdir(unprocessed_leaks):
        os.makedirs(unprocessed_leaks)

    # bellow there is a known bug fix later
    if update_leak_list():
        if not os.path.exists(os.path.join(cur_dir, "current_leak.txt")):
            print("Starting New Process")
            move_new_leak()
            leak_name = open("current_leak.txt", "r+").read()
            split(leak_name, chunk_size)

        else:
            if os.path.exists(manifest_file):
                df = pd.read_csv(manifest_file)
                if df.empty:
                    print("Cleaning last task")
                    folder_cleaner(os.path.join(cur_dir, unprocessed_leaks))
                    run()
                else:
                    print("Processing from the last task")
                    leak_name = open("current_leak.txt", "r+").read()
                    split(leak_name, chunk_size)
            else:
                if move_new_leak():
                    print("Processing new task")
                    leak_name = open("current_leak.txt", "r+").read()
                    split(leak_name, chunk_size)
                else:
                    print("No more leaks to process")
                    if os.path.exists(os.path.join(cur_dir, "current_leak.txt")):
                        os.remove(os.path.join(cur_dir, "current_leak.txt"))
                    if os.path.exists(os.path.join(cur_dir, "leak_list.txt")):
                        os.remove(os.path.join(cur_dir, "leak_list.txt"))
                        end_time()
    else:
        print("Leaks Folder is Empty !")
        end_time()


def clean_filename(filename, whitelist=valid_filename_chars, replace=' '):
    # replace characters
    for r in replace:
        filename = filename.replace(r,'_')

    # keep only valid ascii chars
    cleaned_filename = unicodedata.normalize('NFKD', filename).encode('ASCII', 'ignore').decode()

    # keep only whitelisted chars
    return ''.join(c for c in cleaned_filename if c in whitelist)



# Compressed file
# TODO add compress file management
# #decompress file
# try:
#     if password == None:
#         files = unpack(file_full_path.encode())
#         #print(files.children)
#     else:
#         try:
#             files = unpack(file_full_path.encode(), password=password.encode())
#             #print(files.children)
#         except sflock.exception.IncorrectUsageException:
#             self.abord_file_submission(uuid, "Wrong Password")
#             raise
#         except:
#             self.abord_file_submission(uuid, "file decompression error")
#             raise
#     self.redis_logger.debug('unpacking {} file'.format(files.unpacker))
#     if(not files.children):
#         self.abord_file_submission(uuid, "Empty compressed file")
#         raise
#     # set number of files to submit
#     self.r_serv_log_submit.set(uuid + ':nb_total', len(files.children))
#     n = 1
#     for child in files.children:
#         if self.verify_extention_filename(child.filename.decode()):
#             self.create_paste(uuid, child.contents, ltags, ltagsgalaxies, uuid+'_'+ str(n) , source)
#             n = n + 1
#         else:
#             self.redis_logger.error("Error in module %s: bad extention"%(self.module_name))
#             self.addError(uuid, 'Bad file extension: {}'.format(child.filename.decode()) )

# except FileNotFoundError:
#     self.redis_logger.error("Error in module %s: file not found"%(self.module_name))
#     self.addError(uuid, 'File not found: {}'.format(file_full_path), uuid )

def is_compressed_type(file_type):
    """
    Check if file type is in the list of compressed file extensions format
    """
    compressed_type = ['zip', 'gz', 'tar.gz']

    return file_type in compressed_type


if __name__ == "__main__":

    args_parser = configargparse.ArgParser(default_config_files=['config.yaml'])
    args_parser.add('-g', '--config', is_config_file=True, help='Configuration file path.')
    args_parser.add('-n', '--name', help='Name of the feeder.')
    args_parser.add('-l', '--leaks_folder', help='Leaks Folder to parse and send to AIL.')
    args_parser.add('-r', '--err_folder', help='Output Folder of unprocessed split files.')
    args_parser.add('-c', '--chunks', type=int, required=True, env_var='FEEDER_LEAKS_CHUNKS', help='Chunks size of split files.')
    args_parser.add('-k', '--api_key', help="API key for AIL authentication.")
    args_parser.add('-u', '--ail_url', help='AIL API URL.')
    args_parser.add('-i', '--uuid', help='Uniq identifier of the feeder.')
    args_parser.add('-w', '--wait', type=float, help='Time sleep between API calls in seconds.')

    options = args_parser.parse_args()
    CONFIG = options


    dirname = Path(os.path.realpath(__file__))
    cur_dir = os.path.join(dirname.resolve().parent, CONFIG.leaks_folder)
    list_of_files = sorted(filter(lambda x: os.path.isfile(os.path.join(cur_dir, x)), os.listdir(cur_dir)))
    print(list_of_files)
    for cur_file in list_of_files:
        print(clean_filename(cur_file))


    # run()

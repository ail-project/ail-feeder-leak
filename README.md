# Ail LeakFeeder

AIL LeakFeeder: A Module for AIL Framework that automates the process to feed leaked files automatically to AIL, So basically this feeder will help you ingest AIL with your leaked files automatically.


## How to use it

1- install the requirements
2- add your files inside "leaks_folder" your desired folder can be changed inside config file
3- Run the script

## Requirements

Install the Python dependencies:

```
pip3 install -U -r requirements.txt

@for Pyhton Magic library "python-magic" follow this:
    Debian/Ubuntu: sudo apt-get install libmagic1 
    Windows: pip install python-magic-bin

```
## Config 
###### change it under this file config.yaml

```
# Name of the feeder
name: LeakFeeder
# Leaks Folder to parse and send to AIL
leaks_folder: Leaks_Folder
# Output Folder of unprocessed split files
out_folder: Unprocessed_Leaks
# Output Folder of file that cannot be processed.
unprocessed_folder: Unprocessed_files
# Chunks size of split files
chunks: 100000
# API key for AIL authentication
api_key: <your AIL API key>
# AIL API URL
ail_url: https://<your AIL server>:<AIL port>/api/v1
#  Uniq identifier of the feeder ( https://www.uuidgenerator.net/version4 )
uuid: 17450648-9581-42a6-b7c4-28c13f4664bf
# Time sleep between API calls in seconds
wait: 1
```

## TODO

##### -Iterate inside folders inside "Leaks_Folder"

## Contributors

Thank you so much for all the wonderful contributions ✨🌟


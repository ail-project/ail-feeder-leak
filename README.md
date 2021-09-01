# Ail LeakFeeder

AIL LeakFeeder: A Module for AIL Framework that automates the process to feed leaked files automatically to AIL, So basically this feeder will help you ingest AIL with your leaked files automatically.


## How to use it

##### First Run the script to create the folders for you after installing the requirements

##### You need to add your leaked files into a folder called "Leaks_Folder", Run the script!

## Requirements

Install the Python dependencies:

```
pip3 install -U -r requirements.txt
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
# Chunks size of split files
chunks: 100000
# API key for AIL authentication
api_key: <your AIL API key>
# AIL API URL
ail_url: https://<your AIL server>:<AIL port>/api/v1
# Uniq identifier of the feeder ( https://www.uuidgenerator.net/version4 )
uuid: 17450648-9581-42a6-b7c4-28c13f4664bf
# Time sleep between API calls in seconds
wait: 0.5
```

## TODO

##### -renaming the files to remove _ and .
##### -Decompress automatically files
##### -Iterate inside folder inside "Leaks_Folder"

## Contributors

Thank you so much for all the wonderful contributions ✨🌟


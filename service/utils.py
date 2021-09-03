import os
from pathlib import Path
import shutil
import patoolib
import string
import unicodedata

# Characters authorized in filenames
WHITELISTED_FILENAME_CHARS = f"-() {string.ascii_letters}{string.digits}"


def clean_filename(filename):
    """
    Render a valid filename for the feeder 
    """
    # Remove whitespaces
    cleaned_filename = filename.replace(' ','-')
    # Keep only valid ascii chars
    cleaned_filename = unicodedata.normalize('NFKD', cleaned_filename).encode('ASCII', 'ignore').decode()
    # Keep only whitelisted chars
    return ''.join(c for c in cleaned_filename if c in WHITELISTED_FILENAME_CHARS)


def is_compressed_file_ext(filename):
    """
    Check if filename extension is in the list of allowed compressed file format
    """    
    return filename.lower().endswith(patoolib.ArchiveFormats)


def get_list_of_files(leaks_dir, unprocessed_dir):
    """
    Render a list of leak files
    Uncompress compressed files, sanitize filenames, crush unprocessable files 
    """

    #  Search for compressed files and extract them in Leaks Folder
    list_of_files = sorted(filter(lambda x: os.path.isfile(os.path.join(leaks_dir, x)), os.listdir(leaks_dir)))
    print(list_of_files)
    for cur_file in list_of_files:
        if is_compressed_file_ext(cur_file):
            cur_file = os.path.join(leaks_dir, cur_file)
            patoolib.extract_archive(cur_file, verbosity=0, outdir=leaks_dir, interactive=False)
            # Remove compressed file
            if os.path.exists(cur_file):
                os.unlink(cur_file)
            # TODO Keep trace of original compressed name ?
        # TODO file is binary ?
        # if binary file move to unprocessed

    # Move directories in Unprocessed Folder
    # Only keep flatten uncompressed files
    # TODO manage structured uncompressed files
    list_of_directories = sorted(filter(lambda x: os.path.isdir(os.path.join(leaks_dir, x)), os.listdir(leaks_dir)))
    for cur_dir in list_of_directories:
        source_dir = os.path.join(leaks_dir, cur_dir)
        shutil.move(source_dir, unprocessed_dir)

    # Sanitize filenames
    list_of_files = sorted(filter(lambda x: os.path.isfile(os.path.join(leaks_dir, x)), os.listdir(leaks_dir)))
    for cur_file in list_of_files:
        sanitize_filename = clean_filename(cur_file)
        print(f"sanitize_filename: {sanitize_filename}")
        sanitize_filepath = os.path.join(leaks_dir, sanitize_filename)
        print(f"sanitize_filepath: {sanitize_filepath}")
        os.rename(os.path.join(leaks_dir, cur_file), sanitize_filepath)

    # Get and return reluctant leak files to process
    list_of_files = sorted(filter(lambda x: os.path.isfile(os.path.join(leaks_dir, x)), os.listdir(leaks_dir)))
    return list_of_files


if __name__ == "__main__":
    # zip gunzip, rar, tar

    print(patoolib.ArchiveFormats)

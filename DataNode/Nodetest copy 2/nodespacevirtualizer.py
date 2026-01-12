import os
blocksize=1024*1024*64
maxsize = 1024 * 1024 * 1024  # 1 GB
def format_bytes(size_in_bytes):
    # Define the size units and their corresponding labels
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']

    # Determine the appropriate unit
    unit_index = 0
    while size_in_bytes >= 1000 and unit_index < len(units) - 1:
        size_in_bytes /= 1024.0
        unit_index += 1

    # Format the result with up to 2 decimal places
    formatted_size = "{:.2f} {}".format(size_in_bytes, units[unit_index])
    return formatted_size

def get_folder_size(folder_path):
    try:
        # Get the size of the folder in bytes
        size_bytes = 0
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                size_bytes += os.path.getsize(file_path)
        return size_bytes
    except FileNotFoundError:
        print(f"Error: Folder not found at path {folder_path}")
        return None

def check_folder_size(file_path):
    folder_path = os.path.dirname(os.path.abspath(__file__))
    folder_size = get_folder_size(folder_path)
    dataaddable=False
    if folder_size is not None:
        if(maxsize-folder_size>blocksize):
            dataaddable=True
        size = format_bytes(folder_size)
        precentage_occupied=(folder_size/maxsize)*100
        return([maxsize,precentage_occupied,dataaddable])
    return(None)

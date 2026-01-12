import os

def delete_json_files(directory='.'):
    # Walk through the directory tree
    for root, dirs, files in os.walk(directory):
        # Check if the current directory is a blocklist folder
        if os.path.basename(root) == 'blocklist':
            # Iterate over files in the blocklist folder
            for file in files:
                if(file==".DS_Store"):
                    continue
                file_path = os.path.join(root, file)
                # Delete the JSON file
                os.remove(file_path)
                print(f"Deleted {file_path}")


# Specify the root directory (change it to the path of your 'node' folder)
root_directory = os.path.dirname(os.path.abspath(__file__))

# Call the function with the specified root directory
delete_json_files(root_directory)

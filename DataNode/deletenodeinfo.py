import os

def delete_nodeinfo_files(directory='.'):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file == 'nodeinfo.json':
                file_path = os.path.join(root, file)
                os.remove(file_path)
                print(f"Deleted {file_path}")

if __name__ == "__main__":
    delete_nodeinfo_files()

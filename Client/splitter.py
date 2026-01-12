import json
import os
from PyQt6.QtWidgets import QApplication, QMainWindow, QLabel, QVBoxLayout, QWidget
from PyQt6.QtCore import Qt
returnresult=None
def splitter():
    def drag_enter_event(event):
        mime_data = event.mimeData()

        if mime_data.hasUrls() and mime_data.urls()[0].isLocalFile():
            event.acceptProposedAction()

    def drop_event(event):
        mime_data = event.mimeData()

        if mime_data.hasUrls() and mime_data.urls()[0].isLocalFile():
            file_url = mime_data.urls()[0]
            file_path = file_url.toLocalFile()
            file_name = os.path.basename(file_path)
            print(file_name)
            file_name=file_name.split(".")
            file_name=file_name[0]

            print(f"File dropped: {file_name}")
            process_file(file_path,file_name)
            app.quit()  # Close the application after processing the file

    def process_file(file_path,file_name):
        print("processing")
        result=[]
        print(f"Processing file: {file_path}")
        with open(file_path, "r") as file:
            temp = file.read()
        temp=temp[temp.find("\n")+1:]
        i = 0
        base = 128 * 1000 * 1000
        ct = len(temp)
        check = 0
        switch = False

        while ct > 0:
            if ct > base:
                split = temp[i:(i + base)]
                if switch == True:
                    split = "*^" + split[0:-2]
                    ct += 2
                    i -= 2
                    check -= 2
                    switch = False
                if split[-1] != "}":
                    i -= 2
                    split = split[0:-2] + "^#"
                    ct += 2
                    check -= 2
                    switch = True

            else:
                split = temp[i:]
                if switch == True:
                    split = "*^" + split
            i += base
            ct -= base
            check += len(split)
            # # Modify the output file path accordingly
            # print("here", base, os.path.dirname(os.path.abspath(__file__))+"/partitions/"+file_name+"partition"+str((i//base)+1)+".json)")
            # with open(os.path.dirname(os.path.abspath(__file__))+"/partitions/"+file_name+"partition"+str((i//base)+1)+".json", "w") as file1:
            #     file1.write(split)
            result.append(split)
        global returnresult
        # returnresult=[file_name+"partition",result]
        returnresult=[file_name,result]


    def create_window():
        app = QApplication([])
        window = QMainWindow()
        window.setWindowTitle("File Drop Window")
        window.setGeometry(100, 100, 400, 200)

        central_widget = QLabel("Drag and drop a file here.")
        central_widget.setAlignment(Qt.AlignmentFlag.AlignCenter)

        layout = QVBoxLayout()
        layout.addWidget(central_widget)

        central_widget = QWidget()
        central_widget.setLayout(layout)
        window.setCentralWidget(central_widget)

        window.dragEnterEvent = drag_enter_event
        window.dropEvent = drop_event

        return app, window

    app,window=create_window()
    window.show()
    app.exec()
    print(len(returnresult))
    return(returnresult)

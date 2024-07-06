import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pandas as pd
from insert import Insert


class NewFileHandler(FileSystemEventHandler):

    insert = Insert()
  
    def on_created(self, event):
      
        if event.src_path.endswith('.csv'):

            print(f'-----------> New file detected: {event.src_path}')  
            
            if event.src_path.startswith(r'C:\Users\Samu\OneDrive\Studium\6. Semester\Praxisprojekt\Praktische Umsetzung\daten\Wettersensoren'):
                self.insert.wettersensoren_data(event.src_path)

            elif event.src_path.startswith(r'C:\Users\Samu\OneDrive\Studium\6. Semester\Praxisprojekt\Praktische Umsetzung\daten\Stromsensoren'):
                self.insert.stromsensoren_data(event.src_path)
            
            elif event.src_path.startswith(r'C:\Users\Samu\OneDrive\Studium\6. Semester\Praxisprojekt\Praktische Umsetzung\daten\RaspberryPi'):
                self.insert.raspberry_data(event.src_path)

            else:
                print('-----------> Datei in Ordner RaspberryPi, Stromsensoren oder Wettersensoren ablegen')
    

if __name__ == "__main__":

    path = r'C:\Users\Samu\OneDrive\Studium\6. Semester\Praxisprojekt\Praktische Umsetzung\daten'

    event_handler = NewFileHandler()
    

    observer = Observer()
    

    observer.schedule(event_handler, path, recursive=True)

    observer.start()

    try:
        while True:

            time.sleep(1)
    

    except KeyboardInterrupt:
        observer.stop()
    
    observer.join() 
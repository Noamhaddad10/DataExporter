import sys
sys.setrecursionlimit(sys.getrecursionlimit() * 5)
import PyInstaller.__main__

PyInstaller.__main__.run([
    "logger.py",
    "--hidden-import=pyodbc",
    "--exclude-module=gevent",
    "--exclude-module=tkinter",
    "--exclude-module=PyQt5",
    # "--add-data=create_PDUEncoder/GeneralStructs.csv;csv",
    "--onefile"
])

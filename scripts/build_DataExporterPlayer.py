import PyInstaller.__main__

PyInstaller.__main__.run([
    "DataExporter.py",
    "--hidden-import=pyodbc",
    "--exclude-module=gevent",
    "--exclude-module=tkinter",
    "--noconsole",
    "--onefile"
])

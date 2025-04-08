import os
import psutil
from utils import *

def get_file_size_gb(file_path):
    size_bytes = os.path.getsize(file_path)
    return size_bytes / (1024**3)

def get_available_ram_gb():
    return psutil.virtual_memory().available / (1024**3)

def suggest_framework(file_path):
    file_size_gb = get_file_size_gb(file_path)
    ram_available = get_available_ram_gb()

    log(f"File size: {file_size_gb:.2f} GB")
    log(f"Available RAM: {ram_available:.2f} GB")

    if file_size_gb < 1 and ram_available > 4:
        return "Tip: Use Pandas"
    elif file_size_gb < 5:
        return "Tip: Use Polars"
    elif file_size_gb < 20:
        return "Tip: Use local Spark with local[*] + repartition"
    else:
        return "Tip: Use Spark (if possible, in a cluster) or distributed Dask"


path_file = "/Users/franhan/alfred/data/discogs_20250201_releases.csv"
log(green(suggest_framework(path_file)))

from jugaad_data.nse import bhavcopy_fo_save
from datetime import date
import os

try:
    path = bhavcopy_fo_save(date(2021, 1, 1), ".tmp")
    print(f"Jan 1 2021: {path}")
except Exception as e:
    print(f"Jan 1 2021 Error: {e}")

try:
    path = bhavcopy_fo_save(date(2021, 1, 4), ".tmp")
    print(f"Jan 4 2021: {path}")
except Exception as e:
    print(f"Jan 4 2021 Error: {e}")

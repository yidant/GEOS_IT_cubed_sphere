import sys
import os
import xarray as xr
from datetime import datetime, timedelta, time
from variables import A1, A3cld, A3mstC, A3mstE, A3dyn, I3, CTM_A1, CTM_I1, CN
import warnings

# Ignore specific xarray UserWarning about duplicate dimension names
warnings.filterwarnings("ignore", category=UserWarning, message=".*Duplicate dimension names present.*")

def main():
    if len(sys.argv) != 2:
        print("Usage: python run.py YYYYMMDD")
        sys.exit(1)
    
    # Get the date to run
    date_str = sys.argv[1]
    date_obj = datetime.strptime(date_str, "%Y%m%d")
    date_to_process = date_obj.date()
    YYYY = f"{date_obj.year:04d}"
    MM = f"{date_obj.month:02d}"
    DD = f"{date_obj.day:02d}"
    
    # Define directories
    raw_data_dir = f"/storage1/fs1/rvmartin/Active/GEOS-Chem-shared/ExtData/GEOS_C180/GEOS_IT_Raw/{YYYY}/{MM}/{DD}"
    output_dir = f"/storage1/fs1/rvmartin/Active/t.yidan/geos_it_cubed_sphere/scratch/GEOS_IT/{YYYY}/{MM}"
    
    if not os.path.exists(raw_data_dir):
        print("Invalid Raw Data Directory!")
        sys.exit(2)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Process files for each collection
    IfCompress = 0
    A1(date_to_process, raw_data_dir, output_dir)
    A3cld(date_to_process, raw_data_dir, output_dir)
    A3mstC(date_to_process, raw_data_dir, output_dir)
    A3mstE(date_to_process, raw_data_dir, output_dir)
    A3dyn(date_to_process, raw_data_dir, output_dir)
    I3(date_to_process, raw_data_dir, output_dir)
    
    # CTM_A1(date_to_process, raw_data_dir, output_dir)
    # CTM_I1(date_to_process, raw_data_dir, output_dir)

    # CN(raw_data_dir, output_dir)

if __name__ == "__main__":
    main()


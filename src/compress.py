import xarray as xr
import os
import subprocess

def compress_file(file_path_before, output_dir, file_name_after):
    """
    Compress a NetCDF file using nccopy.
    """
    try:
        ncdump_output = subprocess.check_output(f"ncdump -h {file_path_before}", shell=True).decode()
        
        nXdim = int([line for line in ncdump_output.split('\n') if "Xdim =" in line][0].split('=')[1].strip().strip(';'))
        nYdim = int([line for line in ncdump_output.split('\n') if "Ydim =" in line][0].split('=')[1].strip().strip(';'))
        
        # TODO: CHANGE COMPRESS METHOD HERE
        compress_command = f"nccopy -k3 -d5 -c Xdim/{nXdim},Ydim/{nYdim},nf/1,lev/1,time/1 {file_path_before} {output_dir}/{file_name_after}"
        
        # Execute the compression command
        subprocess.run(compress_command, shell=True, check=True)
        print(f"Compressed file saved to {output_dir}/{file_name_after}")
    except Exception as e:
        print(f"An error occurred during compression: {e}")
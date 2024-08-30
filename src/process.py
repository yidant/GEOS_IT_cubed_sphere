import xarray as xr
import numpy as np
import os
import subprocess
from datetime import datetime, timedelta, time
from attribute import change_attrs

class Collection:
    def __init__(self, processed_collection, time_frequency, time_offset):
        self.processed_collection = processed_collection
        self.time_frequency = time_frequency
        self.time_offset = time_offset

    def concatenate_daily_files(self, datasets, date, output_dir):
        """
        Concatenate multiple hourly files into a single daily file.
        """
        try:
            combined = xr.concat(datasets, dim="time")
            
            # Create the output file path
            daily_filename = f"GEOSIT.{date.strftime('%Y%m%d')}.{self.processed_collection}.nc4"
            daily_file_path = os.path.join(output_dir, daily_filename)

            # Change attributes
            # change_attrs(combined, daily_filename)

            combined.to_netcdf(daily_file_path)
            
            print(f"Daily file saved to {daily_file_path}")
            return daily_file_path
        except Exception as e:
            print(f"An error occurred while concatenating files: {e}")

    def process_files_for_date(self, directory, variable_map, date, output_dir, compute_func=None):
        """
        Process all files in a directory for a given collection and date.
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        current_time = datetime.combine(date, self.time_offset)
        end_time = current_time + timedelta(days=1)
        
        datasets = []
        
        while current_time < end_time:
            time_str = current_time.strftime('%Y-%m-%dT%H%M')
            
            for collection_name, variables in variable_map.items():
            
                filename = f"GEOS.it.asm.{collection_name}.GEOS5294.{time_str}.V01.nc4"
                file_path = os.path.join(directory, filename)
            
                if os.path.exists(file_path):
                    ds = xr.open_dataset(file_path)
                    
                    # Select the specific variables
                    selected_vars = ds[variables]

                    # Perform custom computations if a compute function is provided
                    if compute_func:
                        selected_vars = compute_func(selected_vars)

                    datasets.append(selected_vars)
                else:
                    print(f"File {file_path} does not exist.")
            
            current_time += self.time_frequency
        
        if datasets:
            return self.concatenate_daily_files(datasets, date, output_dir)



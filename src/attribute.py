import xarray as xr
import os
from datetime import datetime, timezone

class File_Attributes:
    def __init__(self, ds):
        self.ds = ds

    def inspect_attr(ds):
        print("Global attributes:")
        for attr in ds.attrs:
            print(f"{attr}: {ds.attrs[attr]}")
        
        for var in ds.data_vars:
            print(f"\nAttributes for variable '{var}':")
            for attr in ds[var].attrs:
                print(f"  {attr}: {ds[var].attrs[attr]}")

    def add_global_attr(ds, attr, value):
        ds.attrs[attr] = value
    
    def add_variable_attr(ds, var, attr, value):
        ds[var].attrs[attr] = value
    
    def delete_global_attr(ds, attr):
        if attr in ds.attrs:
            del ds.attrs[attr]
    
    def delete_variable_attr(ds, var, attr):
        if attr in ds[var].attrs:
            del ds[var].attrs[attr]


def change_attrs(ds, file_name):
    current_time = datetime.now(timezone.utc).strftime("%Y/%m/%d %H:%M:%S GMT+0000")

    File_Attributes.add_global_attr(ds, "Title", "GEOS-IT diagnostics (cubed-sphere grid), processed for GEOS-Chem input")
    File_Attributes.add_global_attr(ds, "Contact", "GEOS-Chem Support Team (geos-chem-support@as.harvard.edu)")
    File_Attributes.add_global_attr(ds, "References", "www.geos-chem.org")
    File_Attributes.add_global_attr(ds, "RangeEndingTime", "23:59:59.999999")
    File_Attributes.add_global_attr(ds, "Filename", file_name)
    File_Attributes.add_global_attr(ds, "History", f"File generated on: {current_time}")
    File_Attributes.add_global_attr(ds, "ProductionDateTime", current_time)

    File_Attributes.delete_global_attr(ds, "Institution")
    File_Attributes.delete_global_attr(ds, "GranuleID")
    File_Attributes.delete_global_attr(ds, "Comment")

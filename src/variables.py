import sys
import os
import xarray as xr
from datetime import datetime, timedelta, time
from process import Collection

def A1(date_to_process, raw_data_dir, output_dir):
    collection_A1 = Collection(
        "A1", 
        timedelta(hours=1),
        time(hour=0, minute=30)
    )
    variable_map = {
        "rad_tavg_1hr_glo_C180x180x6_slv": ["ALBEDO", "CLDTOT", "LWGNT", "SWGDN"],
        "flx_tavg_1hr_glo_C180x180x6_slv": ["EFLUX", "EVAP", "FRSEAICE", "HFLUX", "PBLH", "PRECANV", "PRECCON", "PRECLSC", "PRECSNO", "PRECTOT"],
        "lnd_tavg_1hr_glo_C180x180x6_slv": ["FRSNO", "GRN", "GWETROOT", "GWETTOP", "LAI", "PARDF", "PARDR", "SNODP", "SNOMAS"],
        "slv_tavg_1hr_glo_C180x180x6_slv": ["QV2M", "SLP", "TO3", "TROPPT", "TS", "T2M", "U10M", "V10M"]
    }

    collection_A1.process_files_for_date(raw_data_dir, variable_map, date_to_process, output_dir)

def A3cld(date_to_process, raw_data_dir, output_dir):
    collection_A3cld = Collection(
        "A3cld", 
        timedelta(hours=3),
        time(hour=1, minute=30)
    )
    variable_map = {
        "cld_tavg_3hr_glo_C180x180x6_v72": ["CFAN", "CFCU", "CFLS", "TAUCLI", "TAUCLW"],
        "rad_tavg_3hr_glo_C180x180x6_v72": ["CLOUD"],
        "asm_tavg_3hr_glo_C180x180x6_v72": ["QI", "QL"]
    }

    def A1_compute(ds):
        if 'TAUCLI' in ds and 'TAUCLW' in ds:
            ds['OPTDEPTH'] = ds['TAUCLI'] + ds['TAUCLW']
            ds['OPTDEPTH'].attrs['long_name'] = 'Optical Depth'
            ds['OPTDEPTH'].attrs['units'] = ds['TAUCLI'].attrs.get('units', 'unknown')
        return ds

    collection_A3cld.process_files_for_date(raw_data_dir, variable_map, date_to_process, output_dir, A1_compute)

def A3mstC(date_to_process, raw_data_dir, output_dir):
    collection_A3mstC = Collection(
        "A3mstC", 
        timedelta(hours=3),
        time(hour=1, minute=30)
    )
    variable_map = {
        "mst_tavg_3hr_glo_C180x180x6_v72": ["DQRCU", "DQRLSAN", "REEVAPCN", "REEVAPLSAN"]
    }
    collection_A3mstC.process_files_for_date(raw_data_dir, variable_map, date_to_process, output_dir)

def A3mstE(date_to_process, raw_data_dir, output_dir):
    collection_A3mstE = Collection(
        "A3mstE", 
        timedelta(hours=3),
        time(hour=1, minute=30)
    )
    variable_map = {
        "mst_tavg_3hr_glo_C180x180x6_v73": ["CMFMC", "PFICU", "PFILSAN", "PFLCU", "PFLLSAN"]
    }
    collection_A3mstE.process_files_for_date(raw_data_dir, variable_map, date_to_process, output_dir)

def A3dyn(date_to_process, raw_data_dir, output_dir):
    collection_A3dyn = Collection(
        "A3dyn", 
        timedelta(hours=3),
        time(hour=1, minute=30)
    )
    variable_map = {
        "cld_tavg_3hr_glo_C180x180x6_v72": ["DTRAIN"],
        "asm_tavg_3hr_glo_C180x180x6_v72": ["OMEGA", "RH", "U", "V"]
    }
    collection_A3dyn.process_files_for_date(raw_data_dir, variable_map, date_to_process, output_dir)

def I3(date_to_process, raw_data_dir, output_dir):
    collection_I3 = Collection(
        "I3", 
        timedelta(hours=3),
        time(hour=0, minute=0)
    )
    variable_map = {
        "asm_inst_3hr_glo_C180x180x6_v72": ["PS", "QV", "T"]
    }
    collection_I3.process_files_for_date(raw_data_dir, variable_map, date_to_process, output_dir)

def CN(date_to_process, raw_data_dir, output_dir):
    collection_CN = Collection(
        "CN", 
        # timedelta(hours=3),
        # time(hour=0, minute=0)
    )
    variable_map = {
        # "asm_inst_3hr_glo_C180x180x6_v72": ["PS", "QV", "T"]
    }
    collection_CN.process_files_for_date(raw_data_dir, variable_map, date_to_process, output_dir)

def CTM(date_to_process, raw_data_dir, output_dir):
    collection_CTM = Collection(
        "CTM", 
        timedelta(hours=1),
        time(hour=0, minute=30)
    )
    variable_map = {
        "ctm_tavg_1hr_glo_C180x180x6_v72": ["CX", "CY", "DELP", "MFXC", "MFYC", "PS"],
        "ctm_inst_1hr_glo_C180x180x6_v72": ["PS", "QV"]
    }
    collection_CTM.process_files_for_date(raw_data_dir, variable_map, date_to_process, output_dir)
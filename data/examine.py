import netCDF4 as nc

def examine_nc_file(file_path):
    ds = nc.Dataset(file_path)
    
    print("Dimensions:")
    for dim in ds.dimensions.values():
        print(dim)
    
    print("\nVariables:")
    for var in ds.variables.values():
        print(var)
    
    # 查看时间变量的数据
    time_var = ds.variables['time']
    print("\nTime variable data:")
    print(time_var[:])

if __name__ == "__main__":
    file_path = '/home/azureuser/Storm-Oracle/data/raw/noaa/sst/2023/01/oisst-avhrr-v02r01.20230101.nc'
    #examine_nc_file(file_path)
    examine_nc_file(file_path)

import xarray as xr
import matplotlib.pyplot as plt
import matplotlib.animation as animation

def animate_sst(file_path):
    ds = xr.open_dataset(file_path)
    sst = ds['sst']
    sst_region = sst.sel(lat=slice(10, 20), lon=slice(100, 120))

    fig, ax = plt.subplots(figsize=(10, 6))
    cbar = None

    def update(time_index):
        nonlocal cbar
        ax.clear()
        im = sst_region.isel(time=time_index, zlev=0).plot(ax=ax, add_colorbar=False)
        if cbar is None:
            cbar = fig.colorbar(im, ax=ax, orientation='vertical')
        else:
            cbar.update_normal(im)
        ax.set_title(f'Sea Surface Temperature (SST) - Time Index: {time_index}')
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')

    ani = animation.FuncAnimation(fig, update, frames=sst_region.sizes['time'], repeat=False)
    ani.save('sst_animation.gif', writer='imagemagick', fps=2)
    print("Animation saved as sst_animation.gif")

if __name__ == "__main__":
    file_path = '/home/azureuser/Storm-Oracle/data/raw/noaa/sst/2023/01/oisst-avhrr-v02r01.20230101.nc'
    #examine_nc_file(file_path)
    animate_sst(file_path)

# imports
import os
from osgeo import (
    gdal, 
    ogr,
    osr,
    gdalconst
)
import numpy as np
import geopandas as gpd
from tempfile import TemporaryDirectory

def rasterize_points(points:str, raster:str, nodata:int, dtype:str, lcc_raster:str, pixel_size:int=1.0, use_vector_spatial:bool=False):

    # Open the vector data source to be rasterized
    source_ds = ogr.Open(points)
    source_layer = source_ds.GetLayer()

    if use_vector_spatial:
        # get geotransform, and rows and cols from vector spatial extent info
        # extent
        x_min, x_max, y_min, y_max = source_layer.GetExtent()

        # projection
        srs = source_layer.GetSpatialRef()

        # number of rows and columns
        n_cols = int((x_max - x_min) / pixel_size)
        n_rows=  int((y_max - y_min) / pixel_size)

        # geotransform
        geotransform = (x_min, pixel_size, 0, y_max, 0, -pixel_size)
    
    else:
        # get geotransform, and rows and cols from another raster's spatial extent info
        lcc_ds = gdal.Open(lcc_raster, gdalconst.GA_ReadOnly)

        # projection
        prj=lcc_ds.GetProjection()
        srs=osr.SpatialReference(wkt=prj)

        # number of rows and columns
        n_cols = lcc_ds.RasterXSize
        n_rows=  lcc_ds.RasterYSize

        # geotransform
        geotransform = lcc_ds.GetGeoTransform()

    # Create the destination data source
    target_ds = gdal.GetDriverByName('GTiff').Create(raster, n_cols, n_rows, 1, dtype)
    target_ds.SetGeoTransform(geotransform)
    band = target_ds.GetRasterBand(1)
    band.SetNoDataValue(nodata)
    target_ds.SetProjection(srs.ExportToWkt())

    # Rasterize
    print("rasterizing")
    gdal.RasterizeLayer(target_ds, 
                        [1], source_layer, 
                        options = ["ATTRIBUTE=uid"]) # "ALL_TOUCHED=TRUE",
    
    print("flushing")
    
    target_ds.FlushCache()
    del target_ds

def vectorize_points(raster:str, path:str, state:str):
    #  get raster datasource
    src_ds = gdal.Open( raster )

    #
    srcband = src_ds.GetRasterBand(1)
    dst_layername = f"sample"
    drv = ogr.GetDriverByName("GPKG")

    sp_ref = osr.SpatialReference()
    sp_ref.SetFromUserInput('EPSG:5070')

    gpkg_path = f"{path}/{state}.gpkg"
    dst_ds = drv.CreateDataSource( gpkg_path )
    dst_layer = dst_ds.CreateLayer(dst_layername, srs = sp_ref )

    dst_layer.CreateField(ogr.FieldDefn("uid", ogr.OFTInteger))
    dst_field = 0

    print("poylgonizing")
    gdal.Polygonize( srcband, None, dst_layer, dst_field, [], callback=None )

    # convert to points
    print("converting to points")
    gdf = (
        gpd.read_file(gpkg_path, layer=dst_layername)
        .query("uid != 0")
    )
    gdf.crs = "EPSG:5070"
    gdf.loc[:, 'geometry'] = gdf.geometry.centroid
    gdf.to_file(gpkg_path, layer='points', driver="GPKG")
    gdf.loc[:, 'geometry'] = gdf.geometry.buffer(1.5, cap_style='square')
    gdf.to_file(gpkg_path, layer='buffer_3x3', driver="GPKG")


def dtype_helper(max_val:int):
    if max_val <= 255:
        return gdalconst.GDT_Byte
    elif max_val <= 65535:
        return gdalconst.GDT_UInt16
    elif max_val <= 4294967295:
        return gdalconst.GDT_UInt32
    elif max_val <= 18446744073709551615:
        return gdalconst.GDT_UInt64
    else:
        raise TypeError(f"maximum value too large for integer data type: {max_val}")

if __name__ == "__main__":

    # folder paths
    input_folder = r"" # folder with points data and LCC rasters
    output_folder = r"" # path to folder to write results

    # file paths
    points_path = f"{input_folder}/clean_points/lcc_aa_points_cleaned.gpkg"

    # Define pixel_size and NoData value of new raster
    pixel_size = 1.0
    nodata = 0

    # open points
    points = gpd.read_file(points_path, layer='AA_clean')

    # TODO iterate states -- for now just filter DC for testing
    with TemporaryDirectory() as temp_dir:

        vector_data = os.path.join(temp_dir, 'temp_vectors.fgb')

        #  query out points for the state
        pts = (
            points
            .query("state == 'DC'")
            .filter(items=['uid', 'geometry'], axis=1)
        )

        # get smallest int dtype
        mx_val = np.max(pts['uid'])
        dtype = dtype_helper(mx_val)

        # write queried data to temp directory
        pts.to_file(vector_data, driver='FlatGeobuf')
        pts = None

        # rasterize the points to the same grid as the lc change raster
        lcc_raster = f"{input_folder}/lcc_rasters/DC/wash_11001_landcoverchange_2013_2021.tif"
        output_raster = f"{output_folder}/DC.tif"
        rasterize_points(vector_data, output_raster, nodata, dtype, lcc_raster)

    # vectorize cell centroids and create 3x3 square buffer window
    vectorize_points(output_raster, output_folder, "DC")
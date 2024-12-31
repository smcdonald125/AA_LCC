import geopandas as gpd
from osgeo import (
    gdal,
    ogr, 
    osr
)
import libpysal
import dask_geopandas

def vectorize(raster:str, path:str, state:str, field_name:str):
    print("Vectorizing change")

    #  get raster datasource
    src_ds = gdal.Open( raster )

    # get first banch
    srcband = src_ds.GetRasterBand(1)

    # Get the band's NoData value
    dst_layername = f"lcc"
    drv = ogr.GetDriverByName("GPKG")

    sp_ref = osr.SpatialReference()
    sp_ref.SetFromUserInput('EPSG:5070')

    gpkg_path = f"{path}/{state}_change.gpkg"
    dst_ds = drv.CreateDataSource( gpkg_path )
    dst_layer = dst_ds.CreateLayer(dst_layername, srs = sp_ref )

    dst_layer.CreateField(ogr.FieldDefn(field_name, ogr.OFTInteger))
    dst_field = 0

    print("poylgonizing")
    gdal.Polygonize( srcband, srcband, dst_layer, dst_field, [], callback=None )

    # read in, query out no data, and overwrite
    gdf = (
        gpd.read_file(gpkg_path, layer=dst_layername)
        .query("Value != 65535")
        .to_file(gpkg_path, layer=dst_layername, driver="GPKG")
    )

    # return
    return gdf

def dissolve_by_contiguity(
        gdf: gpd.GeoDataFrame,
        single_part: bool,
        aggfunc: dict) -> gpd.GeoDataFrame:

    """
    dissolve_by_contiguity

    Dissolve features in the input GeoDataframe based on their connectivity.
    Passes the data frame to libpysal to generate weights,
        returning a list corresponding to group membership
        for each feature. Then dissolves on the membership list.

    Parameters
    ----------
    gdf : GeoDataFrame
        GeoDataFrame containing the data to dissolve
    single_part : bool
        Boolean determining if polygons need to be multipart
    aggfunc : dict
        Dictionary of attributes to aggregate and method of aggregation

    Returns
    -------
    GeoDataFrame
        Dissolved geometry data
    """

    # Pass data to libpysal to obtain grouping by connectivity
    weights = libpysal.weights.fuzzy_contiguity(
        gdf,
        silence_warnings=True
    )

    # Return group membership list from weights object
    components = weights.component_labels

    if single_part:
        if aggfunc:
            dissolve_df = (
                gdf.dissolve(by=components, aggfunc=aggfunc)
                .explode(index_parts=True) # Reduce multipart to single part
                .reset_index(drop=True)
            )
        else:
            dissolve_df = (
                gdf.dissolve(by=components)
                .explode(index_parts=True) # Reduce multipart to single part
                .reset_index(drop=True)
            )   
    else:
        if aggfunc:
            dissolve_df = (
                gdf.dissolve(by=components, aggfunc=aggfunc)
                .reset_index(drop=True)
            )
        else:
            dissolve_df = (
                gdf.dissolve(by=components)
                .reset_index(drop=True)
            )   

    return dissolve_df

def dissolve(
    gdf: gpd.GeoDataFrame,
    by: str = None,
    single_part: bool = True,
    aggfunc: dict = None,
    ) -> gpd.GeoDataFrame:
    """
    dissolve

    Helper function to dissolve geometries in an input geodatabase.
      Handles dissolve based on the size of the GeoDataFrame.
      For dataframes larger than the dask Chunksize an initial dissolve
      is done by converting to a Dask GeoDataFrame and mapping partitions to
      the dissolve method. A second round of dissolve is done using the
      dissolve by contiguity function to reduce edge effects from partition strategy.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        GeoDataFrame to dissolve
    by : str, Optional Default None
        If provided, dissolve on the specified column.
    single_part : bool, Optional Default True
        Defaults to converting polygons to single-part. If False is provided, don't explode multi-part polygons.
    aggfunc : dict, Optional Default None
        Defaults to None. If passed, aggregate attributes by specified function. 

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame containing the dissolve geometries.
    """
    if len(gdf) > 20:

        if by:
            # Re-index using the columns in the by parameter if passed
            gdf = gdf.set_index(by)

        ddf = dask_geopandas.from_geopandas(
                gdf,
                chunksize=20,
                # If by column is passed, sort on the index
                #  created from those columns to ensure cleanly divided partitions
                sort=bool(by),
        )

        if aggfunc:
            dask_dissolve = (
                ddf
                .map_partitions(
                        gpd.GeoDataFrame.dissolve,
                        by=by,
                        aggfunc=aggfunc,
                        )
                .compute()
            )
        else:
            dask_dissolve = (
                ddf
                .map_partitions(
                        gpd.GeoDataFrame.dissolve,
                        by=by,
                        )
                .compute()
            )    

        if by:
            # If by column is specified, the dask dissolve is the last step
            if single_part:
                output_dissolve = (
                    dask_dissolve
                        .explode(index_parts=False)
                        .reset_index(
                            by,
                            drop=False
                        )
                )
            else:
                output_dissolve = (
                    dask_dissolve
                        .reset_index(
                            by,
                            drop=False
                        )
            )

        else:
            # If no by column(s) were specified the values returned from dask dissolve
            #   may need further dissolving based on contiguity since the division
            #   into chunks for dask is arbitrary.
            contiguity_dissolve = dissolve_by_contiguity(
                dask_dissolve.explode(index_parts=False).reset_index(drop=True),
                single_part=single_part,
                aggfunc=aggfunc
                )

            output_dissolve = contiguity_dissolve

    else:
        if single_part:
            if aggfunc:
                output_dissolve = (
                    gdf
                        .dissolve(by=by, aggfunc=aggfunc)
                        .explode(index_parts=True)
                        .reset_index(drop=not bool(by))
                )
            else:
                output_dissolve = (
                    gdf
                        .dissolve(by=by)
                        .explode(index_parts=True)
                        .reset_index(drop=not bool(by))
                )
        else:
            if aggfunc:
                output_dissolve = (
                    gdf
                        .dissolve(by=by, aggfunc=aggfunc)
                        .reset_index(drop=not bool(by))
                )     
            else:
                output_dissolve = (
                    gdf
                        .dissolve(by=by)
                        .reset_index(drop=not bool(by))
                )      

    return output_dissolve

def dissolve_change(path:str, gdf:gpd.GeoDataFrame=None):
    print("Dissolving")
    
    # read in file
    if gdf is None:
        gdf = gpd.read_file(path, layer='lcc')

    # dissolve all changee
    gdf = dissolve(gdf, single_part=True)

    # write dissolved change
    gdf.to_file(path, layer="lcc_dis", driver="GPKG")

    return gdf

def calculate_distance(poly_gdf:gpd.GeoDataFrame, pts_gdf:gpd.GeoDataFrame, fld_name:str=None):
    # set field name
    if fld_name is None:
        fld_name = "distance"

    # get points contained in polys
    df = (
        gpd.sjoin(pts_gdf, poly_gdf, how='inner', predicate="within")
        .filter(items=['uid', 'index_right', 'geometry'], axis=1)
        .rename(columns={'geometry':'point'})
    )

    # merge points and polys into single dataframe
    poly_gdf = (
        poly_gdf
        .filter(items=['geometry'])
        .merge(df, left_index=True, right_on="index_right")

    )

    # calculate distance to edge of change poly
    poly_gdf.loc[:, fld_name] = poly_gdf.apply(lambda row: row['geometry'].boundary.distance(row['point']), axis=1)

    # filter fields
    poly_gdf = (
        poly_gdf
        .filter(items=['uid', fld_name], axis=1)
    )

    # return data
    return poly_gdf

if __name__=="__main__":

    # paths
    folder = r"" # path to working directory
    
    # land cover change raster
    lcc_raster_path = f"{folder}/lcc_rasters/DC/wash_11001_landcoverchange_2013_2021.tif"
    dst_path = f"{folder}/clean_points/distance"

    # points path
    points_path = f"{folder}/clean_points/lcc_aa_points_cleaned.gpkg"

    # boolean flag, true to vectorize
    vectorize_points = False

    # state -- put this in a loop
    st = "DC"

    # read and filter points
    points = (
        gpd.read_file(points_path, layer="AA_clean")
        .query("state == @st")
        .query("type == 'change'")
        .filter(items=['uid', 'geometry'], axis=1)
    )
    gdf = None

    # centered points
    centered_points_path = f"{folder}/clean_points/point_rasters/{st}.gpkg"
    c_points = (
        gpd.read_file(centered_points_path, layer='points')
        .merge(points[['uid']], on='uid', how='inner') # orig opoints already filtered by type -- can simply merge IDs
        .filter(items=['uid', 'geometry'], axis=1)
    )

    # compute polygons
    if vectorize_points:
        gdf = vectorize(lcc_raster_path, dst_path, st, "Value")
    else:
        gdf = gpd.read_file(f"{dst_path}/{st}_change.gpkg", layer="lcc")

    # get distance
    dist_orig_lcc = calculate_distance(gdf, points, fld_name='dist_orig_lcc')
    dist_cntr_lcc = calculate_distance(gdf, points, fld_name='dist_cntr_lcc')

    # create binary change patch
    if vectorize_points:
        gdf = dissolve_change(f"{dst_path}/{st}_change.gpkg", gdf=gdf)
    else:
        gdf = gpd.read_file(f"{dst_path}/{st}_change.gpkg", layer="lcc_dis")

    # get distance
    dist_orig_lccdis = calculate_distance(gdf, points, fld_name='dist_orig_lccDis')
    dist_cntr_lccdis = calculate_distance(gdf, points, fld_name='dist_cntr_lccDis')

    # merge distances and points
    points = (
        points
        .merge(dist_orig_lcc, on='uid', how='left')
        .merge(dist_orig_lccdis, on='uid', how='left')
    )

    c_points = (
        c_points
        .merge(dist_cntr_lcc, on='uid', how='left')
        .merge(dist_cntr_lccdis, on='uid', how='left')
    )

    data = (
        points
        .drop('geometry', axis=1)
        .merge(
            (
            c_points
            .drop('geometry', axis=1)
            ),
            on='uid',
            how='outer'
        )
    )

    # write results
    gpkg = f"{dst_path}/{st}_distance.gpkg"
    csv = f"{dst_path}/{st}_distance.csv"

    points.to_file(gpkg, layer='original_points', driver="GPKG")
    points.to_file(gpkg, layer='centered_points', driver="GPKG")
    data.to_csv(csv, index=False)
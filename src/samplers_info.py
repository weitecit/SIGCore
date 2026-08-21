import geopandas as gpd
from pathlib import Path
import mongo_api as mapi
import pandas as pd

AREA_POR_ARBOL = 22 #m2
FACTOR_INCLUSION = 0.25 #el porcentaje de árboles que contamos como válidos

excluded_samplers = ['probensing', 'dani', 'portatil', 'pablosanz']

crops_family = {'citrus': ['khaki', 'orange', 'lemon', 'tangerine']}

projects_folder = Path(r"C:\Users\Daniel\QField\cloud")

def generate_base_gdf(project_paths:list[str], target_crs:int=32630, mongo_samplers:list[str]=None, qfield_samplers:list[str]=None)-> gpd.GeoDataFrame:
    gdfs_list = []
    for pp in project_paths:
        points_gdf = gpd.read_file(pp / 'Marcas.gpkg').to_crs(target_crs)
        points_gdf['n_samples'] =1
        areas_gdf = gpd.read_file(pp / 'Marcas_area.gpkg').to_crs(target_crs)
        areas_gdf['n_samples'] = (areas_gdf.geometry.area*FACTOR_INCLUSION/AREA_POR_ARBOL).round().astype(int)

        merged_gdf = pd.concat([areas_gdf, points_gdf]).reset_index(drop=True)
        gdfs_list.append(merged_gdf)
        

    gdf = pd.concat(gdfs_list).reset_index(drop=True)

    if qfield_samplers:
        gdf = gdf[gdf['user_register'].isin(qfield_samplers)]
    else:
        gdf = gdf[~gdf['user_register'].isin(excluded_samplers)]

    #mongo gdf
    if mongo_samplers:
        mongo_gdf = mapi.get_weifield_points(mongo_samplers, reduce=True).to_crs(target_crs)
        mongo_gdf['n_samples'] = 1 #TODO contemplar el caso de que se cuenten áreas en mongo
        gdf['datetime'] = gdf['datetime'].dt.tz_convert('Europe/Madrid').dt.tz_localize(None)
        gdf = pd.concat([gdf, mongo_gdf]).reset_index(drop=True)
    return gdf

def join_parcel_gdf(gdf: gpd.GeoDataFrame, parcelario_gdf: gpd.GeoDataFrame=None) -> gpd.GeoDataFrame:
    if parcelario_gdf is None:
        parcelario_gdf = mapi.get_parcelario(only_operating=False).to_crs(gdf.crs)
    join_gdf = gdf.sjoin(parcelario_gdf, how='left', predicate='intersects').reset_index(drop=True)
    join_gdf['crop_family'] = _generate_crop_family(join_gdf)
    return join_gdf


def _generate_crop_family(gdf: gpd.GeoDataFrame) -> pd.Series:
    return gdf['crops'].str[0].apply(lambda x: next((k for k, v in crops_family.items() if x in v), x))

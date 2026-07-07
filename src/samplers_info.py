import geopandas as gpd
from pathlib import Path
import mongo_api as mapi
import pandas as pd

AREA_POR_ARBOL = 22 #m2
FACTOR_INCLUSION = 0.25 #el porcentaje de árboles que contamos como válidos

excluded_samplers = ['probensing', 'dani', 'portatil', 'pablosanz']

target_crs = 32630

crops_family = {'citrus': ['khaki', 'orange', 'lemon', 'tangerine']}

projects_folder = Path(r"C:\Users\Daniel\QField\cloud")

#TODO: add a merge with mongo points

def generate_gdf(project_names:list[str])-> gpd.GeoDataFrame:
    gdfs_list = []
    for pn in project_names:
        target_folder = projects_folder / pn
        points_gdf = gpd.read_file(target_folder / 'Marcas.gpkg').to_crs(target_crs)
        points_gdf['n_samples'] =1
        areas_gdf = gpd.read_file(target_folder / 'Marcas_area.gpkg').to_crs(target_crs)
        areas_gdf['n_samples'] = (areas_gdf.geometry.area*FACTOR_INCLUSION/AREA_POR_ARBOL).round().astype(int)

        merged_gdf = pd.concat([areas_gdf, points_gdf]).reset_index(drop=True)
        gdfs_list.append(merged_gdf)
        

    gdf = pd.concat(gdfs_list).reset_index(drop=True)
    gdf = gdf[~gdf['user_register'].isin(excluded_samplers)]
    return gdf

def generate_joined_gdf(gdf: gpd.GeoDataFrame, parcelario_gdf: gpd.GeoDataFrame=None) -> gpd.GeoDataFrame:
    if parcelario_gdf is None:
        parcelario_gdf = mapi.get_parcelario(only_operating=False).to_crs(target_crs)
    join_gdf = gdf.sjoin(parcelario_gdf, how='left', predicate='intersects')
    join_gdf['crop_family'] = _generate_crop_family(join_gdf)
    return join_gdf


def _generate_crop_family(gdf: gpd.GeoDataFrame) -> pd.Series:
    #check if the crop is in the crops_family dictionary, and return the corresponding family, otherwise return 'other'
    return gdf['crops'].str[0].apply(lambda x: next((k for k, v in crops_family.items() if x in v), x))
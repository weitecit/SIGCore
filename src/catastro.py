import os
import math
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import geopandas as gpd
import requests
from pyproj import Transformer
from shapely.geometry.point import Point

from config import REQUIRED_COLUMNS, ASSETS_FOLDER

class SigpacError(Exception):
    pass

def polygonize_data_parallel(
    input_data: str | dict | pd.DataFrame,
    max_workers: int = 10,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    """
    This function polygonizes a set of plots using parallel processing as polygonize_data do.
    This version uses the oficial SIGPAC API instead of downloading the files directly from the SIGPAC Backend.
    """

    #Check if SIGPAC service is available
    try:
        response = requests.get('https://desarrollo.tragsatec.es/ogc-api-feature/')
        if response.status_code != 200:
            raise Exception('Response: ' + str(response.status_code) + ' - ' + response.text)
    except Exception as e:
        raise SigpacError('SIGPAC service is not available: ' + str(e)) from e
    
    df_input = open_data(input_data)

    mc = missing_columns(df_input)
    if len(mc) > 0:
        raise KeyError('Missing columns: ', mc)

    out_data = gpd.GeoDataFrame()
    error_df = pd.DataFrame(columns=df_input.columns)

    def _fetch_one(plot: pd.Series) -> tuple[gpd.GeoDataFrame | None, pd.Series]:
        try:
            detected = _download_plot_file(
                int(plot['province']),
                int(plot['municipality']),
                int(plot['polygon']),
                int(plot['plot_number']),
                int(plot['enclosure']) if 'enclosure' in plot.index and not math.isnan(plot['enclosure']) else None,
            )
            print(
                f'Prov: {plot["province"]} || Mun: {plot["municipality"]} || Pol: {plot["polygon"]} || Par: {plot["plot_number"]}',f'Found: {len(detected)} plots'
                )

            return detected, plot
        except Exception as e:
            print('Error downloading plot: ', e)
            plot['error'] = str(e)
            return None, plot

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_fetch_one, plot.copy()) for _, plot in df_input.iterrows()]
        for fut in futures:
            detected_plots, plot = fut.result()

            if detected_plots is None:
                #Error while finding enclosure
                error_df = pd.concat([error_df, plot.to_frame().T])
                continue

            if detected_plots.empty:
                plot['error'] = 'NOT FOUND'
                error_df = pd.concat([error_df, plot.to_frame().T])
                continue

            if 'field' in df_input.columns:
                detected_plots['field'] = plot['field']
            if 'client' in df_input.columns:
                detected_plots['client'] = plot['client']
            if 'crop' in df_input.columns:
                detected_plots['crop'] = plot['crop']
            if 'operating' in df_input.columns:
                detected_plots['operating'] = bool(plot['operating'])
            if 'cadastral_ref' in df_input.columns:
                detected_plots['cadastral_ref'] = plot['cadastral_ref']

            if out_data.empty: out_data = detected_plots
            else: out_data = pd.concat([out_data, detected_plots])
    
    out_data.drop_duplicates(subset=['geometry'], inplace=True)

    print('DONE!')
    if len(out_data) > 0 and 'field' in out_data.columns:
        print('Fields detected: ', len(out_data['field'].unique()), out_data['field'].unique())
        if out_data.crs is None:
            out_data = out_data.set_crs('EPSG:4258')
        else:
            try:
                if out_data.crs.to_epsg() != 4258:
                    out_data = out_data.to_crs('EPSG:4258')
            except Exception:
                out_data = out_data.to_crs('EPSG:4258')
    print(f'Total: {len(out_data)} plots')
    print('Errors: ', len(error_df))

    return out_data, error_df


def missing_columns(dataframe:gpd.GeoDataFrame)->list[str]:
    missing = list(set(REQUIRED_COLUMNS)- set(dataframe.columns))
    return missing

def open_data(input_data):
    if isinstance(input_data, str):
        if os.path.exists(input_data):
            if os.path.splitext(input_data)[1] == '.xlsx':
                #TODO: Adapt column names to match the input requirements
                return _adapt_columns(pd.read_excel(input_data))
            else:
                raise TypeError('Unsupported format: ', os.path.splitext(input_data)[1])
        else:
            raise FileNotFoundError('File not found: ', input_data)
    elif isinstance(input_data, pd.DataFrame):
        return _adapt_columns(input_data)
    elif isinstance(input_data, list):
        return pd.DataFrame.from_records(input_data)
    else:
        raise TypeError('Unsupported type: ', type(input_data))

def get_siar_stations(point:Point = None, n_nearest:int=None)->gpd.GeoDataFrame:
    stations_gdf = gpd.read_file(ASSETS_FOLDER/'SIAR_stations.geojson')
    stations_gdf = stations_gdf[stations_gdf['Estado']=='Activa'].reset_index()
    if not point: return stations_gdf

    if stations_gdf.crs is not None and getattr(stations_gdf.crs, 'is_geographic', False):
        transformer = Transformer.from_crs(stations_gdf.crs, 'EPSG:3857', always_xy=True)
        x, y = transformer.transform(point.x, point.y)
        point_3857 = Point(x, y)
        stations_proj = stations_gdf.to_crs('EPSG:3857')
        stations_gdf['distance'] = stations_proj.distance(point_3857)
    else:
        stations_gdf['distance'] = stations_gdf.distance(point)
    if not n_nearest: return stations_gdf

    sorted_gdf = stations_gdf.sort_values('distance', ascending=True)
    return sorted_gdf.head(n_nearest)

def _adapt_columns(dataframe:pd.DataFrame)->pd.DataFrame:
    new_df = dataframe.copy()
    new_names = {
        'Nombre Finca':'field',
        'Cliente':'client',
        "Cultivo":"crop",
        'Cultivo (Opcional)':'crop',
        "Operativo":"operating",
        "Provincia":"province",
        "Municipio":"municipality",
        "Poligono":"polygon",
        "Polígono":"polygon",
        "Parcela":"plot_number",
        "Recinto":"enclosure",
        "Superficie (Ha)":"area_ha",
        "Zona":"zone",
        "Agregado":"aggregate"
    }
    return new_df.rename(columns=new_names)

def _download_plot_file(prov:int, mun:int, pol:int, par:int, rec:int=None)->gpd.GeoDataFrame:
    if rec is None or math.isnan(rec):
        req_string = f"{prov}/{mun}/0/0/{pol}/{par}.geojson"
        r = requests.get(f'https://sigpac-hubcloud.es/servicioconsultassigpac/query/recinfoparc/{req_string}')
    else:
        req_string = f"{prov}/{mun}/0/0/{pol}/{par}/{rec}.geojson"
        r = requests.get(f'https://sigpac-hubcloud.es/servicioconsultassigpac/query/recinfo/{req_string}')
    if r.status_code != 200:
        raise KeyError(f'Error getting plot file {req_string}: {r.content}')
        
    gdf = gpd.GeoDataFrame.from_features(r.json()['features'])
    if gdf.empty:
        raise ValueError(f'Plot file {req_string} not found')
    #ha to m2
    gdf.rename(columns={'superficie': 'dn_surface'}, inplace=True)
    gdf['dn_surface'] = gdf['dn_surface'] * 10000
    return gdf




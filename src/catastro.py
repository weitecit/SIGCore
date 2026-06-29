import os
import math
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import geopandas as gpd
import requests
from pyproj import Transformer
from shapely.geometry.point import Point
from itertools import product
import unicodedata
from tqdm import tqdm
import zipfile
import io

from config import REQUIRED_COLUMNS, ASSETS_FOLDER, PROVINCES_FOLDER, SIGPAC_YEAR

province_df = pd.read_csv(ASSETS_FOLDER/'sigpac_provinces.csv')

class SigpacError(Exception):
    pass

def check_sigpac_service_status():
    response = requests.get('https://desarrollo.tragsatec.es/ogc-api-feature/', timeout=5)
    return response.status_code

def polygonize_data(input_data:str|dict|pd.DataFrame)->tuple[gpd.GeoDataFrame,pd.DataFrame]:
    """
        Process a xls file with plots and returns a GeoDataFrame with the plots polygons and a DataFrame
        with the rows from the xls file that were not found in the parcelario.

        Parameters
        ----------
        xls_data : str
            Path to the xls file or Dataframe with the plots

        Returns
        -------
        out_data : GeoDataFrame
            GeoDataFrame with the plots polygons
        error_df : DataFrame
            DataFrame with the rows from the xls file that were not found in the parcelario

        Notes
        -----
        The xls file must have the following columns: province, municipality, polygon, plot_number
        If the column 'field' is present, it will be added to the GeoDataFrame
        If the column 'client' is present, it will be added to the GeoDataFrame
        If the column 'crop' is present, it will be added to the GeoDataFrame
    """
    
    df_input = open_data(input_data)
    
    #Handle errors
    mc = missing_columns(df_input)
    if len(mc)>0: raise KeyError('Missing columns: ', mc)
    
    #Create GeoDataFrame
    out_data = gpd.GeoDataFrame()
    error_df = pd.DataFrame(columns=df_input.columns)

    #Start processing
    provincias = df_input['province'].unique()
    #for provincia
    for prov in provincias:
        #get unique municipios where current provincia
        municipios = df_input[df_input['province']==prov]['municipality'].unique()
        #for municipio
        for mun in municipios:
            #find and open municipio file
            try:
                mun_name = _find_mun_file(prov, mun)
            except Exception:
                #add error log
                print(f'Error: {prov:02} || {mun:03} not found')
                munrows = df_input[(df_input['province']==prov) & (df_input['municipality']==mun)]
                munrows['error'] = 'Municipality not found'
                error_df = pd.concat([error_df, munrows], ignore_index=True)
                continue
            #TODO: Si el archivo de municipio está corrupto lanza el error pyogrio.errors.DataSourceError. Se debe gestionar
            mun_data = gpd.read_file(os.path.join(PROVINCES_FOLDER, mun_name), layer='recinto')
            #get csv parcels of the municipio
            plots_xls = df_input[(df_input['province']==prov) & (df_input['municipality']==mun)]
            #iterate, find coincidences and copy to out_data
            for i, plot in plots_xls.iterrows():
                print(f'Prov: {prov:02} || Mun: {mun:03} || Pol: {plot["polygon"]} || Par: {plot["plot_number"]}', end=' ')
                geometry = mun_data[(mun_data['poligono']==plot['polygon']) & (mun_data['parcela']==plot['plot_number'])]

                if 'enclosure' in df_input.columns:
                    if not math.isnan(plot['enclosure']):
                        geometry = geometry[geometry['recinto']==plot['enclosure']]

                if 'field' in df_input.columns:
                    geometry['field']=plot['field']

                if 'client' in df_input.columns:
                    geometry['client']=plot['client']

                if 'crop' in df_input.columns:
                    geometry['crop']=plot['crop']
                
                if 'operating' in df_input.columns:
                    geometry['operating']= bool(plot['operating'])
                else:
                    geometry['operating']= True

                if 'cadastral_ref' in df_input.columns:
                    geometry['cadastral_ref']=plot['cadastral_ref']

                if len(geometry)>0:
                    out_data = pd.concat([out_data, geometry])
                    print('FOUND: ', len(geometry))
                else:
                    plot['error'] = 'NOT FOUND'
                    error_df = pd.concat([error_df, plot.to_frame().T])
                    print('NOT FOUND  ❌')        

    out_data.drop_duplicates(subset=['geometry'], inplace=True)

    print('DONE!')
    if len(out_data)>0 and 'field' in out_data.columns:
        print('Fields detected: ', len(out_data['field'].unique()) ,out_data['field'].unique())
    print(f'Total: {len(out_data)} plots')
    print('Errors: ', len(error_df))

    return out_data, error_df

def polygonize_data_parallel(
    input_data: str | dict | pd.DataFrame,
    max_workers: int = 10,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    """
    This function polygonizes a set of plots using parallel processing as polygonize_data do.
    This version uses the oficial SIGPAC API instead of downloading the files directly from the SIGPAC Backend.
    """

    #Check if SIGPAC service is available
    status_code = check_sigpac_service_status()
    if status_code != 200:
        raise SigpacError(f'SIGPAC service is not available. Status code: {status_code}')
    
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
    if len(out_data) > 0:
        if 'field' in out_data.columns:
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
        r = requests.get(f'https://sigpac-hubcloud.es/servicioconsultassigpac/query/recinfoparc/{req_string}', timeout=15)
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

def _find_mun_file(prov:int, mun:int):
    all_dirs = os.listdir(PROVINCES_FOLDER)
    for dir in all_dirs:
        prov_i = int(dir[0:2])
        mun_i = int(dir[2:5])
        if(prov_i == prov and mun_i == mun):
            return dir
    
    #if not found: download and extract
    try:
        _download_municipality(prov, mun, SIGPAC_YEAR)
        return _find_mun_file(prov, mun)
    except FileNotFoundError:
        raise FileNotFoundError('Municipality not found')

def _download_municipality(province_number:int, municipality_number:int, year:int, save_folder:str=PROVINCES_FOLDER)->requests.Response:
    auxiliar_numbers = [110, 115, 1126, 105]
    normalized_prov = _normalized_province_name(province_number)
    province_url = f'https://www.fega.gob.es/atom/{year}/rec_{year}/{province_number:02}%20-%20{normalized_prov}/'
    years_to_check = [year, year-1]
    #Try all auxiliar numbers (unknown SIGPAC criteria)
    for aux_n, y in product(auxiliar_numbers, years_to_check):
        filename =  f'{province_number:02}{municipality_number:03d}_rec_{year}_{y}{aux_n:04d}_gpkg.zip'
        url= province_url + filename
        response = requests.get(url, verify=False, stream=True)
        if response.status_code == 200:
            print(f'Municipality {municipality_number} of province {normalized_prov} found.')
            _download_and_extract_response(response, save_folder)
            return response
        
    #Not found
    print(url)
    raise FileNotFoundError(f'Municipality {municipality_number} of province {normalized_prov} not found.')

def _normalized_province_name(province_number:int)->str:
    #Quitar acentos, poner en mayúscula y sustituir espacios
    province_name = province_df[province_df['Número']==province_number].iloc[0]['Provincia']
    
    normalized_province_name = ''.join(c for c in unicodedata.normalize('NFD', province_name) if unicodedata.category(c)!='Mn')
    normalized_province_name = normalized_province_name.upper()
    return normalized_province_name.replace(' ', '%20')

def _download_and_extract_response(response:requests.Response, extract_to:str)->None:
    # Realiza una petición para obtener el tamaño del archivo
    os.makedirs(extract_to, exist_ok=True)
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024

    if response.status_code == 200:
        # Descarga el archivo con barra de progreso
        buffer = io.BytesIO()
        with tqdm(total=total_size, unit='iB', unit_scale=True, desc="Downloading municipality") as t:
            for data in response.iter_content(block_size):
                t.update(len(data))
                buffer.write(data)
        buffer.seek(0)

        # Extrae el archivo ZIP en memoria
        with zipfile.ZipFile(buffer) as thezip:
            thezip.extractall(extract_to)
        print(f'File extracted at {extract_to}')
    else:
        print('Error downloading file:', response.status_code)
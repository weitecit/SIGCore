from pymongo import MongoClient
from catastro import polygonize_data_parallel, open_data
from pymongo.errors import BulkWriteError
import geopandas as gpd
from shapely.geometry import shape, Point
from config import MONGO_STRING
from datetime import datetime
from bson import ObjectId
import math

client = MongoClient(MONGO_STRING)
db = client['main']
SYSTEM_TOKEN = "66a7a3c2fef995522871a9a0"
#=====================================
# EXCEPTIONS
#=====================================

class FieldNotFound(Exception):
    pass

class NoReportsFound(Exception):
    pass

class NoAlertsFound(Exception):
    pass

class NoWeatherFound(Exception):
    pass

#=====================================
# MAIN FUNCTIONS
#=====================================

def upload_plots_from_xls(xls_path: str, override_fields: bool = False, parcel_name:str = None) -> gpd.GeoDataFrame:
    """
    CUIDADO: el override funciona borrando todos los objetos en mongo con el mismo nombre de finca
    """
    # 1. Process the file and get the polygonized data
    if parcel_name:
        xls_df = open_data(xls_path)
        xls_df = xls_df[(xls_df['Nombre Finca'] == parcel_name) & (xls_df['Operativo'] == 1)]
        polygonize_input = xls_df
    else:
        polygonize_input = xls_path

    main_df, error_df = polygonize_data_parallel(polygonize_input)
    
    print(f"❌ Invalid records in SIGPAC: {len(error_df)}")
    
    # 2. Rename 'field' to 'parcel' if needed
    if 'field' in main_df.columns and 'parcel' not in main_df.columns:
        main_df = main_df.rename(columns={'field': 'parcel'})
    
    # 4. Process the upload without override (already handled)
    upload_plotlist_from_dataframe(main_df, override_fields)
    return error_df

def upload_plotlist_from_dataframe(main_df: gpd.GeoDataFrame, override_fields: bool = False) -> None:
    if main_df.empty:
        print("⚠️ No data to process")
        return
    if main_df.crs is None:
        raise Exception("The GeoDataframe has no CRS")
        
    # Rename 'field' to 'parcel' if needed (for backward compatibility)
    if 'field' in main_df.columns and 'parcel' not in main_df.columns:
        main_df = main_df.rename(columns={'field': 'parcel'})

    if override_fields:
        parcels = main_df['parcel'].unique()
        for parcel in parcels:
            try:
                parcel_id = get_parcel_id(parcel)
                result = db.plots.delete_many({'properties.parcel_id': parcel_id})
                print(f"🗑️ Deleted {result.deleted_count} plots")
            except FieldNotFound as e:
                print(f"⚠️ {str(e)}")
                continue

    features = _gdf_to_mongo_structure(main_df)
    new_features = _check_plots_duplicated(features)

    try:
        if new_features is not None and len(new_features) > 0:
            result = db.plots.insert_many(new_features, ordered=False)
            print(f"📊 Successfully inserted {len(result.inserted_ids)} plots")
        else:
            print("No new plots to insert.")

    except BulkWriteError as e:
        print(f"❌❌ Insert failed with {len(e.details['writeErrors'])} errors")

def find_field_plots(field_name:str|None=None)->list[dict]:
        if field_name is None:
            return list(db.plots.find())
        else:
            return list( db.plots.find({'$or':[{'properties.parcel': {'$regex': '^' + str(field_name) + '$', '$options': 'i'}}, {'properties.parcel_id':field_name}]}))

def get_parcelario(field_name:str|None=None, only_operating=True)->gpd.GeoDataFrame:
    features = find_field_plots(field_name)
    if len(features)<=0:
        raise FieldNotFound(f"Field '{field_name}' not found in database")
    gdf = _mongo_to_gdf(features).to_crs(4258)
    return gdf[gdf['operating']] if only_operating else gdf

def get_parcelario_by_id(parcel_id:str, only_operating:bool=True)->gpd.GeoDataFrame:
    result = db.plots.find({'properties.parcel_id':parcel_id})
    features = list(result)
    if len(features) <= 0:
        raise FieldNotFound(f"Parcel '{parcel_id}' not found in database")
    gdf = _mongo_to_gdf(features).to_crs(4258)
    return gdf[gdf['operating']] if only_operating else gdf

#TODO: no se necesita traer todo el parcelario. Se podría hacer una búsqueda más eficiente dado un polígono o extent.
def get_parcelario_by_extent(extent:tuple, source_crs:str|int = 4258)->gpd.GeoDataFrame:

    plot_list = get_parcelario()
    plot_list = plot_list.to_crs(source_crs)
    filtered = plot_list.cx[extent[0]:extent[2], extent[1]:extent[3]]
    return filtered

def get_parcel_centroid(parcel_id, crs:str|int=32630)->Point:
    union = get_parcelario_by_id(parcel_id).to_crs(crs).union_all()
    return union.centroid

def get_parcel_id(parcel_name:str)->str:
    result = db.parcels.find_one({'name':{'$regex': '^' + str(parcel_name) + '$', '$options': 'i'}})
    if result:
        return str(result['_id'])
    else:
        raise FieldNotFound(f"Parcel '{parcel_name}' not found in database")

def get_parcel_name(parcel_id:str)->str:
    result = db.parcels.find_one({'_id':ObjectId(parcel_id)})
    if result:
        return result['name']
    else:
        raise FieldNotFound(f"Parcel '{parcel_id}' not found in database")

def find_plot_by_position(latitude:float, longitude:float)->dict:
    """
    Returns the nearest plot to the given coordinates at a max distance of 100 meters.
    WARNING: Only accepts latitude and longitude coordinates (geographical coordinates)
    Returns 
        The plot if found
        None if no plot is found
    """
    result = list(
        db.plots.aggregate([{
        "$geoNear": {
            "near": {
                "type": "Point",
                "coordinates": [longitude, latitude]
            },
            "distanceField": "distance",
            "spherical": True,
            "maxDistance": 100
        }},
        {"$sort": {"distance": 1}},
        {"$limit": 1},
        {"$project": {"distance":0}}
        ])
    )

    return result[0] if result else None

def find_layers_by_position(latitude:float, longitude:float)->dict:
    """
    Returns the nearest plot to the given coordinates at a max distance of 100 meters.
    Accepts UTM coordinates (EPSG:32630) or WGS84 (EPSG:4326).
    Returns 
        The plot if found
        None if no plot is found
    """
    longitude = float(longitude)
    latitude = float(latitude)
    if abs(longitude) > 180 or abs(latitude) > 90:
        point_gdf = gpd.GeoDataFrame(geometry=[Point(latitude, longitude)], crs="EPSG:25830")
        point_gdf = point_gdf.to_crs("EPSG:4326")
        longitude, latitude = point_gdf.geometry.iloc[0].x, point_gdf.geometry.iloc[0].y
    
    result = list(db.layers.aggregate([
        {"$match": {
            "geometry": {
                "$geoIntersects": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": [longitude, latitude]
                    }
                }
            }
        }}
    ]))

    def clean_nan(obj):
        from bson import ObjectId
        if isinstance(obj, dict):
            return {k: clean_nan(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [clean_nan(v) for v in obj]
        elif isinstance(obj, float) and math.isnan(obj):
            return None
        elif isinstance(obj, ObjectId):
            return str(obj)
        return obj

    return clean_nan(result) if result else []

def get_alerts_gdf(parcel_id:str, week:int=None, year:int=None)->gpd.GeoDataFrame:
    reports = get_reports(parcel_id, week, year)

    #get all alerts from reports
    alerts_result = []
    for r in reports:
        query = {'page':str(r['_id']), 'block_type':'alert'}
        report_result = list(db.blocks.find(query))
        for rr in report_result:
            for p in rr['properties']['points']:
                try:
                    if ',' not in p:
                        print(f"⚠️ Skipping invalid point format (no comma): {p}")
                        continue
                    x,y = p.split(',')
                except ValueError:
                     print(f"⚠️ Skipping invalid point data: {p}")
                     continue
                try:
                    obj = {
                        'level': int(rr['properties']['level']) if 'level' in rr['properties'] else None,
                        'group': int(rr['properties']['group']) if 'group' in rr['properties'] else None,
                        'title': rr['title'],
                        'week': int(r['properties']['week']) if 'week' in r['properties'] else None,
                        'year': int(r['properties']['year']) if 'year' in r['properties'] else None,
                        'geometry':Point(float(x), float(y))
                    }
                    alerts_result.append(obj)
                except TypeError as e:
                    print("Skipping for invalid type")
                    continue

    if len(alerts_result)==0:
        raise NoAlertsFound(f"No alerts found for parcel '{parcel_id}' in week '{week}' and year '{year}'")
    #TODO Algunas alertas tienen un CRS 4326, esto puede dar errores de posición
    crs = 'epsg:32630' if len(alerts_result)>0 else None
    return gpd.GeoDataFrame(alerts_result, columns=['level', 'group', 'title', 'week', 'year', 'geometry'], crs=crs)

def get_reports(parcel_id:str, week:int=None, year:int=None)->list:
    query = {'space_id':parcel_id, 'block_type':'report'}
    if week: query['properties.week'] = week
    if year: query['properties.year'] = year

    query_result = db.blocks.find(query)
    results = list(query_result)

    if len(results)<=0:
        raise NoReportsFound(f"No reports found for parcel {parcel_id} and week {week} and year {year}")

    return results

def get_report_path(parcel_id:str, week:int=None, year:int=None)->list:
    reports = get_reports(parcel_id, week, year)
    paths = [f"maps/{r['repo_id']}/{r['space_id']}/{r['_id']}" for r in reports]
    return paths

def get_weather(parcel_id:str, week:int=None, year:int=None)->gpd.GeoDataFrame:
    query = {'block_type':'weather', 'space_id':parcel_id}
    if week: query['properties.week'] = week
    if year: query['properties.year'] = year
    result = list(db.blocks.find(query))
    if len(result) == 0:
        raise NoWeatherFound(f"No weather data found for parcel {parcel_id} and week {week} and year {year}")
    return result

def get_weather_last(parcel_id:str)->gpd.GeoDataFrame:
    query = {'block_type':'weather', 'space_id':parcel_id}
    result = db.blocks.find_one(query, sort=[('properties.date', -1)])
    if result is None:
        raise NoWeatherFound(f"No weather data found for parcel {parcel_id}")
    return result

def save_kpis(kpi_data: list[dict]):
    if not kpi_data:
        return
    
    # Ensure date is a datetime object
    for item in kpi_data:
        if 'created_at' not in item:
            item['created_at'] = datetime.now()
        if 'updated_at' not in item:
            item['updated_at'] = datetime.now()
            
        # Inject BaseModel and UserLogs matching the Go struct
        _apply_base_model(item)

    db.kpis.insert_many(kpi_data)

def get_block(block_id:str|ObjectId)->dict:
    try:
        if isinstance(block_id, str):
            oid = ObjectId(block_id)
        else:
            oid = block_id
            
        result = db.blocks.find_one({'_id':oid})
        if result is None:
            raise FieldNotFound(f"Block '{block_id}' not found")
        return result
    except Exception as e:
        # If not a valid ObjectId or other error
        raise FieldNotFound(f"Invalid block ID or not found: {block_id}. Error: {e}")


#=====================================
# UTILS
#=====================================
def _get_user_log(user_id: str = SYSTEM_TOKEN) -> dict:
    return {
        "user_id": user_id,
        "name": "System",
        "date": datetime.now()
    }

def _mongo_to_gdf(features:list[dict])->gpd.GeoDataFrame:
    #TODO: check CRS
    properties_list = [f['properties'] for f in features]
    geometries = [shape(f['geometry']) for f in features]
    gdf = gpd.GeoDataFrame(properties_list, geometry=geometries, crs = 4326)
    return gdf

def _gdf_to_mongo_structure(main_df:gpd.GeoDataFrame)->list[dict]:
    has_parcel = 'parcel' in main_df.columns
    has_parcel_id = 'parcel_id' in main_df.columns

    # Check if CRS is set
    if main_df.crs is None:
        raise Exception("The GeoDataframe has no CRS")

    # Convert to 4326 for MongoDB if not already
    try:
        if main_df.crs.is_projected or main_df.crs.to_epsg() != 4326:
            print(f"🔄 Converting to EPSG:4326")
            main_df = main_df.to_crs("EPSG:4326")
    except AttributeError:
        # If crs doesn't have to_epsg or is_projected, just try to_crs
        main_df = main_df.to_crs("EPSG:4326")
    except Exception as e:
        print(f"Warning during CRS conversion: {e}")
        # Continue anyway if it's already in a compatible format or let it fail if absolutely necessary
    
    # Keep the original UTM coordinates
    mongo_fields_dict = {}
    fields_found_in_mongo = []
    
    # Filter plots that have a valid field
    if has_parcel and not has_parcel_id:
        # Get field information from MongoDB
        for parcel in main_df['parcel'].unique():
            try:
                result = get_parcel_id(parcel)
                mongo_fields_dict[parcel] = result
                fields_found_in_mongo.append(parcel)
            except FieldNotFound:
                print(f"⚠️ Field '{parcel}' not found in database")
                continue
            
        filtered_df = main_df[main_df['parcel'].isin(fields_found_in_mongo)]

        print(f"✅ Valid plots found: {len(filtered_df)}")
        print(f"\t {fields_found_in_mongo}")
        print(f"⚠️ Plots with no field found: {len(main_df) - len(filtered_df)}")
        print(f"\t {set(main_df['parcel'].unique()) - set(fields_found_in_mongo)}")
    else:
        filtered_df = main_df

    # Create separate features with UTM coordinates
    features = []
    for _, row in filtered_df.iterrows():
        feature = {
            "type": "Feature",
            "geometry": row["geometry"].__geo_interface__,  # This will use UTM coordinates
            "properties": row.drop("geometry").to_dict(),
            "crs": {
                "type": "name",
                "properties": {
                    "name": "EPSG:4326"
                }
            }
        }
        if has_parcel and not has_parcel_id:    
            feature["properties"]["parcel_id"] = str(mongo_fields_dict[row["parcel"]])
            feature["properties"]["parcel"] = row["parcel"].lower()
        
        features.append(_apply_base_model(feature))

    return features

def _apply_base_model(doc: dict, user_id: str = SYSTEM_TOKEN) -> dict:
    u_log = _get_user_log(user_id)
    
    if 'properties' in doc:
        props = doc['properties']
        if 'created_by' in props:
            doc['created_by'] = props.pop('created_by')
        else:
            doc['created_by'] = u_log
        
        if 'updated_by' in props:
            doc['updated_by'] = props.pop('updated_by')
        else:
            doc['updated_by'] = u_log
    else:
        doc.setdefault('created_by', u_log)
        doc.setdefault('updated_by', u_log)
    
    doc['touched'] = doc.get('touched', False)
    return doc

def _apply_points_model(dataframe:gpd.GeoDataFrame, metadata:dict=None)->gpd.GeoDataFrame:
    """Only for POINTS metadata"""
    script_log = _get_user_log()

    default_metadata = {
        'comments': "",
        'severity': 0,
        'use_case': "roturas_hidricas",
        'hide': False,
        'position_source': None,
        'horizontal_accuracy': None,
        'parcel_id': "",
        'parcel_name': "",
        'source_version': 'unknown',
        'source_name': 'unknown',
        'sampled_at': datetime.now(),
        'created_at': datetime.now(),
        'created_by': script_log,
        'updated_by': script_log,
        'photos_ids': [],
        'validated_at': None,
        'validated_by': None,
    }

    #update default metadata
    if metadata: default_metadata.update(metadata)
    has_parcel_id = default_metadata['parcel_id']
    has_parcel_name = default_metadata['parcel_name']
    new_gdf = dataframe.copy()

    #add default metadata if the column doesnt exists
    for key, value in default_metadata.items():
        if key not in new_gdf.columns:
            if isinstance(value, (list, dict)):
                new_gdf[key] = [value.copy() for _ in range(len(new_gdf))]
            else:
                new_gdf[key] = [value for _ in range(len(new_gdf))]

    # find parcel id and name: prefer direct lookup by id; fallback to geospatial only if neither provided
    if has_parcel_id and not has_parcel_name:
        # We already know parcel_id; avoid geo lookup and fetch name directly
        pid = str(default_metadata['parcel_id'])
        new_gdf['parcel_name'] = get_parcel_name(pid)
        
    elif not has_parcel_id and not has_parcel_name:
        # Neither id nor name provided: infer by geospatial proximity (may be slower)
        def apply_parcel_id(row):
            plot_obj = find_plot_by_position(row['geometry'].y, row['geometry'].x) #TODO: está del revés?
            if plot_obj:
                row['parcel_id'] = plot_obj['properties']['parcel_id']
                row['parcel_name'] = plot_obj['properties']['parcel']
            return row

        new_gdf = new_gdf.to_crs(4326).apply(apply_parcel_id, axis=1)
            
    return new_gdf

def _find_plots_by_parcel(parcel_criteria:dict)->list:
    return list(db.plots.find({'$or': parcel_criteria}, {
        'properties.provincia': 1,
        'properties.municipio': 1,
        'properties.parcela': 1,
        'properties.recinto': 1,
        '_id': 0
    }))

def _check_plots_duplicated(features: list[dict]) -> list[dict]:
    """ Performs a OR-query to fetch existing matches, then filters the input. """

    if not features:
        print("🟡 Duplicates skipped: 0")
        return []

    # Build unique criteria to keep the $or query compact
    keys = [
        (
            f['properties']['provincia'],
            f['properties']['municipio'],
            f['properties']['parcela'],
            f['properties']['recinto'],
        )
        for f in features
    ]

    unique_keys = set(keys)
    query_criteria = [
        {
            'properties.provincia': k[0],
            'properties.municipio': k[1],
            'properties.parcela': k[2],
            'properties.recinto': k[3],
        }
        for k in unique_keys
    ]

    existing = _find_plots_by_parcel(query_criteria) if query_criteria else []
    existing_set = set(
        (
            e['properties']['provincia'],
            e['properties']['municipio'],
            e['properties']['parcela'],
            e['properties']['recinto'],
        )
        for e in existing
    )

    no_duplicated_features = [
        f for f, k in zip(features, keys) if k not in existing_set
    ]

    n_duplicates = len(features) - len(no_duplicated_features) if features else 0
    print(f"🟡 Duplicates skipped: {n_duplicates}")
    return no_duplicated_features

"""
API module for SIGCore.
"""
#EXTERNAL IMPORTS
from contextlib import asynccontextmanager
import logging
import json
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

#INTERNAL IMPORTS
import config
import catastro
import mongo_api
from schemas import PlotCollection

#Logger initialization
@asynccontextmanager
async def lifespan(_: FastAPI):
    print("Initializing API...")
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(config.LOGS_FOLDER / "app.log"),
            ],
        )
    yield

#==============
#API Endpoints
#==============
app = FastAPI(title="SIGCore API", lifespan=lifespan)

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}

@app.post("/api/v1/plots/polygonize", response_model=PlotCollection)
async def polygonize_plots(request:Request):
    try:
        json_data = await request.json()
        plots_array = json_data['plots']
        main_gdf, error_df = catastro.polygonize_data_parallel(plots_array)

        if not main_gdf.empty:
            return JSONResponse(
                content={
                    "polygonized_data": json.loads(main_gdf.to_json()),
                    "errors": error_df.to_dict(orient='records')
                },
                status_code=200
            )
        else:
            return JSONResponse(content={
                "message": "No valid plots found.", 
                "errors": error_df.to_dict(orient='records'), 
                "polygonized_data": []}, 
                status_code=200)
    except KeyError as e:
        return JSONResponse(content={"message": str(e)}, status_code=400)
    except catastro.SigpacError as e:
        return JSONResponse(content={"message": str(e)}, status_code=502)
    except Exception as e:
        return JSONResponse(content={"message": str(e)}, status_code=500)

@app.get("/api/v1/plots/area/{parcel_id}")
async def get_parcel_area(parcel_id:str):
    try:
        plot_list = mongo_api.get_parcelario_by_id(parcel_id)
        if not 'dn_surface' in plot_list.columns:
            raise ValueError('dn_surface column not found in the plot list.')
        area = plot_list['dn_surface'].sum()

        return JSONResponse(content=area, status_code=200)
    except mongo_api.FieldNotFound as e:
        return JSONResponse(content={"message": str(e)}, status_code=405)
    except ValueError as e:
        return JSONResponse(content={"message": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse(content={"message": str(e)}, status_code=500)

@app.get("/api/v1/plots/{parcel_id}", response_model=PlotCollection)
async def get_field(parcel_id:str):
    """
    Endpoint to get the plot list of a field.
    
    Returns a Json with the plot list data.
    
    """
    try:
        parcelario_gdf= mongo_api.get_parcelario_by_id(parcel_id)
        parcelario = json.loads(parcelario_gdf.to_json())
        return JSONResponse(content=parcelario, status_code=200)
    except mongo_api.FieldNotFound as e:
        return JSONResponse(content={"message": str(e)}, status_code=405)
    except Exception as e:
        print(e)
        return JSONResponse(content={"message": str(e)}, status_code=500)

@app.get("/api/v1/plots/centroid/{parcel_id}")
async def get_field_centroid(parcel_id:str):
    try:
        centroid = mongo_api.get_parcel_centroid(parcel_id, crs=32630)
        out_json = {
            'x':centroid.x,
            'y':centroid.y,
            'crs':'epsg:32630'
        }

        return JSONResponse(content=out_json, status_code=200)
    except mongo_api.FieldNotFound as e:
        return JSONResponse(content={"message": str(e)}, status_code=405)
    except Exception as e:
        return JSONResponse(content={"message": str(e)}, status_code=500)

@app.get("/api/v1/plots/extent/{parcel_id}")
async def get_field_extent(parcel_id:str):
    try:
        parcelario_gdf = mongo_api.get_parcelario_by_id(parcel_id)
        extent = parcelario_gdf.to_crs(32630).total_bounds.tolist()  # [minx, miny, maxx, maxy]
        return JSONResponse(content=extent, status_code=200)
    except mongo_api.FieldNotFound as e:
        return JSONResponse(content={"message": str(e)}, status_code=405)
    except Exception as e:
        return JSONResponse(content={"message": str(e)}, status_code=500)

@app.post("/api/v1/plots/nearest_siar", response_model=list[str])
async def get_nearest_siar_station(data:PlotCollection):
    """
    Get the nearest SIAR stations to the centroid of the plots.
    
    Returns a list of SIAR station IDs.
    Example: ["A21", "B34", "C56"]
    """
    try:
        gdf = data.to_gdf()
        nearest_siar = catastro.get_siar_stations(gdf.union_all().centroid, 3)['Id Estación'].tolist()
        return JSONResponse(content=nearest_siar, status_code=200)
    except Exception as e:
        return JSONResponse(content={"message": str(e)}, status_code=500)
    

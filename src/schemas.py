from pydantic import BaseModel, Field
from typing import Generic, List, Optional, TypeVar, TypeAlias
import geopandas as gpd


class Geometry(BaseModel):
    type: str  # "Polygon, Point, Line, MultiPolygon"
    coordinates: List[List[List[float]]]  # [[[x, y], ...]]

TProps = TypeVar("TProps", bound=BaseModel) # TypeVar for generic feature properties

class PlotProperties(BaseModel):
    provincia: int
    municipio: int
    agregado: int
    zona: int
    poligono: int
    parcela: int
    recinto: int
    #parcel: str
    #client: str
    #crop: str
    #operating: bool
    #parcel_id: str

class Feature(BaseModel, Generic[TProps]):
    id: str
    type: str  # "Feature"
    properties: TProps
    geometry: Geometry

class FeatureCollection(BaseModel, Generic[TProps]):
    """
    Generic feature collection model for GeoJSON dictionaries.
    """
    type: str  # "FeatureCollection"
    features: List[Feature[TProps]]
    crs: Optional[dict] | None

    def to_gdf(self)->gpd.GeoDataFrame:
        data = self.model_dump()
        crs = data.get("crs", None)
        crs = crs["properties"]["name"] if crs else None
        return gpd.GeoDataFrame.from_features(data, crs=crs)

#Fastapi no puede usar clases genéricas, por lo que se necesita crear un alias
PlotCollection: TypeAlias = FeatureCollection[PlotProperties]

# Uso:
# fc = FeatureCollection[PlotProperties].model_validate_json(open("plots.json").read())

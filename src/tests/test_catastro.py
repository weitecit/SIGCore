import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock, Mock
import sys
from pathlib import Path
from shapely.geometry.point import Point
import geopandas as gpd


# Import the module to test
try:
    import catastro
    from catastro import *
except ImportError:
    scripts_dir = Path(__file__).parent.parent
    sys.path.insert(0, str(scripts_dir))
    import catastro
    from catastro import *

"""
expected xls columns: Index(['Id Registro', 'Cliente', 'Nombre Finca', 'Cultivo', 'Provincia',
    'Municipio', 'Agregado', 'Zona', 'Poligono', 'Parcela', 'Recinto',
    'Superficie (Ha)', 'Ref. Catastral', 'Servicio'],
    dtype='object')
"""
ASSETS_DIR = Path(__file__).parent / "assets"
dummy_xls = str(ASSETS_DIR / "dummy_parcel.xlsx")
dummy_geojson = str(ASSETS_DIR / "dummy_geojson.geojson")
dummy_empty_geojson = str(ASSETS_DIR / "dummy_empty_geojson.geojson")

# Test coordinates for polygonize function
TEST_COORDS = [
    (-0.5, 38.5), (-0.5, 39.0), (0.0, 39.0), (0.0, 38.5), (-0.5, 38.5)
]

class TestCatastro:
    """Tests for the Catastro module"""

    @pytest.fixture
    def sample_parcel_data(self):
        """Sample parcel data for testing"""
        return {
            'province': ['03'],
            'municipality': ['001'],
            'polygon': [1],
            'plot_number': [1],
            'enclosure': [1],
            'field': ['Test Field'],
            'client': ['Test Client'],
            'crop': ['Test Crop']
        }

    @pytest.fixture
    def deleted_provincia_column(self):
        """DataFrame with 'Provincia' column removed"""
        df = open_data(dummy_xls)
        df.drop('province', axis=1, inplace=True)
        return df

    @pytest.fixture
    def dummy_df(self):
        """Sample DataFrame from dummy Excel file"""
        return open_data(dummy_xls)
        
    @pytest.fixture
    def mock_geodataframe(self):
        """Mock GeoDataFrame for testing"""
        from shapely.geometry import Point
        import geopandas as gpd
        
        return gpd.GeoDataFrame(
            {
                'field': ['Test Field'],
                'geometry': [Point(0, 0)]
            },
            crs='EPSG:4326'
        )

    def test_missing_columns_none_missing(self, dummy_df):
        """Test missing_columns with all required columns present"""
        result = missing_columns(dummy_df)
        assert result == []

    def test_missing_columns_provincia_missing(self, deleted_provincia_column):
        """Test missing_columns when 'province' is missing"""
        result = missing_columns(deleted_provincia_column)
        assert result == ['province']

    def test_open_xls_unsupported_type(self):
        """Test open_xls with unsupported data type"""
        with pytest.raises(Exception) as exinfo:
            open_data(np.array([1,2,3]))
        assert "Unsupported type" in str(exinfo.value)

    def test_polygonize_data_parallel_raises_keyerror_on_missing_required_columns(self):
        df = pd.DataFrame([
            {
                'municipality': 11,
                'polygon': 3,
                'plot_number': 80,
                'enclosure': np.nan,
            }
        ])

        with pytest.raises(KeyError):
            catastro.polygonize_data_parallel(df)
        

    @patch("catastro.gpd.read_file")
    def test_get_siar_stations_filters_only_active(self, mock_read_file):
        stations = gpd.GeoDataFrame(
            {
                "Estado": ["Activa", "Inactiva", "Activa"],
                "Id Estación": ["A", "B", "C"],
                "geometry": [Point(0, 0), Point(10, 0), Point(1, 0)],
            },
            crs="EPSG:4326",
        )
        mock_read_file.return_value = stations

        out = catastro.get_siar_stations()

        assert list(out["Id Estación"]) == ["A", "C"]
        assert "index" in out.columns


    @patch("catastro.gpd.read_file")
    def test_get_siar_stations_returns_nearest_n(self, mock_read_file):
        stations = gpd.GeoDataFrame(
            {
                "Estado": ["Activa", "Activa", "Activa"],
                "Id Estación": ["A", "B", "C"],
                "geometry": [Point(0, 0), Point(2, 0), Point(1, 0)],
            },
            crs="EPSG:4326",
        )
        mock_read_file.return_value = stations

        out = catastro.get_siar_stations(point=Point(0, 0), n_nearest=2)

        assert list(out["Id Estación"]) == ["A", "C"]
        assert "distance" in out.columns


# Run the tests if this file is executed directly
if __name__ == '__main__':
    pytest.main([__file__, '-v'])

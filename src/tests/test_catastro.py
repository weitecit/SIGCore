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

    @patch('catastro._download_parcel')
    @patch('catastro._download_enclosure')
    def test_polygonize_data_parallel_calls_download_parcel_when_enclosure_nan(self, mock_download_enclosure, mock_download_parcel):
        import geopandas as gpd
        from shapely.geometry import Polygon

        plot_row = {
            'province': 12,
            'municipality': 11,
            'polygon': 3,
            'plot_number': 80,
            'enclosure': np.nan,
            'field': 'Finca El Olivar',
            'client': 'Test Client',
            'crop': 'limón',
            'operating': 1,
            'cadastral_ref': 'XYZ',
        }
        df = pd.DataFrame([plot_row])

        detected = gpd.GeoDataFrame(
            {'geometry': [Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])]},
            crs='EPSG:4326',
        )
        mock_download_parcel.return_value = detected
        mock_download_enclosure.return_value = gpd.GeoDataFrame()

        out_gdf, err_df = catastro.polygonize_data_parallel(df, max_workers=1)

        mock_download_parcel.assert_called_once_with(12, 11, 3, 80)
        mock_download_enclosure.assert_not_called()

        assert len(err_df) == 0
        assert len(out_gdf) == 1
        assert out_gdf.iloc[0]['field'] == 'Finca El Olivar'
        assert out_gdf.iloc[0]['client'] == 'Test Client'
        assert out_gdf.iloc[0]['crop'] == 'limón'
        assert out_gdf.iloc[0]['operating'] is np.True_
        assert out_gdf.iloc[0]['cadastral_ref'] == 'XYZ'

    @patch('catastro._download_parcel')
    @patch('catastro._download_enclosure')
    def test_polygonize_data_parallel_calls_download_enclosure_when_enclosure_present(self, mock_download_enclosure, mock_download_parcel):
        import geopandas as gpd
        from shapely.geometry import Polygon

        plot_row = {
            'province': 12,
            'municipality': 11,
            'polygon': 3,
            'plot_number': 112,
            'enclosure': 25,
            'field': 'Finca El Olivar',
        }
        df = pd.DataFrame([plot_row])

        detected = gpd.GeoDataFrame(
            {'geometry': [Polygon([(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)])]},
            crs='EPSG:4326',
        )
        mock_download_enclosure.return_value = detected
        mock_download_parcel.return_value = gpd.GeoDataFrame()

        out_gdf, err_df = catastro.polygonize_data_parallel(df, max_workers=1)

        mock_download_enclosure.assert_called_once_with(12, 11, 3, 112, 25)
        mock_download_parcel.assert_not_called()

        assert len(err_df) == 0
        assert len(out_gdf) == 1
        assert out_gdf.iloc[0]['field'] == 'Finca El Olivar'

    @patch('catastro._download_parcel')
    def test_polygonize_data_parallel_adds_not_found_when_detected_empty(self, mock_download_parcel):
        import geopandas as gpd

        df = pd.DataFrame([
            {
                'province': 12,
                'municipality': 11,
                'polygon': 3,
                'plot_number': 80,
                'enclosure': np.nan,
                'field': 'Finca',
            }
        ])

        mock_download_parcel.return_value = gpd.GeoDataFrame()

        out_gdf, err_df = catastro.polygonize_data_parallel(df, max_workers=1)

        assert len(out_gdf) == 0
        assert len(err_df) == 1
        assert err_df.iloc[0]['error'] == 'NOT FOUND'
        assert err_df.iloc[0]['field'] == 'Finca'

    @patch('catastro._download_enclosure', side_effect=KeyError('boom'))
    def test_polygonize_data_parallel_download_exception_goes_to_errors(self, mock_download_enclosure):
        df = pd.DataFrame([
            {
                'province': 12,
                'municipality': 11,
                'polygon': 3,
                'plot_number': 112,
                'enclosure': 25,
                'field': 'Finca',
            }
        ])

        out_gdf, err_df = catastro.polygonize_data_parallel(df, max_workers=1)

        assert len(out_gdf) == 0
        assert len(err_df) == 1
        assert 'boom' in str(err_df.iloc[0]['error'])

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

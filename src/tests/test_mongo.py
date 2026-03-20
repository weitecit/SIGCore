import pytest
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from unittest.mock import patch, MagicMock, Mock
import sys
from pathlib import Path

# Importar el módulo a testear
try:
    import mongo_api
except ImportError:
    # Si no se puede importar, agregar el path
    scripts_dir = Path(__file__).parent.parent
    sys.path.insert(0, str(scripts_dir))
    import mongo_api


@pytest.fixture
def sample_gdf_with_field():
    """GeoDataFrame con columna 'field' (se renombrará a 'parcel')."""
    geometries = [Point(0, 0), Point(1, 1), Point(2, 2)]
    return gpd.GeoDataFrame(
        {
            'field': ['campo1', 'campo2', 'campo3'],
            'provincia': ['28', '28', '29'],
            'municipio': ['001', '002', '001'],
            'parcela': ['001', '002', '003'],
            'recinto': ['A', 'B', 'C'],
            'geometry': geometries,
        },
        crs='EPSG:4326',
    )


@pytest.fixture
def sample_gdf_with_parcel():
    """GeoDataFrame con columna 'parcel' ya presente."""
    geometries = [Point(0, 0), Point(1, 1), Point(2, 2)]
    return gpd.GeoDataFrame(
        {
            'parcel': ['campo1', 'campo2', 'campo3'],
            'provincia': ['28', '28', '29'],
            'municipio': ['001', '002', '001'],
            'parcela': ['001', '002', '003'],
            'recinto': ['A', 'B', 'C'],
            'geometry': geometries,
        },
        crs='EPSG:4326',
    )


class TestMongoAPI:
    """Tests para el módulo mongo_api actualizado"""

    @patch.object(mongo_api, 'get_parcel_id')
    @patch('mongo_api.db')
    def test_upload_plotlist_from_dataframe_with_override(self, mock_db, mock_get_field_id, sample_gdf_with_field):
        """Test upload_plotlist_from_dataframe con override_fields=True"""
        # Renombrado interno de 'field' -> 'parcel' y borrado previo por parcel_id
        mock_get_field_id.return_value = 'field_id_123'

        # Mock delete_many
        delete_result = Mock()
        delete_result.deleted_count = 5
        mock_db.plots.delete_many.return_value = delete_result

        # Mock de la función print para capturar la salida
        with patch('builtins.print') as mock_print:
            mongo_api.upload_plotlist_from_dataframe(sample_gdf_with_field, override_fields=True)

            # Verificar que se llamó delete_many para cada campo único (tras renombrado a 'parcel')
            assert mock_db.plots.delete_many.call_count == len(sample_gdf_with_field['field'].unique())
            # Verificar que se usó el criterio correcto (parcel_id)
            mock_db.plots.delete_many.assert_called_with({'properties.parcel_id': 'field_id_123'})
            # Verificar que se imprimió el mensaje de éxito
            mock_print.assert_any_call("🗑️ Deleted 5 plots")

    def test_upload_plotlist_from_dataframe_field_rename(self, sample_gdf_with_field):
        """Test que se renombra 'field' a 'parcel'"""
        with patch.object(mongo_api, '_gdf_to_mongo_structure') as mock_gdf_to_mongo, \
             patch.object(mongo_api, '_check_plots_duplicated') as mock_check_dups, \
             patch('mongo_api.db') as mock_db:
            
            mock_features = [{'properties': {'parcel': 'test'}}] * 3
            mock_gdf_to_mongo.return_value = mock_features
            mock_check_dups.return_value = mock_features
            
            insert_result = Mock()
            insert_result.inserted_ids = ['id1', 'id2', 'id3']
            mock_db.plots.insert_many.return_value = insert_result
            
            result = mongo_api.upload_plotlist_from_dataframe(sample_gdf_with_field)
            
            # Verificar que se llamó gdf_to_mongo_structure con el DataFrame que tiene 'parcel', no 'field'
            called_gdf = mock_gdf_to_mongo.call_args[0][0]
            assert 'parcel' in called_gdf.columns
            assert 'field' not in called_gdf.columns

    @patch('mongo_api.db')
    def test_upload_plotlist_from_dataframe_success(self, mock_db, sample_gdf_with_parcel):
        """Test upload exitoso de dataframe con 'parcel'"""
        # Mock insert_many
        insert_result = Mock()
        insert_result.inserted_ids = ['id1', 'id2', 'id3']
        mock_db.plots.insert_many.return_value = insert_result
        
        with patch.object(mongo_api, '_gdf_to_mongo_structure') as mock_gdf_to_mongo, \
             patch.object(mongo_api, '_check_plots_duplicated') as mock_check_dups:
            
            mock_features = [{'properties': {'parcel': 'test'}}] * 3
            mock_gdf_to_mongo.return_value = mock_features
            mock_check_dups.return_value = mock_features
            
            mongo_api.upload_plotlist_from_dataframe(sample_gdf_with_parcel)
            
            mock_db.plots.insert_many.assert_called_once_with(mock_features, ordered=False)


    @patch('mongo_api.db')
    def test_get_field_plots_updated_query(self, mock_db):
        """Test que get_field_plots usa 'parcel' en lugar de 'field'"""
        mock_db.plots.find.return_value = [
            {'properties': {'parcel': 'test_field', 'parcela': 'P001'}}
        ]
        
        result = mongo_api.find_field_plots('test_field')
        
        assert len(result) == 1
        # Verificar que se usó la consulta correcta con 'parcel'
        call_args = mock_db.plots.find.call_args[0][0]
        assert '$or' in call_args
        or_conditions = call_args['$or']
        # Debe buscar por 'properties.parcel' y 'properties.parcel_id'
        parcel_condition = or_conditions[0]
        parcel_id_condition = or_conditions[1]
        assert 'properties.parcel' in parcel_condition
        assert 'properties.parcel_id' in parcel_id_condition

    @patch('mongo_api.db')
    def test_get_plot_by_position_2(self, mock_db):
        """Test función nueva get_plot_by_position_2"""
        mock_db.plots.aggregate.return_value = [
            {'properties': {'parcel': 'test', 'parcel_id': 'P123'}}
        ]
        
        result = mongo_api.find_plot_by_position(0.0, 0.0)
        
        assert result is not None
        assert result['properties']['parcel'] == 'test'
        
        # Verificar que se llamó aggregate con los parámetros correctos
        call_args = mock_db.plots.aggregate.call_args[0][0]
        assert len(call_args) == 4  # $geoNear, $sort, $limit, $project
        assert '$geoNear' in call_args[0]

    @patch.object(mongo_api, '_mongo_to_gdf')
    @patch.object(mongo_api, 'find_field_plots')
    def test_get_parcelario_only_operating_true_filters_rows(self, mock_find_field_plots, mock_mongo_to_gdf):
        """Test get_parcelario filtra por operating cuando only_operating=True"""
        gdf = gpd.GeoDataFrame(
            {
                'operating': [True, False, True],
                'geometry': [Point(0, 0), Point(1, 1), Point(2, 2)],
            },
            crs='EPSG:4258',
        )

        mock_find_field_plots.return_value = [{'properties': {}}, {'properties': {}}, {'properties': {}}]
        mock_mongo_to_gdf.return_value = gdf

        result = mongo_api.get_parcelario(field_name=None, only_operating=True)

        assert len(result) == 2
        assert result['operating'].all()

    @patch.object(mongo_api, '_mongo_to_gdf')
    @patch.object(mongo_api, 'find_field_plots')
    def test_get_parcelario_only_operating_false_does_not_filter(self, mock_find_field_plots, mock_mongo_to_gdf):
        """Test get_parcelario no filtra por operating cuando only_operating=False"""
        gdf = gpd.GeoDataFrame(
            {
                'operating': [True, False, True],
                'geometry': [Point(0, 0), Point(1, 1), Point(2, 2)],
            },
            crs='EPSG:4258',
        )

        mock_find_field_plots.return_value = [{'properties': {}}, {'properties': {}}, {'properties': {}}]
        mock_mongo_to_gdf.return_value = gdf

        result = mongo_api.get_parcelario(field_name=None, only_operating=False)

        assert len(result) == 3
        assert result['operating'].tolist() == [True, False, True]

    @patch.object(mongo_api, '_mongo_to_gdf')
    @patch('mongo_api.db')
    def test_get_parcelario_by_id_only_operating_true_filters_rows(self, mock_db, mock_mongo_to_gdf):
        """Test get_parcelario_by_id filtra por operating cuando only_operating=True"""
        gdf = gpd.GeoDataFrame(
            {
                'operating': [True, False, True],
                'geometry': [Point(0, 0), Point(1, 1), Point(2, 2)],
            },
            crs='EPSG:4258',
        )

        mock_db.plots.find.return_value = [{'properties': {}}, {'properties': {}}, {'properties': {}}]
        mock_mongo_to_gdf.return_value = gdf

        result = mongo_api.get_parcelario_by_id('P123', only_operating=True)

        assert len(result) == 2
        assert result['operating'].all()

    @patch.object(mongo_api, '_mongo_to_gdf')
    @patch('mongo_api.db')
    def test_get_parcelario_by_id_only_operating_false_does_not_filter(self, mock_db, mock_mongo_to_gdf):
        """Test get_parcelario_by_id no filtra por operating cuando only_operating=False"""
        gdf = gpd.GeoDataFrame(
            {
                'operating': [True, False, True],
                'geometry': [Point(0, 0), Point(1, 1), Point(2, 2)],
            },
            crs='EPSG:4258',
        )

        mock_db.plots.find.return_value = [{'properties': {}}, {'properties': {}}, {'properties': {}}]
        mock_mongo_to_gdf.return_value = gdf

        result = mongo_api.get_parcelario_by_id('P123', only_operating=False)

        assert len(result) == 3
        assert result['operating'].tolist() == [True, False, True]

    def test_get_plot_by_position_2_no_results(self):
        """Test get_plot_by_position_2 sin resultados"""
        with patch('mongo_api.db') as mock_db:
            mock_db.plots.aggregate.return_value = []
            
            result = mongo_api.find_plot_by_position(0.0, 0.0)
            
            assert result is None

    @patch.object(mongo_api, 'find_plot_by_position')
    def test_add_metadata_with_parcel_lookup(self, mock_get_plot, sample_gdf_with_parcel):
        """Test agregar metadata con búsqueda de parcela por posición (actualizado)"""
        # Mock de get_plot_by_position_2
        mock_get_plot.return_value = {
            'properties': {
                'parcel_id': 'P123',
                'parcel': 'Parcela Test'  # Cambió de 'parcel' a 'parcel'
            }
        }
        
        # Metadata sin parcel_id especificado
        custom_metadata = {'parcel_id': None, 'parcel_name': None}
        
        result = mongo_api._apply_points_model(sample_gdf_with_parcel, custom_metadata)
        
        # Verificar que se agregaron los datos de parcela
        assert result['parcel_id'].iloc[0] == 'P123'
        assert result['parcel_name'].iloc[0] == 'Parcela Test'

    def test_check_plots_duplicated_no_duplicates(self):
        """Test check duplicados cuando no hay duplicados"""
        mock_features = [
            {'properties': {'provincia': '28', 'municipio': '001', 'parcela': '001', 'recinto': 'A'}},
            {'properties': {'provincia': '28', 'municipio': '002', 'parcela': '002', 'recinto': 'B'}}
        ]
        
        with patch.object(mongo_api, '_find_plots_by_parcel') as mock_find:
            mock_find.return_value = []  # No duplicados en DB
            
            new_features = mongo_api._check_plots_duplicated(mock_features)
            
            assert len(new_features) == 2

    def test_check_plots_duplicated_with_duplicates(self):
        """Test check duplicados cuando hay duplicados"""
        mock_features = [
            {'properties': {'provincia': '28', 'municipio': '001', 'parcela': '001', 'recinto': 'A'}},
            {'properties': {'provincia': '28', 'municipio': '002', 'parcela': '002', 'recinto': 'B'}}
        ]
        
        existing_in_db = [
            {
                'properties': {
                    'provincia': '28',
                    'municipio': '001',
                    'parcela': '001',
                    'recinto': 'A'
                }
            }
        ]
        
        with patch.object(mongo_api, '_find_plots_by_parcel') as mock_find:
            mock_find.return_value = existing_in_db
            
            new_features = mongo_api._check_plots_duplicated(mock_features)
            
            assert len(new_features) == 1  # Solo uno no está duplicado


class TestBackwardCompatibility:
    """Tests para verificar compatibilidad con cambios"""

    def test_field_to_parcel_rename_consistency(self, sample_gdf_with_field):
        """Test que el renombrado es consistente en todo el flujo"""
        with patch.object(mongo_api, 'get_parcel_id') as mock_get_field_id, \
             patch('mongo_api.db') as mock_db:
            
            mock_get_field_id.return_value = 'test_id'
            
            # Test upload_plotlist_from_dataframe
            mock_db.plots.insert_many.return_value = Mock(inserted_ids=['id1'])
            
            with patch.object(mongo_api, '_gdf_to_mongo_structure') as mock_gdf_to_mongo, \
                 patch.object(mongo_api, '_check_plots_duplicated') as mock_check_dups:
                mock_gdf_to_mongo.return_value = [{'properties': {'parcel': 'test'}}]
                mock_check_dups.return_value = [{'properties': {'parcel': 'test'}}]
                
                mongo_api.upload_plotlist_from_dataframe(sample_gdf_with_field)
                
                # Verificar que el proceso funcionó con el renombrado
                mock_db.plots.insert_many.assert_called_once_with([{'properties': {'parcel': 'test'}}], ordered=False)


class TestErrorHandling:
    """Tests para manejo de errores específicos"""

    def test_gdf_to_mongo_structure_no_crs_error(self):
        """Test error cuando GDF no tiene CRS"""
        gdf_no_crs = gpd.GeoDataFrame({
            'parcel': ['campo1'],
            'geometry': [Point(0, 0)]
        })  # Sin especificar CRS
        
        with pytest.raises(Exception, match="The GeoDataframe has no CRS"):
            mongo_api._gdf_to_mongo_structure(gdf_no_crs)

    def test_get_parcelario_field_not_found(self):
        """Test error cuando campo no se encuentra"""
        with patch.object(mongo_api, 'find_field_plots') as mock_get_plots:
            mock_get_plots.return_value = []
            
            with pytest.raises(mongo_api.FieldNotFound, match="Field 'test_field' not found in database"):
                mongo_api.get_parcelario('test_field')


class TestUtilityFunctions:
    """Tests para funciones utilitarias"""

    def test_module_constants(self):
        """Test que las constantes del módulo están definidas"""
        assert hasattr(mongo_api, 'client')
        assert hasattr(mongo_api, 'db')

    @patch('mongo_api.db')
    def test_find_plots_by_parcel(self, mock_db):
        """Test búsqueda de plots por parcela"""
        mock_db.plots.find.return_value = [
            {'properties': {'provincia': '28', 'municipio': '001', 'parcela': '001', 'recinto': 'A'}}
        ]
        
        criteria = [{'properties.provincia': '28', 'properties.municipio': '001'}]
        result = mongo_api._find_plots_by_parcel(criteria)
        
        assert len(result) == 1
        mock_db.plots.find.assert_called_once()


# Configuración para ejecutar los tests
if __name__ == '__main__':
    pytest.main([__file__, '-v'])
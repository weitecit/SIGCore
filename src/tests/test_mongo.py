import pytest
import geopandas as gpd
from shapely.geometry import Point
from unittest.mock import patch
import mongo_api


class TestMongoAPI:
    """Tests para el módulo mongo_api actualizado"""


    @patch('mongo_api.db')
    def test_get_field_plots_updated_query(self, mock_db):
        """Test que get_field_plots usa 'layers' collection con 'layer_type': 'plot'"""
        mock_db.layers.find.return_value = [
            {'properties': {'parcel': 'test_field', 'parcela': 'P001'}}
        ]
        
        result = mongo_api.find_field_plots('test_field')
        
        assert len(result) == 1
        # Verificar que se usó la consulta correcta con 'parcel' y 'layer_type': 'plot'
        call_args = mock_db.layers.find.call_args[0][0]
        assert '$or' in call_args
        assert call_args['layer_type'] == 'plot'
        or_conditions = call_args['$or']
        # Debe buscar por 'properties.parcel' y 'properties.parcel_id'
        parcel_condition = or_conditions[0]
        parcel_id_condition = or_conditions[1]
        assert 'properties.parcel' in parcel_condition
        assert 'properties.parcel_id' in parcel_id_condition

    @patch('mongo_api.db')
    def test_get_field_plots_none_field_name(self, mock_db):
        """Test que get_field_plots con field_name=None usa layer_type: 'plot'"""
        mock_db.layers.find.return_value = [
            {'properties': {'parcel': 'field1', 'parcela': 'P001'}},
            {'properties': {'parcel': 'field2', 'parcela': 'P002'}}
        ]
        
        result = mongo_api.find_field_plots(None)
        
        assert len(result) == 2
        # Verificar que se usó la consulta correcta solo con layer_type: 'plot'
        call_args = mock_db.layers.find.call_args[0][0]
        assert call_args == {'layer_type': 'plot'}

    @patch('mongo_api.db')
    def test_get_plot_by_position_2(self, mock_db):
        """Test función find_plot_by_position con layers collection"""
        mock_db.layers.aggregate.return_value = [
            {'properties': {'parcel': 'test', 'parcel_id': 'P123'}}
        ]
        
        result = mongo_api.find_plot_by_position(0.0, 0.0)
        
        assert result is not None
        assert result['properties']['parcel'] == 'test'
        
        # Verificar que se llamó aggregate con los parámetros correctos
        call_args = mock_db.layers.aggregate.call_args[0][0]
        assert len(call_args) == 5  # $geoNear, $match, $sort, $limit, $project
        assert '$geoNear' in call_args[0]
        # Verificar que incluye el filtro layer_type: 'plot'
        match_stage = None
        for stage in call_args:
            if '$match' in stage:
                match_stage = stage
                break
        assert match_stage is not None
        assert match_stage['$match']['layer_type'] == 'plot'

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

        mock_db.layers.find.return_value = [{'properties': {}}, {'properties': {}}, {'properties': {}}]
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

        mock_db.layers.find.return_value = [{'properties': {}}, {'properties': {}}, {'properties': {}}]
        mock_mongo_to_gdf.return_value = gdf

        result = mongo_api.get_parcelario_by_id('P123', only_operating=False)

        assert len(result) == 3
        assert result['operating'].tolist() == [True, False, True]

    def test_get_plot_by_position_2_no_results(self):
        """Test get_plot_by_position_2 sin resultados"""
        with patch('mongo_api.db') as mock_db:
            mock_db.layers.aggregate.return_value = []
            
            result = mongo_api.find_plot_by_position(0.0, 0.0)
            
            assert result is None


class TestErrorHandling:
    """Tests para manejo de errores específicos"""

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



# Configuración para ejecutar los tests
if __name__ == '__main__':
    pytest.main([__file__, '-v'])
import pandas as pd
import numpy as np
from typing import Any, Callable, Dict
import geopandas as gpd
from pyproj import Transformer
from shapely.ops import transform


def _concat_partitions(partitioned_input: Dict[str, Callable[[], Any]]) -> pd.DataFrame:
    """Concatenate input partitions into one pandas DataFrame.

    Args:
        partitioned_input: A dictionary with partition ids as keys and load functions as values.

    Returns:
        Pandas DataFrame representing a concatenation of all loaded partitions.
    """
    result = pd.DataFrame()

    for partition_key, partition_load_func in sorted(partitioned_input.items()):
        partition_data = partition_load_func()  # load the actual partition data
        # concat with existing result
        result = pd.concat([result, partition_data],
                           ignore_index=True, sort=True)

    return result


def _parse_date(x: pd.Series) -> pd.Series:
    x = pd.to_datetime(x, format='mixed')
    y = x.dt.time.astype(str)
    x = x.dt.date.astype(str)
    return x, y


def _get_time_of_day(hour: int) -> str:

    if 5 <= hour < 12:
        return 'morning'
    elif 12 <= hour < 17:
        return 'afternoon'
    elif 17 <= hour < 21:
        return 'evening'
    else:
        return 'night'


def _rename_columns(x: pd.DataFrame, column_names: dict) -> pd.DataFrame:
    x.rename(columns=column_names, inplace=True)
    return x


def _drop_columns(x: pd.DataFrame, column_names: list) -> pd.DataFrame:
    x.drop(columns=column_names, inplace=True)
    return x


def _parse_integers(x: pd.Series) -> pd.Series:
    x = pd.to_numeric(x, errors='coerce').fillna(0)
    x = x.astype(int)
    return x


def _rename_incidents(x: pd.Series, map_of_values: dict) -> pd.Series:
    x = x.replace(map_of_values)
    return x


def _create_id(x: pd.DataFrame) -> pd.Series:
    y = x.index
    return y


def _remove_incident_outliers(x: pd.DataFrame, values_to_drop: dict) -> pd.DataFrame:
    for column, value in values_to_drop.items():
        x = x[x[column] != value]
    return x


def _remove_incident_NaN(x: pd.DataFrame, columns_to_check: list) -> pd.DataFrame:
    x = x.dropna(subset=columns_to_check)
    return x


def _remove_incidents_2024(x: pd.DataFrame, column_name: str) -> pd.DataFrame:
    # Drop rows where the year is 2024
    x = x[x[column_name] != 2024]
    return x


def _combine_neighbourhoods_and_city(x: pd.Series, y: pd.Series) -> pd.Series:
    z = x.combine_first(y)
    return z


def _get_latitude_longitude(x: pd.DataFrame) -> pd.DataFrame:
    # Convert from geographic (WGS84) to a projected CRS: UTM zone 13N for part of North America
    data_projected = x.to_crs(epsg=32613)

    # Calculate centroids in the projected CRS
    data_projected['geometry'] = data_projected['geometry'].centroid

    # Convert back to the original geographic CRS (WGS84)
    x['geometry'] = data_projected['geometry'].to_crs(epsg=4326)

    # Extract latitude and longitude
    x['LATITUDE'] = x['geometry'].y
    x['LONGITUDE'] = x['geometry'].x

    # Convert GeoDataFrame to DataFrame
    x.drop(columns=['geometry'], inplace=True)

    return x


def combine_incidents(partitioned_input: Dict[str, Callable[[], Any]]) -> pd.DataFrame:
    """TODO"""
    incidents = _concat_partitions(partitioned_input)
    incidents['INCIDENT_ID'] = _create_id(incidents)

    return incidents


def preprocess_incidents(incidents: pd.DataFrame) -> pd.DataFrame:

    drop_columns = ["INCIDENT_NBR", "NOM_ARROND",
                    "DIVISION", "NOM_VILLE", 'MTM8_X', 'MTM8_Y']
    incidents = _drop_columns(incidents, column_names=drop_columns)

    # Rename columns
    new_incidents_column_names = {
        'CASERNE': 'DISPATCHED_FIRE_STATION_ID',
        'INCIDENT_TYPE_DESC': 'INCIDENT_TYPE',
        'DESCRIPTION_GROUPE': 'INCIDENT_CATEGORY',
        'NOMBRE_UNITES': 'UNITS_DEPLOYED',
    }
    incidents = _rename_columns(
        incidents, column_names=new_incidents_column_names)

    map_of_incidents_values = {
        'SANS FEU': 'Sans incendie',
        'FAU-ALER': 'Fausses alertes/annulations',
        '1-REPOND': 'Premier répondant',
        'AUTREFEU': 'Autres incendies',
        'INCENDIE': 'Incendies de bâtiments',
    }
    incidents['INCIDENT_CATEGORY'] = _rename_incidents(
        incidents['INCIDENT_CATEGORY'], map_of_incidents_values)

    outliers_to_drop = {
        'INCIDENT_CATEGORY': 'NOUVEAU',
        'LATITUDE': 0
    }
    incidents = _remove_incident_outliers(incidents, outliers_to_drop)

    columns_to_check = ['LATITUDE', 'LONGITUDE', 'INCIDENT_CATEGORY']
    incidents = _remove_incident_NaN(incidents, columns_to_check)

    # Convert the 'datetime' column to datetime objects
    incidents['CREATION_DATE_TIME'] = pd.to_datetime(
        incidents['CREATION_DATE_TIME'], format='mixed'
    )

    incidents['CREATION_DATE'], incidents['CREATION_TIME'] = _parse_date(
        incidents['CREATION_DATE_TIME'])
    # Extract date and time buckets
    incidents['day_of_week'] = incidents['CREATION_DATE_TIME'].dt.dayofweek
    incidents['month'] = incidents['CREATION_DATE_TIME'].dt.month
    incidents['year'] = incidents['CREATION_DATE_TIME'].dt.year
    incidents['hour'] = incidents['CREATION_DATE_TIME'].dt.hour
    incidents['time_of_day'] = incidents['hour'].apply(_get_time_of_day)

    # Drop the initial column after the datetime data is extracted
    incidents.drop(columns=['CREATION_DATE_TIME'], inplace=True)

    # Remove incidents from 2024
    incidents = _remove_incidents_2024(incidents, 'year')

    # Convert units deployed into integers
    incidents['UNITS_DEPLOYED'] = _parse_integers(incidents['UNITS_DEPLOYED'])

    return incidents


def preprocess_firestations(firestations: pd.DataFrame) -> pd.DataFrame:

    # Rename columns
    new_firestations_column_names = {
        'CASERNE': 'FIRE_STATION_ID',
        'NO_CIVIQUE': 'STREET_NUMBER',
        'RUE': 'STREET_NAME',
        'ARRONDISSEMENT': 'NEIGHBORHOOD',
        'VILLE': 'CITY',
        'NOM_RUE': 'STREET_NAME',
        'DATE_DEBUT': 'START_DATE',
        'DATE_FIN': 'END_DATE'
    }
    firestations = _rename_columns(firestations, new_firestations_column_names)

    firestations['AREA'] = _combine_neighbourhoods_and_city(
        firestations['NEIGHBORHOOD'], firestations['CITY']
    )

    drop_firestation_columns = ['NEIGHBORHOOD', 'CITY', 'MTM8_X', 'MTM8_Y']
    firestations = _drop_columns(
        firestations, column_names=drop_firestation_columns)

    firestations['START_DATE'], _ = _parse_date(firestations['START_DATE'])

    firestations['END_DATE'], _ = _parse_date(firestations['END_DATE'])

    return firestations


def preprocess_firestation_areas(firestation_areas: dict) -> pd.DataFrame:

    drop_columns_firestation_areas = ['NOM_CAS_AD', 'OBJECTID']
    firestation_areas = _drop_columns(
        firestation_areas, column_names=drop_columns_firestation_areas)

    # Rename columns
    new_firestation_areas_column_names = {
        'NO_CAS_ADM': 'FIRE_STATION_ID',
    }
    firestation_areas = _rename_columns(
        firestation_areas, new_firestation_areas_column_names)

    return firestation_areas


def preprocess_property_assessments(property_assessments: dict) -> pd.DataFrame:

    property_assessments = _get_latitude_longitude(property_assessments)

    drop_columns_property_assessments = ['MUNICIPALITE', 'CIVIQUE_DEBUT', 'CIVIQUE_FIN', 'NOM_RUE',
                                         'SUITE_DEBUT', 'LETTRE_DEBUT', 'LETTRE_DEBUT', 'LETTRE_FIN', 'MATRICULE83', 'NO_ARROND_ILE_CUM']

    property_assessments = _drop_columns(
        property_assessments, column_names=drop_columns_property_assessments)

    # Rename columns
    new_property_assessments_column_names = {
        'ID_UEV': 'ASSESSMENT_ID',
        'ETAGE_HORS_SOL': 'ABOVE_GROUND_FLOORS',
        'NOMBRE_LOGEMENT': 'HOUSING_UNITS',
        'ANNEE_CONSTRUCTION': 'YEAR_CONSTRUCTION',
        'CODE_UTILISATION': 'USE_CODE',
        'LIBELLE_UTILISATION': 'USE_DESCRIPTION',
        'CATEGORIE_UEF': 'USE_CATEGORY',
        'SUPERFICIE_TERRAIN': 'AREA_LAND',
        'SUPERFICIE_BATIMENT': 'AREA_BUILDING',
    }
    property_assessments = _rename_columns(
        property_assessments, new_property_assessments_column_names)

    # Parse columns containing integers
    property_assessments['ABOVE_GROUND_FLOORS'] = _parse_integers(
        property_assessments['ABOVE_GROUND_FLOORS'])
    property_assessments['HOUSING_UNITS'] = _parse_integers(
        property_assessments['HOUSING_UNITS'])
    property_assessments['YEAR_CONSTRUCTION'] = _parse_integers(
        property_assessments['YEAR_CONSTRUCTION'])
    property_assessments['USE_CODE'] = _parse_integers(
        property_assessments['USE_CODE'])
    property_assessments['AREA_LAND'] = _parse_integers(
        property_assessments['AREA_LAND'])
    property_assessments['AREA_BUILDING'] = _parse_integers(
        property_assessments['AREA_BUILDING'])

    # Add NaN when YEAR_CONSTRUCTION > current year
    property_assessments['YEAR_CONSTRUCTION'] = np.where(
        property_assessments['YEAR_CONSTRUCTION'] <= 2024, property_assessments['YEAR_CONSTRUCTION'], np.nan)

    # Rename USE_CATEGORY values
    map_of_assessments_values = {
        'Régulier': 'Regular',
    }
    property_assessments['USE_CATEGORY'] = _rename_incidents(
        property_assessments['USE_CATEGORY'], map_of_assessments_values)

    return property_assessments


def preprocess_census(census: pd.DataFrame) -> pd.DataFrame:

    # Convert to GeoDataFrames
    census_gdf = gpd.GeoDataFrame(census, geometry='geometry')

    # Get centroid for each geometry element
    data_projected = census_gdf.to_crs(epsg=32613)
    data_projected['centroid'] = data_projected['geometry'].centroid
    census_gdf['centroid'] = data_projected['centroid'].to_crs(epsg=4326)
    census_gdf['LONGITUDE'] = census_gdf['centroid'].x
    census_gdf['LATITUDE'] = census_gdf['centroid'].y

    # Convert to Dataframe
    census = census_gdf.drop(columns=['geometry', 'centroid'])

    # Rename columns
    new_census_column_names = {
        'Average size of census families': 'AVERAGE_FAMILY_SIZE',
        'Population density per square kilometre': 'POPULATION_DENSITY',
        'Population, 2021': '2021_POPULATION',
    }
    census = _rename_columns(census, new_census_column_names)

    outliers_to_drop = {
        'POPULATION_DENSITY': 0,
        '2021_POPULATION': 0
    }
    census = _remove_incident_outliers(census, outliers_to_drop)

    return census

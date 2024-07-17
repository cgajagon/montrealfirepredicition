import pandas as pd
import geopandas as gpd
import numpy as np
from datetime import datetime

from shapely.geometry import Point, Polygon

from shapely.ops import nearest_points


def _generate_flag(x: pd.Series, condition: list) -> pd.Series:

    y = np.where(x.isin(condition), True, False)

    return y


def _spatial_join_points(x_left: gpd.GeoDataFrame, x_right: gpd.GeoDataFrame, rsuffix: str) -> gpd.GeoDataFrame:

    y = gpd.sjoin(x_left, x_right, how='inner',
                  predicate='intersects', rsuffix=rsuffix)

    return y


def _remove_duplicates(x: gpd.GeoDataFrame, subset=list) -> gpd.GeoDataFrame:

    x.drop_duplicates(subset=subset, keep='first', inplace=True)

    return x


def _intersect_mesh(x: gpd.GeoDataFrame, mesh: gpd.GeoDataFrame, duplicate_keys: list) -> gpd.GeoDataFrame:

    table_gdf = _spatial_join_points(x, mesh, 'mesh')

    table_gdf = _remove_duplicates(table_gdf, duplicate_keys)

    return table_gdf


def _get_centroid_coordinates(x: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    x_projected = x.to_crs(epsg=32613)
    x_projected['centroid'] = x_projected['geometry'].centroid
    x['centroid'] = x_projected['centroid'].to_crs(epsg=4326)
    x['grid_long'] = x['centroid'].x
    x['grid_lat'] = x['centroid'].y

    return x


def _haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in kilometers
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)
    a = np.sin(delta_phi / 2.0) ** 2 + np.cos(phi1) * \
        np.cos(phi2) * np.sin(delta_lambda / 2.0) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c


def _calculate_building_age(x: pd.Series) -> pd.Series:
    current_year = datetime.now().year

    y = current_year - x

    return y


def create_square_mesh(firestation_areas: pd.DataFrame, square_size: float) -> pd.DataFrame:

    gdf = gpd.GeoDataFrame(firestation_areas, geometry='geometry')

    # Get the bounding box of the entire area
    minx, miny, maxx, maxy = gdf.total_bounds
    # Create a grid of points covering the bounding box
    x_coords = np.arange(minx, maxx, square_size)
    y_coords = np.arange(miny, maxy, square_size)
    grid_points = [Point(x, y) for x in x_coords for y in y_coords]
    # Create square polygons from the grid points
    squares = []
    for point in grid_points:
        x, y = point.x, point.y
        square = Polygon([(x, y), (x + square_size, y), (x +
                         square_size, y + square_size), (x, y + square_size)])
        squares.append(square)

    # Create a GeoDataFrame for the squares
    squares_gdf = gpd.GeoDataFrame({'geometry': squares}, crs=4326)

    # Clip the squares to the polygon boundaries
    clipped_squares_gdf = gpd.overlay(squares_gdf, gdf, how='intersection')

    # Calculate coordinates of the centroids for each grid elements
    clipped_squares_gdf = _get_centroid_coordinates(clipped_squares_gdf)

    # Drop the centroid
    clipped_squares_gdf = clipped_squares_gdf.drop(columns='centroid')

    return clipped_squares_gdf


def spatial_join_incidents(incidents: pd.DataFrame, mesh: pd.DataFrame) -> pd.DataFrame:

    # Convert to GeoDataFrames
    square_mesh_gdf = gpd.GeoDataFrame(mesh, geometry='geometry')
    incidents_gdf = gpd.GeoDataFrame(incidents, geometry=gpd.points_from_xy(
        incidents.LONGITUDE, incidents.LATITUDE), crs=4326)

    # Spatial join points with the mesh
    incidents_grid_gdf = _intersect_mesh(
        incidents_gdf, square_mesh_gdf, ['INCIDENT_ID'])

    # Convert to Dataframe
    incidents_grid = incidents_grid_gdf.drop(columns=['geometry'])

    return incidents_grid


def spatial_join_property_assessments(property_assessments: pd.DataFrame, mesh: pd.DataFrame) -> pd.DataFrame:

    # Convert to GeoDataFrames
    square_mesh_gdf = gpd.GeoDataFrame(mesh, geometry='geometry')
    property_assessments_gdf = gpd.GeoDataFrame(property_assessments, geometry=gpd.points_from_xy(
        property_assessments.LONGITUDE, property_assessments.LATITUDE), crs=4326)

    # Spatial join points with the mesh
    property_assessments_grid_gdf = _intersect_mesh(
        property_assessments_gdf, square_mesh_gdf, ['ASSESSMENT_ID'])

    # Convert to Dataframe
    property_assessments_grid = property_assessments_grid_gdf.drop(columns=[
                                                                   'geometry'])

    return property_assessments_grid


def spatial_join_census(census: pd.DataFrame, mesh: pd.DataFrame) -> gpd.GeoDataFrame:

    # Convert to GeoDataFrames
    square_mesh_gdf = gpd.GeoDataFrame(mesh, geometry='geometry')
    census_gdf = gpd.GeoDataFrame(census, geometry=gpd.points_from_xy(
        census.LONGITUDE, census.LATITUDE), crs=4326)

    # Spatial join points with the mesh
    census_grid_gdf = _intersect_mesh(census_gdf, square_mesh_gdf, ['DGUID'])

    # Convert to Dataframe
    census_grid = census_grid_gdf.drop(columns=['geometry'])

    return census_grid


def merge_incidents_property_assessments(incidents: pd.DataFrame, property_assessments: pd.DataFrame) -> pd.DataFrame:

    # Merge tables
    merged_incidents_property_assessments = pd.concat(
        [incidents, property_assessments], ignore_index=True, sort=False)

    return merged_incidents_property_assessments


def merge_incidents_property_assessments_census(incidents_property_assessments: pd.DataFrame, census: pd.DataFrame) -> pd.DataFrame:

    # Merge tables
    merged_incidents_property_assessments_census = pd.concat(
        [incidents_property_assessments, census], ignore_index=True, sort=False)

    return merged_incidents_property_assessments_census


def create_input_table(table, firestations):

    # Calculate building age
    table['building_age'] = np.where(table['ASSESSMENT_ID'].isna(
    ) == False, table['YEAR_CONSTRUCTION'].apply(_calculate_building_age), np.nan)

    # Calculate distance to firestation
    table = table.merge(firestations[["FIRE_STATION_ID", "LATITUDE", "LONGITUDE"]],
                        left_on='FIRE_STATION_ID', right_on='FIRE_STATION_ID', how='left', suffixes=("", "_firestation"))
    table['distance_to_fire_station'] = _haversine_distance(
        table['grid_lat'], table['grid_long'], table['LATITUDE_firestation'], table['LONGITUDE_firestation'])

    # Generate a flag for fire incidents
    table['is_fire'] = _generate_flag(table['INCIDENT_CATEGORY'], [
                                      'Autres incendies', 'Incendies de bâtiments'])

    # Drop latitude and longitude of each data
    table.drop(columns=["LATITUDE", "LONGITUDE"], inplace=True)

    return table

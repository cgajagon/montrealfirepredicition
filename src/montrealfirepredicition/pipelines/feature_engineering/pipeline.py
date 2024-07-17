from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    create_square_mesh,
    spatial_join_incidents,
    spatial_join_property_assessments,
    spatial_join_census,
    merge_incidents_property_assessments,
    merge_incidents_property_assessments_census,
    create_input_table
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([

        node(
            func=create_square_mesh,
            inputs=["preprocessed_firestation_areas", "params:square_size"],
            outputs="square_mesh",
            name="create_square_mesh_node",
        ),
        node(
            func=spatial_join_incidents,
            inputs=["preprocessed_incidents", "square_mesh"],
            outputs="spatial_joined_incidents",
            name="spatial_join_incidents_node",
        ),
        node(
            func=spatial_join_property_assessments,
            inputs=["preprocessed_property_assessments", "square_mesh"],
            outputs="spatial_joined_property_assessments",
            name="spatial_join_property_assessments_node",
        ),
        node(
            func=spatial_join_census,
            inputs=["preprocessed_census", "square_mesh"],
            outputs="spatial_joined_census",
            name="spatial_join_census_node",
        ),
        node(
            func=merge_incidents_property_assessments,
            inputs=["spatial_joined_incidents",
                    "spatial_joined_property_assessments"],
            outputs="merged_incidents_property_assessments",
            name="merge_incidents_property_assessments_node",
        ),
        node(
            func=merge_incidents_property_assessments_census,
            inputs=["merged_incidents_property_assessments",
                    "spatial_joined_census"],
            outputs="merged_incidents_property_assessments_census",
            name="merge_incidents_property_assessment_census_node",
        ),
        node(
            func=create_input_table,
            inputs=["merged_incidents_property_assessments_census",
                    "preprocessed_firestations"],
            outputs="input_table",
            name="create_input_table_node",
        ),
    ])

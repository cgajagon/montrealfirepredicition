from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    combine_incidents,
    preprocess_incidents,
    preprocess_firestations,
    preprocess_property_assessments,
    preprocess_firestation_areas,
    preprocess_census
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=combine_incidents,
                inputs="incidents",
                outputs="combined_incidents",
                name="combine_incidents_node",
            ),
            node(
                func=preprocess_incidents,
                inputs="combined_incidents",
                outputs="preprocessed_incidents",
                name="preprocess_incidents_node",
            ),
            node(
                func=preprocess_firestations,
                inputs="firestations",
                outputs="preprocessed_firestations",
                name="preprocess_firestations_node",
            ),
            node(
                func=preprocess_firestation_areas,
                inputs="firestation_areas",
                outputs="preprocessed_firestation_areas",
                name="preprocess_firestation_areas_node",
            ),
            node(
                func=preprocess_property_assessments,
                inputs="property_assessments",
                outputs="preprocessed_property_assessments",
                name="preprocess_property_assessments_node",
            ),

            node(
                func=preprocess_census,
                inputs="census",
                outputs="preprocessed_census",
                name="preprocess_census_node",
            ),
        ],
        namespace="ingestion"
    )

"""dlt pipeline to ingest NYC taxi trip data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def nyc_taxi_rest_api_source() -> dlt.sources.DltSource:
    """Define dlt resources for the NYC taxi REST API."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
            # Page-number pagination: 1-based pages, stop when an empty page is returned
            "paginator": {
                "type": "page_number",
                "base_page": 1,
                "page_param": "page",
                "stop_after_empty_page": True,
            },
        },
        "resources": [
            {
                "name": "nyc_taxi_trips",
                "endpoint": {
                    # Root path returns a list of trip records directly
                    "path": "",
                    "data_selector": "*",  # Select all items from root-level array
                },
            },
        ],
    }

    yield from rest_api_resources(config)


taxi_pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    dev_mode=True,
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = taxi_pipeline.run(nyc_taxi_rest_api_source())
    print(load_info)  # noqa: T201

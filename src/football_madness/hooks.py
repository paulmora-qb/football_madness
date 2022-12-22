"""Project hooks."""
from typing import Any, Dict, Iterable, List, Optional

import mlflow
import mlflow.spark
import pandas as pd
from kedro.config import ConfigLoader
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.pipeline.node import Node
from kedro.versioning import Journal


class ProjectHooks:
    @hook_impl
    def register_config_loader(
        self, conf_paths: Iterable[str], env: str, extra_params: Dict[str, Any],
    ) -> ConfigLoader:
        return ConfigLoader(conf_paths)

    @hook_impl
    def register_catalog(
        self,
        catalog: Optional[Dict[str, Dict[str, Any]]],
        credentials: Dict[str, Dict[str, Any]],
        load_versions: Dict[str, str],
        save_version: str,
        journal: Journal,
    ) -> DataCatalog:
        return DataCatalog.from_config(
            catalog, credentials, load_versions, save_version, journal
        )


def recursive_retrieving(dictionary: Dict[str, Any], keys: List[str]) -> Any:
    """This function is digging into the catalog provided by kedro, by going recursively
    looping through the delimiters of the parameters. For example if one states that one
    would like to save `splitting_params.train_size`, then the dictionary is first
    searched for `splitting_params` and the second term in place is searched for, namely
    `train_size`. In the end, after the desired value is found, the `keys` input is None
    and we return the dictionary, which at the end is the value we were looking for
    """
    if dictionary is None:
        return dictionary
    return (
        recursive_retrieving(dictionary.get(keys[0], None), keys[1:])
        if keys
        else dictionary
    )


class ModelTrackingHooks:
    """Namespace for grouping all model-tracking hooks with MLflow together."""

    @hook_impl
    def before_pipeline_run(
        self, catalog: DataCatalog, run_params: Dict[str, Any]
    ) -> None:
        """Hook implementation to start an MLflow run
        with the session_id of the Kedro pipeline run.
        """
        mlflow.start_run(run_name=run_params["run_id"])
        mlflow.log_params(run_params)

        from kedro.config import ConfigLoader

        conf_paths = ["conf/base", "conf/local"]
        conf_loader = ConfigLoader(conf_paths)

        config_mlflow = {}
        config_mlflow.update()

        # TODO: Write the possiblity of having multiple mlflow configurations
        # conf_catalog = conf_loader.get("catalog*", "catalog*/**").get("mlflow", {})
        conf_parameters = conf_loader.get("parameters*", "parameters*/**").get(
            "mlflow", {}
        )

        # Saving what we would like to log on the way
        self.mlflow_savings = {
            "metrics": conf_parameters.get("metrics", {}),
            "tags": conf_parameters.get("tags", {}),
            "artifcats": conf_parameters.get("artifacts", {}),
        }

        # Logging already what we can at that point
        all_parameters_dictionary = catalog.load("parameters")
        for param in conf_parameters.get("parameters", {}):
            mlflow.log_param(
                param, recursive_retrieving(all_parameters_dictionary, param.split("."))
            )

    @hook_impl
    def after_node_run(
        self, node: Node, outputs: Dict[str, Any], inputs: Dict[str, Any]
    ) -> None:
        pass

        # input_output_dict = pd.json_normalize(
        #     {key: val for key, val in {**inputs, **outputs}.items()}, sep=".",
        # ).to_dict(orient="records")[0]

        # self.mlflow_savings["metrics"]

        # mlflow.log_params(input_output_dict, self.mlflow_savings["parameters"])

    @hook_impl
    def after_pipeline_run(self) -> None:
        """Hook implementation to end the MLflow run
        after the Kedro pipeline finishes.
        """
        mlflow.end_run()

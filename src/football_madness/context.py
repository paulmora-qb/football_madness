"""Context for including a spark session"""

from pathlib import Path
from typing import Any, Dict, Union

from kedro.framework.context import KedroContext
from pyspark import SparkConf
from pyspark.sql import SparkSession


class SparkContext(KedroContext):
    """Configures a Sparkession in the project"""

    def __init__(
        self,
        package_name: str,
        project_path: Union[Path, str],
        env: str = None,
        extra_params: Dict[str, Any] = None,
    ):
        super().__init__(package_name, project_path, env, extra_params)
        self.init_spark_session()

    def init_spark_session(self) -> None:
        """Creates a sparksession using the .yml file regarding the spark settings"""

        parameters = self.config_loader.get("spark*", "spark*/**")
        spark_conf = SparkConf().setAll(parameters.items())

        spark_session_conf = (
            SparkSession.builder.appName(self.package_name)
            .enableHiveSupport()
            .config(conf=spark_conf)
        )
        _spark_session = spark_session_conf.getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")

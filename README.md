# Football Madness - Forecasting the results of football games

## Overview

This project builds an end-to-end pipeline of football game result predictions. This
is done using the data from [website](link.com). Within the project this data is used
to generate relevant features, applies generic data science steps (imputation, encoding,
hyperparameter-tuning) and creates predictions for a desired season, league and/or team.

The project is Python based, using kedro for the pipelining and pyspark for functions.
One aim of this project was to conduct an end-to-end data-science project using pyspark.

## How to run this project

### How to install dependencies

Declare any dependencies in `src/requirements.txt` for `pip` installation and `src/environment.yml` for `conda` installation.

To install them, run:

```
kedro install
```

### How to run the Kedro pipeline

You can run your Kedro project with:

```
kedro run
```

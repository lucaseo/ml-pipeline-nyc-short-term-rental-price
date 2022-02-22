#!/usr/bin/env python
"""
Performs basic cleaning on the data and save the results in Weights & Biases
"""
import os
import argparse
import logging
import tempfile

import wandb

import pandas as pd
import wandb

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

def clean_data(df: pd.DataFrame, min_price: float, max_price: float) -> pd.DataFrame:

    # filter data: price
    df = df[(df['price'] >= min_price) & (df['price'] <= max_price)].copy() # you can also use pandas between

    # filter data: longitude, latitude
    df = df[(df['longitude'] >= -74.25) & (df['longitude'] <= -73.50)].copy()
    df = df[(df['latitude'] >= 40 / 5) & (df['longitude'] <= 41.2)].copy()

    # convert data
    df['last_review'] = pd.to_datetime(df['last_review'])

    return df



def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()


    # Download and read input artifact from W&B
    logger.info("Downloading dataset from W&B")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)

    # Clean dataset
    df_clean = clean_data(df, args.min_price, args.max_price)

    with tempfile.TemporaryDirectory() as temp_dir:
        logger.info("Saving cleaned dataset in W&B")

        # save dataset file
        df_clean.to_csv(os.path.join(temp_dir, "clean_sample.csv"), index=False)

        # upload dataset file to W&B
        artifact = wandb.Artifact(args.output_artifact, type=args.output_type, description=args.output_description)
        artifact.add_file(os.path.join(temp_dir, "clean_sample.csv"))
        run.log_artifact(artifact)


    ######################
    # YOUR CODE HERE     #
    ######################


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="input artifact name",
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="output artifact name",
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="type of the artifact",
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="description for the artifact",
        required=True
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="lower-bound (minimum) value for price",
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="upper-bound (maximum) value for price",
        required=True
    )

    args = parser.parse_args()

    go(args)
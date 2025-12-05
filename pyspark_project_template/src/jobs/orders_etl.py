import sys
import yaml
from utils.spark import get_spark
from utils.logger import get_logger

log = get_logger("orders_etl")

def load_config(env):
    with open(f"config/{env}.yaml") as f:
        return yaml.safe_load(f)

def run(env):
    config = load_config(env)
    spark = get_spark("OrdersETL")

    log.info(f"Starting ETL for environment: {env}")

    df = spark.read.csv(config["input_path"], header=True, inferSchema=True)

    log.info("Applying transformations...")
    df_clean = df.dropna()

    df_clean.write.mode("overwrite").parquet(config["output_path"])
    log.info("ETL job completed successfully!")

if __name__ == "__main__":
    env = sys.argv[1]
    run(env)

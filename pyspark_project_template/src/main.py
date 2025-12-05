import sys
from jobs.orders_etl import run

if __name__ == "__main__":
    env = sys.argv[1]  # dev/qa/prod
    run(env)

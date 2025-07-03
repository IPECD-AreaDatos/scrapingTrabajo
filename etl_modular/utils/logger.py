import logging
import os

def setup_logger(name="etl"):
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        filename=f"logs/{name}.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

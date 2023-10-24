import asyncio
import csv

import structlog
from kelvin.app.client import KelvinApp
from kelvin.message import Number
from kelvin.message.krn import KRNAssetDataStream

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M.%S"),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ]
)

logger = structlog.getLogger()

mapping = {
    1: {"csv_value": "water_pressure", "data_stream": "water-pressure"},
    2: {"csv_value": "casing_pressure", "data_stream": "casing-pressure"},
    3: {"csv_value": "torque", "data_stream": "torque_new"},
    4: {"csv_value": "water_flow", "data_stream": "water-flow"},
    5: {"csv_value": "gas_volume_flow", "data_stream": "gas-volume-flow"},
    6: {"csv_value": "pump_speed", "data_stream": "pump-speed"},
    7: {"csv_value": "downhole_pressure", "data_stream": "downhole-pressure"},
    8: {"csv_value": "water_level", "data_stream": "water-level"},
    9: {"csv_value": "torque_op", "data_stream": "torque-op"},
    10: {"csv_value": "gas_line_pressure", "data_stream": "gas-line-pressure"},
}


async def read_and_upload(kelvin_app: KelvinApp, csv_file: str, well_name: str):
    while True:
        try:
            with open(csv_file) as file:
                dict_reader = csv.DictReader(file)
                for item in dict_reader:
                    logger.info("Publishing messages")
                    for mapping_item in mapping.values():
                        if item[mapping_item["csv_value"]] is None or len(item[mapping_item["csv_value"]]) == 0:
                            continue

                        await kelvin_app.publish_message(
                            Number(
                                resource=KRNAssetDataStream(well_name, mapping_item["data_stream"]),
                                payload=float(item[mapping_item["csv_value"]]),
                            )  # type: ignore
                        )
                    logger.info("Going to sleep")
                    await asyncio.sleep(30)
        except Exception as exception:
            logger.info("Failed to Read and add data. Starting again", error=exception)


async def main() -> None:
    kelvin_app = KelvinApp()
    await kelvin_app.connect()

    tasks = [
        read_and_upload(kelvin_app=kelvin_app, csv_file="sources/well03101_refined.csv", well_name="well03101"),
        read_and_upload(kelvin_app=kelvin_app, csv_file="sources/well05601_refined.csv", well_name="well05601"),
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())

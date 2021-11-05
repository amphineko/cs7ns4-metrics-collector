#!/usr/bin/env python

import argparse
import itertools
import time

import redfish as Redfish
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


def read_chassis(redfish):
    chassis = redfish.get('/redfish/v1/Chassis').dict['Members']
    chassis = [chassis['@odata.id'] for chassis in chassis]
    return chassis


def read_chassis_tempertures(chassis, redfish):
    thermals = [redfish.get(id + '/Thermal').dict for id in chassis]
    thermals = [thermal['Temperatures'] for thermal in thermals]
    thermals = list(itertools.chain.from_iterable(thermals))
    return thermals


def write_chassis_tempertures(thermals, tag, bucket, influx):
    with influx.write_api(write_options=SYNCHRONOUS) as writer:
        points = [
            Point.measurement('location_temperature').tag(
                'location',
                tag,
            ).field(
                'temperature',
                float(temp['ReadingCelsius']),
            ) for temp in thermals
        ]
        writer.write(bucket=bucket, record=points)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Redfish Collector")

    parser.add_argument('-H', '--redfish-host', required=True)
    parser.add_argument('-u', '--redfish-user', required=True)
    parser.add_argument('-p', '--redfish-password', required=True)

    parser.add_argument('-b', '--influx-bucket', required=True)
    parser.add_argument('-l', '--influx-location-tag', required=True)
    parser.add_argument('-o', '--influx-org', required=True)
    parser.add_argument('-s', '--influx-url', required=True)
    parser.add_argument('-t', '--influx-token', required=True)

    parser.add_argument('-i', '--collector-interval', required=True, type=int)

    args = parser.parse_args()

    redfish = Redfish.redfish_client(base_url=args.redfish_host,
                                     username=args.redfish_user,
                                     password=args.redfish_password)
    redfish.login(auth='session')

    influx = InfluxDBClient(args.influx_url,
                            args.influx_token,
                            org=args.influx_org)
    # writer = influx.write_api(write_options=SYNCHRONOUS)

    try:
        while True:
            chassis = read_chassis(redfish)
            chassis_temperatures = read_chassis_tempertures(chassis, redfish)
            system_temperatures = [
                temp for temp in chassis_temperatures
                if temp['Name'] == 'System Temp'
            ]

            write_chassis_tempertures(
                system_temperatures,
                args.influx_location_tag,
                args.influx_bucket,
                influx,
            )

            print('[INFO] written %d points to influxdb' %
                  (len(system_temperatures)))

            time.sleep(int(args.collector_interval))
    finally:
        redfish.logout()

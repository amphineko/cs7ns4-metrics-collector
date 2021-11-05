#!/usr/bin/env python

import argparse
import io
import time

import requests
from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write.point import Point
from influxdb_client.client.write_api import SYNCHRONOUS
from lxml import etree


def get_station_tempertures(station_string, hours_before_now):
    params = {
        'dataSource': 'metars',
        'requestType': 'retrieve',
        'format': 'xml',
        'stationString': station_string,
        'hoursBeforeNow': hours_before_now,
    }
    r = requests.get(
        'https://aviationweather.gov/adds/dataserver_current/httpparam',
        params=params,
    )

    r = io.BytesIO(r.content)
    root = etree.parse(r)

    return root


def import_station_tempertures(r, bucket, influx):
    with influx.write_api(write_options=SYNCHRONOUS) as writer:
        points = [
            Point.measurement('location_temperature').time(
                metar.xpath('observation_time')[0].text).tag(
                    'location',
                    metar.xpath('station_id')[0].text,
                ).field(
                    'temperature',
                    float(metar.xpath('temp_c')[0].text),
                ) for metar in r
        ]
        writer.write(bucket=bucket, record=points)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Redfish Collector")

    parser.add_argument('-i', '--station-string', required=True)
    parser.add_argument('-H', '--hours-before-now', required=True, type=int)
    parser.add_argument('-I', '--fetch-interval', required=False, type=int)

    parser.add_argument('-b', '--influx-bucket', required=True)
    parser.add_argument('-l', '--influx-location-tag', required=True)
    parser.add_argument('-o', '--influx-org', required=True)
    parser.add_argument('-s', '--influx-url', required=True)
    parser.add_argument('-t', '--influx-token', required=True)

    args = parser.parse_args()

    influx = InfluxDBClient(args.influx_url,
                            args.influx_token,
                            org=args.influx_org)

    try:
        interval = int(args.fetch_interval)
    except ValueError:
        interval = None

    while True:
        r = get_station_tempertures(args.station_string, args.hours_before_now)
        r = r.xpath('/response/data/METAR')
        import_station_tempertures(r, args.influx_bucket, influx)

        print('[INFO] imported %d METARs' % (len(r)))

        if interval is None:
            break

        time.sleep(interval)

#!/usr/bin/env python3
"""
Python ADBC example for SwanLake Flight SQL server.
"""

import sys

from adbc_driver_flightsql.dbapi import connect


def main():
    # Connect to SwanLake Flight SQL server
    endpoint = "grpc://localhost:4214"
    print(f"Connecting to SwanLake at {endpoint}...")

    with connect(endpoint) as conn:
        print("Connected to SwanLake successfully!")
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1, 2.0, 'Hello, world!'")
            print(cursor.fetch_arrow_table())


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)

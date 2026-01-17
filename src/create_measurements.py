#!/ usr / bin / env python3
"""
Generate measurements.txt for the 1 Billion Row Challenge.

Each line has the format: <station_name>;<temperature>
Temperature is a float with exactly one decimal place.
"""

import csv
import os
import random
import sys
import time
from pathlib import Path

#Default number of rows
DEFAULT_NUM_ROWS = 1_000_000_000

#Path configuration
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR / "data"
DATASETS_DIR = PROJECT_DIR / "datasets"
STATIONS_FILE = DATA_DIR / "weather_stations.csv"
OUTPUT_FILE = DATASETS_DIR / "measurements.txt"


def load_stations(filepath: Path) -> list[tuple[str, float]]:
    """Load weather stations from CSV file."""
    stations = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row["station_name"]
            mean_temp = float(row["mean_temp"])
            stations.append((name, mean_temp))
    return stations


def generate_measurement(station: tuple[str, float]) -> str:
    """Generate a single measurement line."""
    name, mean_temp = station
#Generate temperature with normal distribution around mean
#Standard deviation of 10 degrees for realistic variation
    temp = random.gauss(mean_temp, 10.0)
#Clamp to valid range
    temp = max(-99.9, min(99.9, temp))
    return f"{name};{temp:.1f}\n"


def create_measurements(num_rows: int, output_path: Path) -> None:
    """Generate the measurements file."""
    print(f"Loading weather stations from {STATIONS_FILE}...")
    stations = load_stations(STATIONS_FILE)
    print(f"Loaded {len(stations)} weather stations")

    print(f"Generating {num_rows:,} measurements to {output_path}...")
    print("This may take a while for large datasets...")

#Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    start_time = time.time()
    batch_size = 10_000_000  # Write in batches for better performance

    with open(output_path, "w", encoding="utf-8") as f:
        rows_written = 0
        while rows_written < num_rows:
#Generate a batch
            batch_count = min(batch_size, num_rows - rows_written)
            batch = [generate_measurement(random.choice(stations)) for _ in range(batch_count)]
            f.writelines(batch)
            rows_written += batch_count

#Progress update
            elapsed = time.time() - start_time
            progress = rows_written / num_rows * 100
            rate = rows_written / elapsed if elapsed > 0 else 0
            eta = (num_rows - rows_written) / rate if rate > 0 else 0
            print(
                f"\rProgress: {progress:.1f}% ({rows_written:,}/{num_rows:,}) "
                f"- {rate:,.0f} rows/sec - ETA: {eta:.0f}s",
                end="",
                flush=True,
            )

    elapsed = time.time() - start_time
    file_size = output_path.stat().st_size / (1024**3)  # Size in GB

    print(f"\n\nCompleted in {elapsed:.1f} seconds")
    print(f"File size: {file_size:.2f} GB")
    print(f"Output: {output_path}")


def main():
    num_rows = DEFAULT_NUM_ROWS

    if len(sys.argv) > 1:
        try:
            num_rows
= int(sys.argv[1]) except ValueError
    : print(f "Usage: {sys.argv[0]} [num_rows]")
          print(f "  num_rows: Number of measurements to generate (default: "
                  "{DEFAULT_NUM_ROWS:,})") sys.exit(1)

              create_measurements(num_rows, OUTPUT_FILE)

                  if __name__ == "__main__" : main()

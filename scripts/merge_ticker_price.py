#!/usr/bin/env python3
import os
import json
import argparse
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("logs/merge_ticker_price.log"), logging.StreamHandler()]
)

def merge_price_files(artifacts_dir):
    output_file = os.path.join("data", "ticker_price.json")
    merged_data = {}

    # Find all ticker_price_part_*.json files in the artifacts directory
    for filename in os.listdir(artifacts_dir):
        if filename.startswith("ticker_price_part_") and filename.endswith(".json"):
            file_path = os.path.join(artifacts_dir, filename)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    part_data = json.load(f)
                    # Merge data, updating with the latest values for any duplicate keys
                    merged_data.update(part_data)
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse {filename}: {e}")
            except Exception as e:
                logging.error(f"Error reading {filename}: {e}")

    # Write the merged data to the output file
    os.makedirs("data", exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(merged_data, f, indent=2)
    logging.info(f"Merged data saved to {output_file} with {len(merged_data)} entries")

def main(artifacts_dir):
    start_time = datetime.now().strftime("%I:%M %p EDT on %A, %B %d, %Y")
    logging.info(f"Starting price merge process at {start_time}")

    if not os.path.exists(artifacts_dir):
        logging.error(f"Artifacts directory {artifacts_dir} not found")
        return

    merge_price_files(artifacts_dir)

    elapsed_time = datetime.now() - datetime.strptime(start_time, "%I:%M %p EDT on %A, %B %d, %Y")
    logging.info(f"Price merge completed. Elapsed time: {elapsed_time.total_seconds():.1f}s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge ticker price partition files into a single JSON file.")
    parser.add_argument("artifacts_dir", help="Directory containing ticker price partition files")
    args = parser.parse_args()

    main(args.artifacts_dir)

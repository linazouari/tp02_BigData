
import csv
import os
from pathlib import Path



OUTPUT_FILE = Path("big_data.csv")
TARGET_GB = 5.0
TARGET_BYTES = int(TARGET_GB * 1024**3)
BATCH_SIZE = 10_000

headers = ["id", "value", "quantity", "category", "description"]

categories = ["A", "B", "C", "D"]
description = "Big data processing experiment " * 3

def main():
    if OUTPUT_FILE.exists():
        print(f"File already exists: {OUTPUT_FILE.resolve()}")
        print("Rename or remove it before generating a new dataset.")
        return

    written_rows = 0

    try:
        with OUTPUT_FILE.open(
            "w", newline="", encoding="utf-8"
        ) as file:
            writer = csv.writer(file)
            writer.writerow(headers)

            while file.tell() < TARGET_BYTES:
                batch = []

                for _ in range(BATCH_SIZE):
                    row_id = written_rows + 1
                    value = (row_id * 7) % 100_000
                    quantity = (row_id % 100) + 1
                    category = categories[row_id % len(categories)]

                    batch.append([
                        row_id,
                        value,
                        quantity,
                        category,
                        f"{description} record={row_id}"
                    ])
                    written_rows += 1

                writer.writerows(batch)
                file.flush()

                size_gb = os.path.getsize(OUTPUT_FILE) / 1024**3
                print(
                    f"Rows: {written_rows:,} | "
                    f"Size: {size_gb:.2f} GiB"
                )

        print("\nDataset generation completed.")
        print(f"Path: {OUTPUT_FILE.resolve()}")
        print(f"Rows: {written_rows:,}")
        print(
            f"Final size: "
            f"{os.path.getsize(OUTPUT_FILE) / 1024**3:.2f} GiB"
        )

    except KeyboardInterrupt:
        print("\nGeneration interrupted by user.")
        print(f"Partial file remains at: {OUTPUT_FILE.resolve()}")
    except OSError as error:
        print(f"\nFile generation failed: {error}")
        print("Check the available disk space.")
        print(f"Partial file, if any: {OUTPUT_FILE.resolve()}")

if __name__ == "__main__":
    main()

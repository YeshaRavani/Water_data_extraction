"""
Clean up Location names: strip quotes and extra whitespace.
"""

import csv
import os

BASE_DIR = "schema_aligned_ground_truths"

def main():
    print("Cleaning up Location names (whitespace and quotes)...")
    for folder in os.listdir(BASE_DIR):
        folder_path = os.path.join(BASE_DIR, folder)
        if not os.path.isdir(folder_path):
            continue
            
        csv_path = os.path.join(folder_path, "schema_aligned_ground_truth.csv")
        if os.path.exists(csv_path):
            with open(csv_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                header = reader.fieldnames
                rows = list(reader)

            for row in rows:
                if row.get("Location"):
                    # Strip quotes and whitespace
                    row["Location"] = row["Location"].strip().strip('"').strip("'").strip()

            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=header)
                writer.writeheader()
                writer.writerows(rows)
    print("✅ Done.")

if __name__ == "__main__":
    main()

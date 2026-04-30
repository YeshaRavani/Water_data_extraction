"""
Replace location shortforms with full names in all ground truth CSVs.
"""

import csv
import os
import shutil

BASE_DIR = "schema_aligned_ground_truths"

# Mapping dictionary for each paper (folder name -> {shortform: full_name})
MAPPINGS = {
    "alsubih_khan_2024_yamuna_pharmaceuticals": {
        "SP1": "Wazirabad barrage",
        "SP2": "Wazirabad Khyber Pass drain",
        "SP3": "Delhi Gate drain",
        "SP4": "Maharani Bagh drain",
        "SP5": "Okhla Barrage",
        "S1": "Wazirabad barrage",
        "S2": "Wazirabad Khyber Pass drain",
        "S3": "Delhi Gate drain",
        "S4": "Maharani Bagh drain",
        "S5": "Okhla Barrage",
    },
    "antil_et_al_2025_yamuna_water_quality": {
        "PS": "Paonta Sahib",
        "YM": "Yamunotri",
        "WBBD": "Wazirabad Before Drain",
        "WBAD": "Wazirabad After Drain",
        "PJ": "Prayagraj",
        "HP": "Himachal Pradesh",
        "UK": "Uttarakhand",
        "UP": "Uttar Pradesh",
    },
    "biswas_and_vellanki_2021_yamuna_emerging_contaminants": {
        "Y-1": "Dakpatthar",
        "Y-2": "Hindon river",
        "Y-3": "Hindon canal",
        "Y-4": "Nizamuddin Bridge",
        "Y-5": "Okhla Barrage",
        "Y-6": "Downstream of Shahadara drain",
        "Y-7": "Upstream of Mathura",
        "Y-8": "Downstream of Mathura",
        "Y-9": "Upstream of Agra city",
        "Y-10": "Agra city (Taj Mahal)",
        "Y-11": "Downstream of Agra city",
        "Y-12": "Upstream of Etawah",
        "Y-13": "Downstream of Etawah",
        "Dakpatthar (Y-1)": "Dakpatthar",
        "Hindon river (Y-2)": "Hindon river",
        "Hindon canal (Y-3)": "Hindon canal",
        "Nizamuddin bridge (Y-4)": "Nizamuddin Bridge",
        "Okhla barrage (Y-5)": "Okhla Barrage",
        "down-stream of Yamuna-Shahadara drain confluence (Y-6)": "Downstream of Shahadara drain",
    },
    "mandal_et_al_2010_yamuna_water_quality": {
        "ACMS": "Agra Canal Midstream",
        "ACQS": "Agra Canal Quarter Stream",
        "NMS": "Nizamuddin Mid Stream",
        "NQS": "Nizamuddin Quarter Stream",
    },
    "parween_et_al_2017_yamuna_pollution": {
        "L1": "Jagatpur (Rural)",
        "L2": "Jagatpur (Rural)",
        "L3": "Narela (Urban)",
        "L4": "Narela (Urban)",
        "L5": "Najafgarh Drain (Industrial)",
        "L6": "Najafgarh Drain (Industrial)",
        "L7": "Rajghat (Agricultural)",
        "L8": "Rajghat (Agricultural)",
        "L9": "Rajghat (Agricultural)",
        "L10": "Indraprastha (Downstream Drains)",
        "L11": "Okhla Sanctuary",
        "L12": "Okhla Sanctuary",
        "L13": "Nizamuddin (High Traffic)",
        "L14": "Shahdara Drain (Downstream)",
    },
    "SAYANTAN SAMUI": {
        f"S{i}": f"S{i} (Barapullah Basin)" for i in range(1, 44)
    },
    "sharma_et_al_2024_yamuna_water_quality": {
        "HP": "Himachal Pradesh",
        "UK": "Uttarakhand",
        "UP": "Uttar Pradesh",
        "MP": "Madhya Pradesh",
    },
    "vaid etal": {
        **{f"N{i}": f"Najafgarh Drain (N{i})" for i in range(1, 16)},
        "S1": "Badshahpur & Basai Drain",
        "S2": "Goyla Dairy Outlet",
        "S3": "Palam Drain",
        "S4": "Mungeshpur Drain",
        "S5": "Keshopur STP Outlet",
        "S6": "Basaidarapur Drain",
        "S7": "Kanhaiya Nagar Drain",
        "S8": "Supplementary Drain",
        "Y1": "Yamuna River (Upstream Najafgarh)",
        "Y2": "Yamuna River (Downstream Najafgarh)",
    }
}

def clean_location(loc: str, mapping: dict) -> str:
    """Replace shortform in location string with full name."""
    if not loc:
        return loc
    
    # Try exact match first
    loc_clean = loc.strip().strip('"')
    if loc_clean in mapping:
        return mapping[loc_clean]
    
    # Try partial replacement (e.g. "PJ" in "Prayagraj (PJ)")
    # Sort keys by length descending to avoid partial matches on shorter codes
    for shortform in sorted(mapping.keys(), key=len, reverse=True):
        if shortform in loc:
            return mapping[shortform]
            
    return loc

def process_file(csv_path: str, folder_name: str):
    if folder_name not in MAPPINGS:
        return

    mapping = MAPPINGS[folder_name]
    
    with open(csv_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        rows = list(reader)

    updated_count = 0
    for row in rows:
        old_loc = row["Location"]
        new_loc = clean_location(old_loc, mapping)
        if old_loc != new_loc:
            row["Location"] = new_loc
            updated_count += 1

    if updated_count > 0:
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            writer.writerows(rows)
        print(f"  ✔ {folder_name}: Updated {updated_count} locations.")
    else:
        print(f"  - {folder_name}: No replacements needed.")

def main():
    print("="*60)
    print("Replacing Location Shortforms with Full Names")
    print("="*60)
    
    for folder in os.listdir(BASE_DIR):
        folder_path = os.path.join(BASE_DIR, folder)
        if not os.path.isdir(folder_path):
            continue
            
        csv_path = os.path.join(folder_path, "schema_aligned_ground_truth.csv")
        if os.path.exists(csv_path):
            process_file(csv_path, folder)

    print("\n✅ Location names updated.")

if __name__ == "__main__":
    main()

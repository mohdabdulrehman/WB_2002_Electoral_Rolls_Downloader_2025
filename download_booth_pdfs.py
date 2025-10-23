import pandas as pd
import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

EXCEL_FILE = "all_booths_urls.xlsx"
OUTPUT_FOLDER = "pdfs"
MAX_WORKERS = 5
MAX_RETRIES = 3

def download_booth(row):
    district = row['District']
    ac_no = row['AC No']
    ac_name = row['AC Name']
    booth_no = row['Booth No']
    url = row['URL']

    folder_path = os.path.join(OUTPUT_FOLDER, f"{ac_no} - {ac_name}")
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, f"{booth_no}.pdf")

    if os.path.exists(file_path):
        return f"Skipped {booth_no}.pdf (already exists)"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            start_time = time.time()
            res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
            if res.status_code == 200:
                with open(file_path, "wb") as f:
                    f.write(res.content)
                elapsed = time.time() - start_time
                return f"Downloaded {booth_no}.pdf ({elapsed:.2f}s)"
            else:
                print(f"Retry {attempt}: Status {res.status_code} for {booth_no}.pdf")
        except Exception as e:
            print(f"Retry {attempt}: Error {booth_no}.pdf -> {e}")
        time.sleep(1)
    return f"Failed {booth_no}.pdf after {MAX_RETRIES} retries"

def main():
    df = pd.read_excel(EXCEL_FILE)
    assemblies = df.groupby(['AC No', 'AC Name'])

    total_assemblies = len(assemblies)
    print(f"Total assemblies: {total_assemblies}\n")

    for i, ((ac_no, ac_name), group) in enumerate(assemblies, 1):
        print(f"=== Assembly {i}/{total_assemblies}: {ac_no} - {ac_name} ({len(group)} booths) ===")
        assembly_start = time.time()
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(download_booth, row) for idx, row in group.iterrows()]
            for future in as_completed(futures):
                print(future.result())
        assembly_elapsed = time.time() - assembly_start
        print(f"Assembly {ac_no} completed in {assembly_elapsed:.2f}s\n")

if __name__ == "__main__":
    main()

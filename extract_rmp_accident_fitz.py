import fitz  # PyMuPDF
import re
import pandas as pd
import os
from pathlib import Path
import time

# Define paths
pdf_dir = r"C:\MS Data Science - WMU\EDGI\epa-risk-management-plans\reports"
output_csv = r"C:\MS Data Science - WMU\EDGI\rmp\rmp_accident_history.csv"

# Start timing
start_time = time.time()

# Initialize list to store results
results = []

# Function to extract EPA Facility ID from filename
def extract_epa_facility_id_from_filename(filename):
    match = re.search(r"(\d+)\.pdf$", filename)
    if not match:
        print(f"Could not extract EPA Facility ID from filename: {filename}")
    return match.group(1) if match else None

# Function to parse Section 6: Accident History
def parse_accident_history(page_text):
    print("Parsing Section 6. Sample text:", page_text[:200])
    # Check for "No records found"
    if re.search(r"No records found|No accidents reported", page_text, re.IGNORECASE):
        print("No accidents found")
        return {"Has Accident": "No", "Accident Date": None, "Chemical": None, "Injuries": None}
    
    # If accidents exist, parse details
    section_6_match = re.search(r"Section 6\. Accident History\s*([\s\S]*?)(?=Section 7|$)", page_text, re.IGNORECASE)
    if not section_6_match:
        print("Section 6 not found in sample text")
        return {"Has Accident": "No", "Accident Date": None, "Chemical": None, "Injuries": None}
    
    section_6_text = section_6_match.group(1)
    print("Section 6 text sample:", section_6_text[:200])
    accident_date = re.search(r"Date:\s*(\d{1,2}/\d{1,2}/\d{4})", section_6_text, re.IGNORECASE)
    chemical = re.search(r"Chemical:\s*([A-Za-z\s]+)", section_6_text, re.IGNORECASE)
    injuries = re.search(r"Injuries:\s*(\d+)", section_6_text, re.IGNORECASE)

    result = {
        "Has Accident": "Yes",
        "Accident Date": accident_date.group(1) if accident_date else None,
        "Chemical": chemical.group(1).strip() if chemical else None,
        "Injuries": int(injuries.group(1)) if injuries else 0
    }
    print("Parsed accident data:", result)
    return result

# Process PDFs in batches to manage memory
batch_size = 1000
processed_files = 0

# Iterate through PDF files in subfolders
for pdf_file in Path(pdf_dir).rglob("*.pdf"):  # Recursively search subfolders
    try:
        with fitz.open(pdf_file) as doc:
            # Extract EPA Facility ID from filename
            epa_id = extract_epa_facility_id_from_filename(pdf_file.name)
            if not epa_id:
                print(f"Skipping {pdf_file} due to invalid EPA Facility ID")
                continue
            
            # Extract Section 6 (search all pages)
            section_6_text = None
            for i in range(len(doc)):
                page_text = doc[i].get_text("text")
                if not page_text:
                    print(f"No text extracted from page {i+1} of {pdf_file.name}")
                    continue
                if re.search(r"Section 6\. Accident History", page_text, re.IGNORECASE):
                    section_6_text = page_text
                    print(f"Found Section 6 on page {i+1} of {pdf_file.name}")
                    break
            if not section_6_text:
                print(f"Section 6 not found in {pdf_file.name}")
                continue
            
            accident_data = parse_accident_history(section_6_text)
            accident_data["EPA Facility ID"] = epa_id
            results.append(accident_data)
            print(f"Processed {pdf_file.name}: {accident_data}")
        
        processed_files += 1
        if processed_files % batch_size == 0:
            print(f"Processed {processed_files} files...")
            pd.DataFrame(results).to_csv(output_csv, mode='a', header=not os.path.exists(output_csv), index=False)
            results = []

    except Exception as e:
        print(f"Error processing {pdf_file.name}: {e}")

# Save remaining results
if results:
    pd.DataFrame(results).to_csv(output_csv, index=False)

# End timing
end_time = time.time()
print(f"Total processing time: {end_time - start_time:.2f} seconds")
print(f"Total processed: {processed_files}")
print(f"Facilities with accidents: {len([r for r in results if r['Has Accident'] == 'Yes'])}")
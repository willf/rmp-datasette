import fitz  # PyMuPDF
import re
import pandas as pd
import os
from pathlib import Path
import time

# Define paths
pdf_dir = r"C:\MS Data Science - WMU\EDGI\epa-risk-management-plans\reports"
output_csv = r"C:\MS Data Science - WMU\EDGI\rmp\rmp_accident_history.csv"
error_log = r"C:\MS Data Science - WMU\EDGI\rmp\error_log.txt"

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

# Function to extract NAICS Code before Section 6
def extract_process_naics(text_before_section_6):
    naics_code = re.search(r"NAICS Code:\s*(\d+)", text_before_section_6, re.IGNORECASE)
    return naics_code.group(1) if naics_code else None

# Function to parse all Accident History blocks in Section 6
def parse_accident_history(section_6_text):
    accidents = []
    if re.search(r"No records found|No accidents reported", section_6_text, re.IGNORECASE):
        print("No accidents found")
        return [{"Has Accident": "No", "Accident Count": 0, "Accident Details": None}]
    
    accident_blocks = re.finditer(r"Accident History ID: Accident \d+([\s\S]*?)(?=Accident History ID: Accident \d+|\Z)", section_6_text, re.IGNORECASE)
    accident_count = 0
    
    for block in accident_blocks:
        block_text = block.group(0)
        accident_count += 1
        date_match = re.search(r"Date of Accident:\s*([A-Za-z]+\s*\d{4})", block_text, re.IGNORECASE)
        naics_match = re.search(r"NAICS Code of Process Involved:\s*(\d+)", block_text, re.IGNORECASE)
        
        accident = {
            "Date of Accident": date_match.group(1) if date_match else None,
            "NAICS Code of Process Involved": naics_match.group(1) if naics_match else None
        }
        accidents.append(accident)
        print(f"Parsed accident {accident_count}: {accident}")
    
    return [{"Has Accident": "Yes" if accident_count > 0 else "No", "Accident Count": accident_count, "Accident Details": accidents}]

# Build set of valid EPA Facility IDs from existing PDF filenames
valid_facility_ids = set()
pdf_files = list(Path(pdf_dir).rglob("*.pdf"))
for pdf_file in pdf_files:
    epa_id = extract_epa_facility_id_from_filename(pdf_file.name)
    if epa_id:
        valid_facility_ids.add(epa_id)

print(f"Found {len(pdf_files)} PDF files across all subfolders")
print(f"Found {len(valid_facility_ids)} unique facility IDs from PDFs")

# Process all PDF files
batch_size = 1000
processed_files = 0
total_files = len(pdf_files)

print(f"Total PDF files to process: {total_files}")
with open(error_log, "a") as log_file:
    log_file.write(f"Processing started at {time.ctime(start_time)}\n")
    for pdf_file in pdf_files:
        try:
            with fitz.open(pdf_file) as doc:
                epa_id = extract_epa_facility_id_from_filename(pdf_file.name)
                if not epa_id or epa_id not in valid_facility_ids:
                    print(f"Skipping {pdf_file} due to invalid EPA Facility ID")
                    log_file.write(f"Skipped {pdf_file} (invalid ID) at {time.ctime()}\n")
                    continue
                
                text_before_section_6 = ""
                section_6_text = ""
                in_section_6 = False
                for i in range(len(doc)):
                    page_text = doc[i].get_text("text")
                    if not page_text:
                        print(f"No text extracted from page {i+1} of {pdf_file.name}")
                        log_file.write(f"No text extracted from page {i+1} of {pdf_file.name} at {time.ctime()}\n")
                        continue
                    if re.search(r"Section 6\. Accident History", page_text, re.IGNORECASE):
                        in_section_6 = True
                    if in_section_6 and re.search(r"Section 9\. Emergency Response", page_text, re.IGNORECASE):
                        break
                    if in_section_6:
                        section_6_text += page_text + "\n"
                    else:
                        text_before_section_6 += page_text + "\n"

                naics_code = extract_process_naics(text_before_section_6)

                if not section_6_text:
                    print(f"Section 6 not found in {pdf_file.name}")
                    log_file.write(f"Section 6 not found in {pdf_file.name} at {time.ctime()}\n")
                    results.append({
                        "EPA Facility ID": epa_id,
                        "Has Accident": "No",
                        "Accident Count": 0,
                        "NAICS Code": naics_code,
                        "Accident Details": None
                    })
                    continue
                
                accident_data = parse_accident_history(section_6_text)
                for data in accident_data:
                    data["EPA Facility ID"] = epa_id
                    data["NAICS Code"] = naics_code
                    results.append(data)
                    print(f"Processed {pdf_file.name}: {data}")
            
            processed_files += 1
            if processed_files % batch_size == 0:
                print(f"Processed {processed_files}/{total_files} files...")
                pd.DataFrame(results).to_csv(output_csv, mode='a', header=not os.path.exists(output_csv), index=False)
                log_file.write(f"Batch saved at {processed_files}/{total_files} files at {time.ctime()}\n")
                results = []  # Clear after successful write

        except Exception as e:
            print(f"Error processing {pdf_file.name}: {e}")
            log_file.write(f"Error processing {pdf_file.name}: {e} at {time.ctime()}\n")
            continue

if results:
    pd.DataFrame(results).to_csv(output_csv, mode='a', header=False, index=False)

# Keep log file open until the end
with open(error_log, "a") as log_file:
    if results:
        log_file.write(f"Final batch saved at {time.ctime()}\n")
    end_time = time.time()
    log_file.write(f"Processing ended at {time.ctime()} with {processed_files}/{total_files} files processed\n")
    log_file.write(f"Total time: {end_time - start_time:.2f} seconds\n")

print(f"Total processing time: {end_time - start_time:.2f} seconds")
print(f"Total processed: {processed_files}/{total_files}")
print(f"Facilities with accidents: {len([r for r in results if r['Has Accident'] == 'Yes'])}")
print(f"Total accident records extracted: {sum(r['Accident Count'] for r in results if r['Has Accident'] == 'Yes')}")
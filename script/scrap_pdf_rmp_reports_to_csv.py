# -*- coding: utf-8 -*-
"""
Created on Thu May  8 22:22:49 2025

@author: MOGIC
"""
import fitz  # PyMuPDF
import glob
import os
import pandas as pd
import time
import logging
from datetime import datetime
import re

# Set up logging
logging.basicConfig(
    filename="fac_acc_chem_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Define paths and output files
input_dir = r"C:\MS Data Science - WMU\EDGI\epa-risk-management-plans\reports"
output_chemicals_file = "rmp_chemical.csv"  # New file for unique chemicals
output_facility_chemicals_file = "rmp_facility_chemicals.csv"  # Updated junction table
output_naics_file = "rmp_naics.csv"  # New file for unique NAICS codes
output_facility_naics_file = "rmp_facility_naics.csv"  # Updated junction table
output_accidents_file = "rmp_facility_accidents.csv"
output_accident_chemicals_file = "rmp_accident_chemicals.csv"

# Initialize counters for unique IDs and statistics
chemical_id_counter = 1
facility_chemical_id_counter = 1
naics_id_counter = 1
facility_naics_id_counter = 1
accident_chemical_id_counter = 1
chemical_data = []  # For rmp_chemical.csv
facility_chemicals_data = []  # For rmp_facility_chemicals.csv
naics_data = []  # For rmp_naics.csv
facility_naics_data = []  # For rmp_facility_naics.csv
accidents_data = []
accident_chemicals_data = []
unique_chemicals = {}  # Track unique chemicals by cas_number
unique_naics = {}  # Track unique NAICS codes by naics_code
stats = {
    "total_pdfs": 0,
    "successful_chemicals": 0,
    "successful_naics": 0,
    "total_accidents": 0,
    "accidents_with_chemicals": 0,
    "total_accident_chemicals": 0,
    "unique_accident_chemicals": 0,
    "skipped_pdfs": 0,
    "errors": 0,
    "facilities_with_accidents": 0,
    "start_time": time.time(),
    "pdf_times": []  # To track time per PDF
}

# Log script start
logging.info("Starting the PDF scraping process")

# Function to filter out header/footer lines
def filter_header_footer(text):
    lines = text.splitlines()
    filtered_lines = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if re.search(r"Facility Name:|EPA Facility Identifier:|Plan Sequence Number:|Data displayed is accurate as of|Page \d+ of \d+", line):
            continue
        filtered_lines.append(line)
    return "\n".join(filtered_lines)

# Function to extract text from PDF
def extract_text_from_pdf(pdf_path):
    start_time = time.time()
    doc = fitz.open(pdf_path)
    full_text = ""
    for page in doc:
        text = page.get_text("text")
        if text:
            full_text += text + "\n"
    doc.close()
    end_time = time.time()
    stats["pdf_times"].append(end_time - start_time)
    logging.info(f"Extracted text from {pdf_path}, length: {len(full_text)} characters, time: {end_time - start_time:.2f} seconds")
    return full_text

# Function to parse chemicals from text
def parse_chemicals(text, facility_id, chemical_id_counter, facility_chemical_id_counter):
    chemicals = []
    current_chemical = {}
    current_key = None
    value_buffer = []
    
    lines = text.splitlines()
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith("Process NAICS"):
            break
        if line.startswith("Program Level:"):
            if current_key and value_buffer:
                current_chemical[current_key] = " ".join(value_buffer).strip()
                value_buffer = []
            current_key = "program_level"
            continue
        elif line.startswith("Chemical Name:"):
            if current_chemical.get("chemical_name"):
                chemicals.append(current_chemical)
            current_chemical = {}
            if value_buffer:
                current_chemical[current_key] = " ".join(value_buffer).strip()
                value_buffer = []
            current_key = "chemical_name"
            continue
        elif line.startswith("CAS Number:"):
            if value_buffer:
                current_chemical[current_key] = " ".join(value_buffer).strip()
                value_buffer = []
            current_key = "cas_number"
            continue
        elif line.startswith("Flammable/Toxic:"):
            if value_buffer:
                current_chemical[current_key] = " ".join(value_buffer).strip()
                value_buffer = []
            current_key = "flammable_toxic"
            continue
        
        if current_key:
            if "Flammable Mixture Chemical Components" in line:
                continue
            value_buffer.append(line)
            if current_key == "chemical_name" and line.endswith("]"):
                current_chemical[current_key] = " ".join(value_buffer).strip()
                value_buffer = []
                current_key = None
    
    if current_key and value_buffer:
        current_chemical[current_key] = " ".join(value_buffer).strip()
    if current_chemical.get("chemical_name"):
        chemicals.append(current_chemical)
    
    # Process chemicals for rmp_chemical and rmp_facility_chemical
    facility_chemicals_list = []
    for chem in chemicals:
        cas_number = chem["cas_number"]
        chemical_name = chem["chemical_name"]
        flammable_toxic = chem.get("flammable_toxic", None)
        
        # Add to rmp_chemical if not already present
        if cas_number not in unique_chemicals:
            unique_chemicals[cas_number] = {
                "chemical_id": chemical_id_counter,
                "chemical_name": chemical_name,
                "cas_number": cas_number,
                "flammable_toxic": flammable_toxic
            }
            chemical_data.append(unique_chemicals[cas_number])
            chemical_id_counter += 1
        
        # Add to rmp_facility_chemical
        facility_chemical_entry = {
            "facility_chemical_id": facility_chemical_id_counter,
            "facility_id": facility_id,
            "chemical_id": unique_chemicals[cas_number]["chemical_id"],
            "program_level": chem.get("program_level", None)
        }
        facility_chemicals_list.append(facility_chemical_entry)
        facility_chemical_id_counter += 1
    
    logging.info(f"Parsed chemicals for {facility_id}: {len(facility_chemicals_list)} chemicals, sample: {facility_chemicals_list[:1]}")
    return facility_chemicals_list, chemical_id_counter, facility_chemical_id_counter

# Function to parse NAICS from text
def parse_naics(text, facility_id, facility_naics_id_counter):
    naics = []
    current_naics = {}
    current_key = None
    value_buffer = []
    
    lines = text.splitlines()
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith("Section 6. Accident History"):
            break
        if line.startswith("NAICS Code:"):
            if current_naics.get("naics_code") and current_key == "naics_description" and value_buffer:
                current_naics["naics_description"] = " ".join(value_buffer).strip()
                value_buffer = []
                naics.append(current_naics)
            current_naics = {}
            current_key = "naics_code"
            value_buffer = []
            continue
        elif line.startswith("NAICS Description:"):
            if current_key == "naics_code" and value_buffer:
                current_naics["naics_code"] = " ".join(value_buffer).strip()
                value_buffer = []
            current_key = "naics_description"
            continue
        
        if current_key:
            value_buffer.append(line)
    
    if current_key == "naics_description" and value_buffer:
        current_naics["naics_description"] = " ".join(value_buffer).strip()
    if current_naics.get("naics_code"):
        naics.append(current_naics)
    
    # Process NAICS for rmp_naics and rmp_facility_naics
    facility_naics_list = []
    for n in naics:
        naics_code = n["naics_code"]
        naics_description = n["naics_description"]
        
        # Add to rmp_naics if not already present
        if naics_code not in unique_naics:
            unique_naics[naics_code] = {
                "naics_code": naics_code,
                "naics_description": naics_description
            }
            naics_data.append(unique_naics[naics_code])
        
        # Add to rmp_facility_naics
        facility_naics_entry = {
            "facility_naics_id": facility_naics_id_counter,
            "facility_id": facility_id,
            "naics_code": naics_code
        }
        facility_naics_list.append(facility_naics_entry)
        facility_naics_id_counter += 1
    
    logging.info(f"Parsed NAICS for {facility_id}: {len(facility_naics_list)} NAICS entries, sample: {facility_naics_list[:1]}")
    return facility_naics_list, facility_naics_id_counter

# Updated Function to parse accidents from text
def parse_accidents(text, facility_id):
    accidents = []
    accident_blocks = re.finditer(r"Accident History ID: Accident \d+([\s\S]*?)(?=Accident History ID: Accident \d+|\Z)", text, re.IGNORECASE)
    accident_count = 0
    
    for block in accident_blocks:
        accident_count += 1
        block_text = block.group(0)
        current_accident = {}
        
        # Extract Accident ID
        accident_id_match = re.search(r"Accident History ID: Accident (\d+)", block_text, re.IGNORECASE)
        if accident_id_match:
            accident_number = accident_id_match.group(1)
            current_accident["accident_id"] = f"Accident {accident_number}"
            current_accident["facility_accident_id"] = f"{facility_id}_{accident_number}"
        else:
            current_accident["accident_id"] = "Unknown"
            current_accident["facility_accident_id"] = f"{facility_id}_unknown"
        
        # Extract other fields using regex
        date_match = re.search(r"Date of Accident:\s*([A-Za-z]+\s*\d{4})", block_text, re.IGNORECASE)
        current_accident["date_of_accident"] = date_match.group(1) if date_match else None
        
        time_match = re.search(r"Time Accident Began \(HH:MM\):\s*(\d{2}:\d{2})", block_text, re.IGNORECASE)
        current_accident["time_accident_began"] = time_match.group(1) if time_match else None
        
        naics_match = re.search(r"NAICS Code of Process Involved:\s*(\d+)", block_text, re.IGNORECASE)
        naics_code = naics_match.group(1) if naics_match else None
        current_accident["naics_code"] = naics_code
        
        # Add NAICS to rmp_naics if not already present (from accident)
        if naics_code and naics_code not in unique_naics:
            naics_desc_match = re.search(r"NAICS Description:\s*(.+)", block_text, re.IGNORECASE)
            naics_description = naics_desc_match.group(1).strip() if naics_desc_match else None
            unique_naics[naics_code] = {
                "naics_code": naics_code,
                "naics_description": naics_description
            }
            naics_data.append(unique_naics[naics_code])
        
        release_duration_match = re.search(r"Release Duration:\s*(\d{3})\s*Hours\s*(\d{2})\s*Minutes", block_text, re.IGNORECASE)
        if release_duration_match:
            hours = release_duration_match.group(1)
            minutes = release_duration_match.group(2)
            current_accident["release_duration"] = f"{hours} Hours {minutes} Minutes"
        else:
            current_accident["release_duration"] = None
        
        current_accident["facility_id"] = facility_id
        accidents.append(current_accident)
        logging.info(f"Parsed accident {accident_count} for facility {facility_id}: {current_accident}")
    
    logging.info(f"Total accidents parsed for facility {facility_id}: {len(accidents)}")
    return accidents

# Function to parse accident chemicals from text
def parse_accident_chemicals(text, facility_id, accident_id, accident_chemical_id_counter, chemical_id_counter):
    chemicals = []
    current_chemical = {}
    current_key = None
    value_buffer = []
    chemical_id = 0
    in_flammable_mixture = False
    temp_quantities = {"quantity_released_lbs": None, "percent_weight": None}  # Initialize with None

    lines = text.splitlines()
    logging.info(f"Raw chemical text for {accident_id}: {text}")
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
        if line.startswith("Accident History ID:") or line.startswith("Section 7") or line.startswith("Section 9"):
            if current_chemical and current_key and value_buffer and current_chemical.get("chemical_name"):
                current_chemical[current_key] = " ".join(value_buffer).strip()
                chemicals.append(current_chemical)
            break
        if line.startswith("Flammable Mixture Chemical Components"):
            if current_chemical and current_key and value_buffer and current_chemical.get("chemical_name"):
                current_chemical[current_key] = " ".join(value_buffer).strip()
                chemicals.append(current_chemical)
            current_chemical = {}
            value_buffer = []
            in_flammable_mixture = True
            # Set defaults for flammable mixture components
            temp_quantities["quantity_released_lbs"] = "N/A"
            temp_quantities["percent_weight"] = "N/A"
            continue
        elif line.startswith("Chemicals in Accident History"):
            if current_chemical and current_key and value_buffer and current_chemical.get("chemical_name"):
                current_chemical[current_key] = " ".join(value_buffer).strip()
                chemicals.append(current_chemical)
            current_chemical = {}
            value_buffer = []
            temp_quantities = {"quantity_released_lbs": None, "percent_weight": None}  # Reset for new section
            in_flammable_mixture = False
            continue
        elif line.startswith("Quantity Released (lbs):"):
            if current_chemical and current_key and value_buffer and current_chemical.get("chemical_name"):
                current_chemical[current_key] = " ".join(value_buffer).strip()
                chemicals.append(current_chemical)
            current_chemical = {}
            current_key = "quantity_released_lbs"
            value_buffer = []
            qty = line.replace("Quantity Released (lbs):", "").strip()
            temp_quantities["quantity_released_lbs"] = qty if qty else "N/A"
            continue
        elif line.startswith("Percent Weight:"):
            if current_key and value_buffer:
                temp_quantities["percent_weight"] = " ".join(value_buffer).strip() if value_buffer else "N/A"
            current_key = "percent_weight"
            value_buffer = []
            pct = line.replace("Percent Weight:", "").strip()
            temp_quantities["percent_weight"] = pct if pct != "" else "N/A"
            continue
        elif line.startswith("Chemical Name:"):
            if current_chemical and current_key and value_buffer and current_chemical.get("chemical_name"):
                current_chemical[current_key] = " ".join(value_buffer).strip()
                chemicals.append(current_chemical)
            current_chemical = {}
            current_chemical.update(temp_quantities)
            current_key = "chemical_name"
            value_buffer = []
            value_buffer.append(line.replace("Chemical Name:", "").strip())
            continue
        elif line.startswith("CAS Number:"):
            if current_key and value_buffer:
                current_chemical[current_key] = " ".join(value_buffer).strip()
            current_key = "cas_number"
            value_buffer = []
            value_buffer.append(line.replace("CAS Number:", "").strip())
            continue
        elif line.startswith("Flammable/Toxic:"):
            if current_key and value_buffer:
                current_chemical[current_key] = " ".join(value_buffer).strip()
            current_key = "flammable_toxic"
            value_buffer = []
            value_buffer.append(line.replace("Flammable/Toxic:", "").strip())
            continue
        
        if current_key:
            value_buffer.append(line)
            next_line = lines[i + 1] if i + 1 < len(lines) else ""
            if current_key == "quantity_released_lbs" and next_line.startswith("Percent Weight:"):
                temp_quantities["quantity_released_lbs"] = " ".join(value_buffer).strip() if value_buffer else "N/A"
                value_buffer = []
            elif current_key == "percent_weight" and next_line.startswith("Chemical Name:"):
                temp_quantities["percent_weight"] = " ".join(value_buffer).strip() if value_buffer else "N/A"
                value_buffer = []
            elif current_key == "chemical_name" and next_line.startswith("CAS Number:"):
                current_chemical[current_key] = " ".join(value_buffer).strip()
                value_buffer = []
            elif current_key == "cas_number" and next_line.startswith("Flammable/Toxic:"):
                current_chemical[current_key] = " ".join(value_buffer).strip()
                value_buffer = []
            elif current_key == "flammable_toxic" and (next_line.startswith("Section 9") or next_line.startswith("Accident History ID:") or next_line.startswith("Section 7")):
                current_chemical[current_key] = " ".join(value_buffer).strip()
                value_buffer = []
    
    if current_key and value_buffer and current_chemical.get("chemical_name"):
        current_chemical[current_key] = " ".join(value_buffer).strip()
    if current_chemical.get("chemical_name"):
        chemicals.append(current_chemical)
    
    # Process accident chemicals for rmp_chemical and rmp_accident_chemical
    accident_chemicals_list = []
    accident_number = int(accident_id.replace("Accident ", ""))
    for chem in chemicals:
        cas_number = chem["cas_number"]
        chemical_name = chem["chemical_name"]
        flammable_toxic = chem.get("flammable_toxic", None)
        
        # Add to rmp_chemical if not already present
        if cas_number not in unique_chemicals:
            unique_chemicals[cas_number] = {
                "chemical_id": chemical_id_counter,
                "chemical_name": chemical_name,
                "cas_number": cas_number,
                "flammable_toxic": flammable_toxic
            }
            chemical_data.append(unique_chemicals[cas_number])
            chemical_id_counter += 1
        
        # Add to rmp_accident_chemical using existing chemical_id
        chemical_id += 1
        accident_chemical_entry = {
            "accident_chemical_id": accident_chemical_id_counter,
            "facility_accident_chemical_id": f"{facility_id}_{accident_number}_{chemical_id}",
            "facility_accident_id": f"{facility_id}_{accident_number}",
            "quantity_released_lbs": chem.get("quantity_released_lbs", "N/A"),
            "percent_weight": chem.get("percent_weight", "N/A"),
            "chemical_id": unique_chemicals[cas_number]["chemical_id"]
        }
        if in_flammable_mixture and "quantity_released_lbs" not in chem:
            accident_chemical_entry["quantity_released_lbs"] = "N/A"
            accident_chemical_entry["percent_weight"] = "N/A"
        accident_chemicals_list.append(accident_chemical_entry)
        accident_chemical_id_counter += 1
        # Reset temp_quantities after each chemical to prevent duplication
        temp_quantities = {"quantity_released_lbs": None, "percent_weight": None}
    
    logging.info(f"Parsed {len(accident_chemicals_list)} chemicals for {accident_id}: {accident_chemicals_list}")
    return accident_chemicals_list, accident_chemical_id_counter, chemical_id_counter

# Main processing loop
pdf_files = glob.glob(os.path.join(input_dir, "**", "*.pdf"), recursive=True)
stats["total_pdfs"] = len(pdf_files)
logging.info(f"Found {stats['total_pdfs']} PDF files to process")

for pdf_file in pdf_files:
    facility_id = os.path.basename(pdf_file).replace(".pdf", "")
    logging.info(f"Processing {pdf_file} (Facility ID: {facility_id})")

    try:
        text = extract_text_from_pdf(pdf_file)
        if not text:
            logging.warning(f"No text extracted from {pdf_file}")
            stats["skipped_pdfs"] += 1
            continue
    except Exception as e:
        logging.error(f"Error reading {pdf_file}: {e}")
        stats["errors"] += 1
        stats["skipped_pdfs"] += 1
        continue

    process_chemicals_start = text.find("Process Chemicals")
    if process_chemicals_start == -1:
        logging.warning(f"'Process Chemicals' section not found in {pdf_file}")
        stats["skipped_pdfs"] += 1
        continue
    process_chemicals_start += len("Process Chemicals")

    process_naics_start = text.find("Process NAICS", process_chemicals_start)
    if process_naics_start == -1:
        logging.warning(f"'Process NAICS' section not found in {pdf_file}")
        stats["skipped_pdfs"] += 1
        continue

    accident_history_start = text.find("Section 6. Accident History", process_naics_start)
    if accident_history_start == -1:
        logging.warning(f"'Section 6. Accident History' section not found in {pdf_file}")
        stats["skipped_pdfs"] += 1
        continue
    section_7_pos = text.find("Section 7", accident_history_start)
    section_9_pos = text.find("Section 9. Emergency Response", accident_history_start)
    if section_7_pos == -1 and section_9_pos == -1:
        accident_history_end = len(text)
    elif section_7_pos == -1:
        accident_history_end = section_9_pos
    elif section_9_pos == -1:
        accident_history_end = section_7_pos
    else:
        accident_history_end = min(section_7_pos, section_9_pos)

    chemicals_text = text[process_chemicals_start:process_naics_start].strip()
    chemicals_text = filter_header_footer(chemicals_text)
    logging.info(f"Chemicals text for {facility_id}:\n{chemicals_text}")
    if chemicals_text:
        new_facility_chemicals, chemical_id_counter, facility_chemical_id_counter = parse_chemicals(chemicals_text, facility_id, chemical_id_counter, facility_chemical_id_counter)
        facility_chemicals_data.extend(new_facility_chemicals)
        if new_facility_chemicals:
            stats["successful_chemicals"] += 1
        stats["total_chemicals"] = stats.get("total_chemicals", 0) + len(new_facility_chemicals)

    naics_text = text[process_naics_start + len("Process NAICS"):accident_history_start].strip()
    naics_text = filter_header_footer(naics_text)
    logging.info(f"NAICS text for {facility_id}:\n{naics_text}")
    if naics_text:
        new_facility_naics, facility_naics_id_counter = parse_naics(naics_text, facility_id, facility_naics_id_counter)
        facility_naics_data.extend(new_facility_naics)
        if new_facility_naics:
            stats["successful_naics"] += 1
        stats["total_naics"] = stats.get("total_naics", 0) + len(new_facility_naics)

    accidents_text = text[accident_history_start:accident_history_end].strip()
    accidents_text = filter_header_footer(accidents_text)
    logging.info(f"Accidents text for {facility_id}:\n{accidents_text}")
    if accidents_text:
        new_accidents = parse_accidents(accidents_text, facility_id)
        if new_accidents:
            stats["facilities_with_accidents"] += 1
            stats["total_accidents"] += len(new_accidents)
            accidents_data.extend(new_accidents)
            logging.info(f"Added {len(new_accidents)} accidents for facility {facility_id}, total accidents so far: {len(accidents_data)}")
        for accident in new_accidents:
            accident_id_pattern = f"Accident History ID: {accident['accident_id']}"
            chemical_start = accidents_text.find(accident_id_pattern)
            if chemical_start == -1:
                stats["accidents_with_chemicals"] += 0
                logging.info(f"No chemicals found for {accident['accident_id']} in {facility_id}")
                continue
            chemical_section_start = accidents_text.find("Chemicals in Accident History", chemical_start)
            if chemical_section_start == -1:
                stats["accidents_with_chemicals"] += 0
                logging.info(f"No chemicals found for {accident['accident_id']} in {facility_id}")
                continue
            next_accident_start = accidents_text.find("Accident History ID:", chemical_section_start + len("Chemicals in Accident History"))
            section_9_end = accidents_text.find("Section 9. Emergency Response", chemical_section_start)
            if next_accident_start == -1 and section_9_end == -1:
                chemical_section_end = len(accidents_text)
            elif next_accident_start == -1:
                chemical_section_end = section_9_end
            elif section_9_end == -1:
                chemical_section_end = next_accident_start
            else:
                chemical_section_end = min(next_accident_start, section_9_end)
            accident_chemicals_text = accidents_text[chemical_section_start:chemical_section_end].strip()
            logging.info(f"Accident chemicals text for {accident['accident_id']}:\n{accident_chemicals_text}")
            if accident_chemicals_text:
                new_accident_chemicals, accident_chemical_id_counter, chemical_id_counter = parse_accident_chemicals(accident_chemicals_text, facility_id, accident['accident_id'], accident_chemical_id_counter, chemical_id_counter)
                accident_chemicals_data.extend(new_accident_chemicals)
                if new_accident_chemicals:
                    stats["accidents_with_chemicals"] += 1
                    stats["total_accident_chemicals"] += len(new_accident_chemicals)
                    logging.info(f"Added {len(new_accident_chemicals)} chemicals for {accident['accident_id']}, total chemicals so far: {len(accident_chemicals_data)}")

# Save the data to CSV files
chemical_df = pd.DataFrame(chemical_data, columns=["chemical_id", "chemical_name", "cas_number", "flammable_toxic"])
if os.path.exists(output_chemicals_file):
    chemical_df.to_csv(output_chemicals_file, mode='a', header=False, index=False)
else:
    chemical_df.to_csv(output_chemicals_file, index=False)

facility_chemicals_df = pd.DataFrame(facility_chemicals_data, columns=["facility_chemical_id", "facility_id", "chemical_id", "program_level"])
if os.path.exists(output_facility_chemicals_file):
    facility_chemicals_df.to_csv(output_facility_chemicals_file, mode='a', header=False, index=False)
else:
    facility_chemicals_df.to_csv(output_facility_chemicals_file, index=False)

naics_df = pd.DataFrame(naics_data, columns=["naics_code", "naics_description"])
if os.path.exists(output_naics_file):
    naics_df.to_csv(output_naics_file, mode='a', header=False, index=False)
else:
    naics_df.to_csv(output_naics_file, index=False)

facility_naics_df = pd.DataFrame(facility_naics_data, columns=["facility_naics_id", "facility_id", "naics_code"])
if os.path.exists(output_facility_naics_file):
    facility_naics_df.to_csv(output_facility_naics_file, mode='a', header=False, index=False)
else:
    facility_naics_df.to_csv(output_facility_naics_file, index=False)

accidents_df = pd.DataFrame(accidents_data, columns=["facility_accident_id", "accident_id", "facility_id", "date_of_accident", "time_accident_began", "release_duration", "naics_code"])
if os.path.exists(output_accidents_file):
    accidents_df.to_csv(output_accidents_file, mode='a', header=False, index=False)
else:
    accidents_df.to_csv(output_accidents_file, index=False)

accident_chemicals_df = pd.DataFrame(accident_chemicals_data, columns=["accident_chemical_id", "facility_accident_chemical_id", "facility_accident_id", "quantity_released_lbs", "percent_weight", "chemical_id"])
if os.path.exists(output_accident_chemicals_file):
    accident_chemicals_df.to_csv(output_accident_chemicals_file, mode='a', header=False, index=False)
else:
    accident_chemicals_df.to_csv(output_accident_chemicals_file, index=False)

# Calculate statistics
end_time = time.time()
total_time = end_time - stats["start_time"]
avg_time_per_pdf = total_time / stats["total_pdfs"] if stats["total_pdfs"] > 0 else 0
stats["unique_accident_chemicals"] = len(set(chem["chemical_id"] for chem in accident_chemicals_data))

# Log summary statistics
logging.info("=== Summary Statistics ===")
logging.info(f"Total PDFs Found: {stats['total_pdfs']}")
logging.info(f"Facilities with Accidents: {stats['facilities_with_accidents']}")
logging.info(f"Total Accidents Found: {stats['total_accidents']}")
logging.info(f"Accidents with Chemicals Extracted: {stats['accidents_with_chemicals']}")
logging.info(f"Total Accident Chemicals Extracted: {stats['total_accident_chemicals']}")
logging.info(f"Unique Accident Chemicals: {stats['unique_accident_chemicals']}")
logging.info(f"PDFs Skipped: {stats['skipped_pdfs']}")
logging.info(f"Errors Encountered: {stats['errors']}")
logging.info(f"Total Runtime: {total_time:.2f} seconds")
logging.info(f"Average Time per PDF: {avg_time_per_pdf:.4f} seconds")
logging.info("Script completed")

print(f"Data saved to {output_chemicals_file}, {output_facility_chemicals_file}, {output_naics_file}, {output_facility_naics_file}, {output_accidents_file}, and {output_accident_chemicals_file}")
print(f"See fac_acc_chem_log.txt for detailed statistics and logs")
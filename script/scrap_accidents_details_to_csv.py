import fitz  # PyMuPDF
import re
import pandas as pd
import os
from pathlib import Path
import time
import logging

# Set up logging
logging.basicConfig(
    filename="accident_history_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Define paths
pdf_dir = r"C:\MS Data Science - WMU\EDGI\epa-risk-management-plans\reports"
rmp_accident_history_csv = r"C:\MS Data Science - WMU\EDGI\rmp-datasette\bckup\rmp_accident_history.csv"
output_csv = r"C:\MS Data Science - WMU\EDGI\rmp-datasette\bckup\rmp_accident_history_detailed.csv"
error_log = r"C:\MS Data Science - WMU\EDGI\rmp-datasette\bckup\error_log.txt"

# Read the rmp_accident_history.csv to get facilities with accidents
rmp_df = pd.read_csv(rmp_accident_history_csv)
facilities_with_accidents = set(rmp_df[rmp_df["Has Accident"] == "Yes"]["EPA Facility ID"].astype(str))
total_expected_accidents = rmp_df[rmp_df["Has Accident"] == "Yes"]["Accident Count"].sum()
print(f"Facilities with accidents: {len(facilities_with_accidents)}")
print(f"Sample facility IDs from CSV: {list(facilities_with_accidents)[:5]}")
print(f"Total expected accidents from CSV: {total_expected_accidents}")
logging.info(f"Facilities with accidents: {len(facilities_with_accidents)}, Expected accidents: {total_expected_accidents}")

# Check if the PDF directory exists and list PDFs recursively
print(f"Checking PDF directory: {pdf_dir}")
if not os.path.exists(pdf_dir):
    print(f"Error: PDF directory {pdf_dir} does not exist.")
    exit(1)

all_pdf_files = list(Path(pdf_dir).rglob("*.pdf"))
print(f"Total PDFs found in directory and subfolders: {len(all_pdf_files)}")
if not all_pdf_files:
    print("Error: No PDF files found in the directory or subfolders.")
    exit(1)

# Function to extract EPA Facility ID from filename
def extract_epa_facility_id_from_filename(filename):
    match = re.search(r"(\d+)\.pdf$", filename)
    if not match:
        logging.warning(f"Could not extract EPA Facility ID from filename: {filename} (pattern mismatch)")
        print(f"Could not extract EPA Facility ID from filename: {filename} (pattern mismatch)")
        return None
    return match.group(1)

# Extract facility IDs from PDFs for debugging
pdf_facility_ids = set()
for pdf in all_pdf_files:
    facility_id = extract_epa_facility_id_from_filename(pdf.name)
    if facility_id:
        pdf_facility_ids.add(facility_id)
print(f"Sample facility IDs from PDFs: {list(pdf_facility_ids)[:5]}")
print(f"Total facility IDs extracted from PDFs: {len(pdf_facility_ids)}")
logging.info(f"Sample facility IDs from PDFs: {list(pdf_facility_ids)[:5]}, Total: {len(pdf_facility_ids)}")

# Filter PDFs based on facilities with accidents
pdf_files = [f for f in all_pdf_files if extract_epa_facility_id_from_filename(f.name) in facilities_with_accidents]
total_files = len(pdf_files)
print(f"PDFs matching facilities with accidents: {total_files}")
if total_files == 0:
    print("Error: No PDFs match the facility IDs from the CSV.")
    print(f"Intersection of CSV and PDF facility IDs: {len(pdf_facility_ids.intersection(facilities_with_accidents))}")
    logging.error(f"No PDFs match facility IDs. Intersection: {len(pdf_facility_ids.intersection(facilities_with_accidents))}")
    exit(1)

# Initialize variables
processed_files = 0
batch_size = 1000

# Start timing
start_time = time.time()

# Initialize list to store results
results = []
all_results = []

# Function to extract NAICS Code before Section 6
def extract_process_naics(text_before_section_6):
    naics_code = re.search(r"NAICS Code:\s*(\d+)", text_before_section_6, re.IGNORECASE)
    return naics_code.group(1) if naics_code else None

# Function to parse all Accident History blocks with detailed information
def parse_accident_history(section_6_text):
    accidents = []
    if re.search(r"No records found|No accidents reported", section_6_text, re.IGNORECASE):
        logging.warning(f"No accidents found in section_6_text: {section_6_text[:100]}...")
        return {
            "Has Accident": "No",
            "Accident Count": 0,
            "Accident Details": None
        }
    
    accident_blocks = re.finditer(r"Accident History ID: Accident \d+([\s\S]*?)(?=Accident History ID: Accident \d+|\Z)", section_6_text, re.IGNORECASE)
    accident_count = 0
    
    for block in accident_blocks:
        block_text = block.group(0)
        accident_count += 1
        accident = {}

        # Basic accident info
        accident_id_match = re.search(r"Accident History ID: Accident (\d+)", block_text, re.IGNORECASE)
        accident["Accident History ID"] = accident_id_match.group(1) if accident_id_match else None
        date_match = re.search(r"Date of Accident:\s*([A-Za-z]+\s*\d{4})", block_text, re.IGNORECASE)
        accident["Date of Accident"] = date_match.group(1) if date_match else None
        time_match = re.search(r"Time Accident Began \(HH:MM\):\s*(\d{2}:\d{2})", block_text, re.IGNORECASE)
        accident["Time Accident Began"] = time_match.group(1) if time_match else None
        naics_match = re.search(r"NAICS Code of Process Involved:\s*(\d+)", block_text, re.IGNORECASE)
        accident["NAICS Code of Process Involved"] = naics_match.group(1) if naics_match else None
        naics_desc_match = re.search(r"NAICS Description:\s*(.+)", block_text, re.IGNORECASE)
        accident["NAICS Description"] = naics_desc_match.group(1).strip() if naics_desc_match else None
        release_duration_match = re.search(r"Release Duration:\s*(\d{3})\s*Hours\s*(\d{2})\s*Minutes", block_text, re.IGNORECASE)
        accident["Release Duration (Hours)"] = release_duration_match.group(1) if release_duration_match else None
        accident["Release Duration (Minutes)"] = release_duration_match.group(2) if release_duration_match else None

        # Release Event
        accident["Gas Release"] = re.search(r"Gas Release:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Gas Release:", block_text, re.IGNORECASE) else None
        accident["Liquid Spill/Evaporation"] = re.search(r"Liquid Spill/Evaporation:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Liquid Spill/Evaporation:", block_text, re.IGNORECASE) else None
        accident["Fire"] = re.search(r"Fire:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Fire:", block_text, re.IGNORECASE) else None
        accident["Explosion"] = re.search(r"Explosion:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Explosion:", block_text, re.IGNORECASE) else None
        accident["Uncontrolled/Runaway Reaction"] = re.search(r"Uncontrolled/Runaway Reaction:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Uncontrolled/Runaway Reaction:", block_text, re.IGNORECASE) else None

        # Release Source
        accident["Storage Vessel"] = re.search(r"Storage Vessel:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Storage Vessel:", block_text, re.IGNORECASE) else None
        accident["Piping"] = re.search(r"Piping:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Piping:", block_text, re.IGNORECASE) else None
        accident["Process Vessel"] = re.search(r"Process Vessel:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Process Vessel:", block_text, re.IGNORECASE) else None
        accident["Transfer Hose"] = re.search(r"Transfer Hose:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Transfer Hose:", block_text, re.IGNORECASE) else None
        accident["Valve"] = re.search(r"Valve:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Valve:", block_text, re.IGNORECASE) else None
        accident["Pump"] = re.search(r"Pump:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Pump:", block_text, re.IGNORECASE) else None
        accident["Joint"] = re.search(r"Joint:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Joint:", block_text, re.IGNORECASE) else None
        other_source_match = re.search(r"Other Release Source:\s*(.+)", block_text, re.IGNORECASE)
        accident["Other Release Source"] = other_source_match.group(1).strip() if other_source_match else None

        # Weather Conditions
        wind_speed_match = re.search(r"Wind Speed:\s*(\d+\.?\d*)", block_text, re.IGNORECASE)
        accident["Wind Speed"] = wind_speed_match.group(1) if wind_speed_match else None
        units_match = re.search(r"Units:\s*(.+)", block_text, re.IGNORECASE)
        accident["Wind Speed Units"] = units_match.group(1).strip() if units_match else None
        direction_match = re.search(r"Direction:\s*(.+)", block_text, re.IGNORECASE)
        accident["Wind Direction"] = direction_match.group(1).strip() if direction_match else None
        temp_match = re.search(r"Temperature:\s*(\d+)", block_text, re.IGNORECASE)
        accident["Temperature"] = temp_match.group(1) if temp_match else None
        stability_match = re.search(r"Atmospheric Stability Class:\s*([A-Z])", block_text, re.IGNORECASE)
        accident["Atmospheric Stability Class"] = stability_match.group(1) if stability_match else None
        precip_match = re.search(r"Precipitation Present:\s*(Yes|No|N/A)", block_text, re.IGNORECASE)
        accident["Precipitation Present"] = precip_match.group(1) if precip_match else None
        unknown_weather_match = re.search(r"Unknown Weather Conditions:\s*(Yes|No|N/A)", block_text, re.IGNORECASE)
        accident["Unknown Weather Conditions"] = unknown_weather_match.group(1) if unknown_weather_match else None

        # On-Site Impacts
        accident["Employee/Contractor Deaths"] = re.search(r"Employee or Contractor Deaths:\s*(\d+)", block_text, re.IGNORECASE).group(1) if re.search(r"Employee or Contractor Deaths:", block_text, re.IGNORECASE) else None
        accident["Public Responder Deaths"] = re.search(r"Public Responder Deaths:\s*(\d+)", block_text, re.IGNORECASE).group(1) if re.search(r"Public Responder Deaths:", block_text, re.IGNORECASE) else None
        accident["Public Deaths"] = re.search(r"Public Deaths:\s*(\d+)", block_text, re.IGNORECASE).group(1) if re.search(r"Public Deaths:", block_text, re.IGNORECASE) else None
        accident["Employee/Contractor Injuries"] = re.search(r"Employee or Contractor Injuries:\s*(\d+)", block_text, re.IGNORECASE).group(1) if re.search(r"Employee or Contractor Injuries:", block_text, re.IGNORECASE) else None
        accident["Public Responder Injuries"] = re.search(r"Public Responder Injuries:\s*(\d+)", block_text, re.IGNORECASE).group(1) if re.search(r"Public Responder Injuries:", block_text, re.IGNORECASE) else None
        accident["Public Injuries"] = re.search(r"Public Injuries:\s*(\d+)", block_text, re.IGNORECASE).group(1) if re.search(r"Public Injuries:", block_text, re.IGNORECASE) else None
        property_damage_match = re.search(r"On-Site Property Damage \(\$\):\s*(\d+)", block_text, re.IGNORECASE)
        accident["On-Site Property Damage ($)"] = property_damage_match.group(1) if property_damage_match else None

        # Off-Site Impacts
        accident["Off-Site Deaths"] = re.search(r"Deaths:\s*(\d+)", block_text, re.IGNORECASE).group(1) if re.search(r"Deaths:", block_text, re.IGNORECASE) else None
        accident["Off-Site Hospitalizations"] = re.search(r"Hospitalizations:\s*(\d+)", block_text, re.IGNORECASE).group(1) if re.search(r"Hospitalizations:", block_text, re.IGNORECASE) else None
        accident["Off-Site Public Deaths"] = re.search(r"Public Deaths:\s*(\d+)", block_text, re.IGNORECASE).group(1) if re.search(r"Public Deaths:", block_text, re.IGNORECASE) else None
        accident["Off-Site Other Medical Treatments"] = re.search(r"Other Medical Treatments:\s*(\d+)", block_text, re.IGNORECASE).group(1) if re.search(r"Other Medical Treatments:", block_text, re.IGNORECASE) else None
        accident["Evacuated"] = re.search(r"Evacuated:\s*(\d+)", block_text, re.IGNORECASE).group(1) if re.search(r"Evacuated:", block_text, re.IGNORECASE) else None
        accident["Sheltered-in-Place"] = re.search(r"Sheltered-in-Place:\s*(\d+)", block_text, re.IGNORECASE).group(1) if re.search(r"Sheltered-in-Place:", block_text, re.IGNORECASE) else None
        offsite_property_match = re.search(r"Off-Site Property Damage \(\$\):\s*(\d+)", block_text, re.IGNORECASE)
        accident["Off-Site Property Damage ($)"] = offsite_property_match.group(1) if offsite_property_match else None

        # Environmental Damage
        accident["Fish or Animal Kills"] = re.search(r"Fish or Animal Kills:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Fish or Animal Kills:", block_text, re.IGNORECASE) else None
        accident["Tree, Lawn, Shrub, or Crop Damage"] = re.search(r"Tree, Lawn, Shrub, or Crop Damage:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Tree, Lawn, Shrub, or Crop Damage:", block_text, re.IGNORECASE) else None
        accident["Water Contamination"] = re.search(r"Water Contamination:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Water Contamination:", block_text, re.IGNORECASE) else None
        accident["Soil Contamination"] = re.search(r"Soil Contamination:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Soil Contamination:", block_text, re.IGNORECASE) else None
        other_env_match = re.search(r"Other Environmental Damage:\s*(.+)", block_text, re.IGNORECASE)
        accident["Other Environmental Damage"] = other_env_match.group(1).strip() if other_env_match else None

        # Initiating Event
        initiating_match = re.search(r"Initiating Event:\s*(.+)", block_text, re.IGNORECASE)
        accident["Initiating Event"] = initiating_match.group(1).strip() if initiating_match else None

        # Contributing Factors
        accident["Contributing - Equipment Failure"] = re.search(r"Equipment Failure:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Equipment Failure:", block_text, re.IGNORECASE) else None
        accident["Contributing - Human Error"] = re.search(r"Human Error:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Human Error:", block_text, re.IGNORECASE) else None
        accident["Contributing - Improper Procedures"] = re.search(r"Improper Procedures:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Improper Procedures:", block_text, re.IGNORECASE) else None
        accident["Contributing - Overpressurization"] = re.search(r"Overpressurization:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Overpressurization:", block_text, re.IGNORECASE) else None
        accident["Contributing - Upset Condition"] = re.search(r"Upset Condition:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Upset Condition:", block_text, re.IGNORECASE) else None
        accident["Contributing - By-Pass Condition"] = re.search(r"By-Pass Condition:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"By-Pass Condition:", block_text, re.IGNORECASE) else None
        accident["Contributing - Maintenance Activity/Inactivity"] = re.search(r"Maintenance Activity/Inactivity:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Maintenance Activity/Inactivity:", block_text, re.IGNORECASE) else None
        accident["Contributing - Process Design Failure"] = re.search(r"Process Design Failure:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Process Design Failure:", block_text, re.IGNORECASE) else None
        accident["Contributing - Unsuitable Equipment"] = re.search(r"Unsuitable Equipment:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Unsuitable Equipment:", block_text, re.IGNORECASE) else None
        accident["Contributing - Unusual Weather Condition"] = re.search(r"Unusual Weather Condition:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Unusual Weather Condition:", block_text, re.IGNORECASE) else None
        accident["Contributing - Management Error"] = re.search(r"Management Error:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Management Error:", block_text, re.IGNORECASE) else None
        other_contrib_match = re.search(r"Other Contributing Factor:\s*(.+)", block_text, re.IGNORECASE)
        accident["Contributing - Other"] = other_contrib_match.group(1).strip() if other_contrib_match else None

        # Off-Site Responders
        responders_match = re.search(r"Off-Site Responders Notified:\s*(Yes|No, not notified)", block_text, re.IGNORECASE)
        accident["Off-Site Responders Notified"] = responders_match.group(1) if responders_match else None

        # Changes Introduced
        accident["Change - Improved/Upgraded Equipment"] = re.search(r"Improved or Upgraded Equipment:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Improved or Upgraded Equipment:", block_text, re.IGNORECASE) else None
        accident["Change - Revised Maintenance"] = re.search(r"Revised Maintenance:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Revised Maintenance:", block_text, re.IGNORECASE) else None
        accident["Change - Revised Training"] = re.search(r"Revised Training:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Revised Training:", block_text, re.IGNORECASE) else None
        accident["Change - Revised Operating Procedures"] = re.search(r"Revised Operating Procedures:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Revised Operating Procedures:", block_text, re.IGNORECASE) else None
        accident["Change - New Process Controls"] = re.search(r"New Process Controls:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"New Process Controls:", block_text, re.IGNORECASE) else None
        accident["Change - New Mitigation Systems"] = re.search(r"New Mitigation Systems:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"New Mitigation Systems:", block_text, re.IGNORECASE) else None
        accident["Change - Revised Emergency Response Plan"] = re.search(r"Revised Emergency Response Plan:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Revised Emergency Response Plan:", block_text, re.IGNORECASE) else None
        accident["Change - Changed Process"] = re.search(r"Changed Process:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Changed Process:", block_text, re.IGNORECASE) else None
        accident["Change - Reduced Inventory"] = re.search(r"Reduced Inventory:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Reduced Inventory:", block_text, re.IGNORECASE) else None
        accident["Change - None"] = re.search(r"None:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"None:", block_text, re.IGNORECASE) else None
        other_change_match = re.search(r"Other Changes Introduced:\s*(.+)", block_text, re.IGNORECASE)
        accident["Change - Other"] = other_change_match.group(1).strip() if other_change_match else None

        accidents.append(accident)
        logging.info(f"Parsed accident {accident_count}: {accident}")

    return {
        "Has Accident": "Yes" if accident_count > 0 else "No",
        "Accident Count": accident_count,
        "Accident Details": accidents if accidents else None
    }

# Main processing loop
with open(error_log, "a") as log_file:
    log_file.write(f"Processing started at {time.ctime(start_time)}\n")
    for pdf_file in pdf_files:
        try:
            with fitz.open(pdf_file) as doc:
                epa_id = extract_epa_facility_id_from_filename(pdf_file.name)
                if not epa_id:
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
                    continue
                
                accident_data = parse_accident_history(section_6_text)
                
                if accident_data["Has Accident"] == "No":
                    print(f"Unexpected: {pdf_file.name} has no accidents but was in the 'Yes' list")
                    log_file.write(f"Unexpected: {pdf_file.name} has no accidents (EPA ID: {epa_id}), expected count: {rmp_df[rmp_df['EPA Facility ID'].astype(str) == epa_id]['Accident Count'].iloc[0] if not rmp_df[rmp_df['EPA Facility ID'].astype(str) == epa_id].empty else 0} at {time.ctime()}\n")
                else:
                    for accident in accident_data["Accident Details"] or []:
                        row = {
                            "EPA Facility ID": epa_id,
                            "Has Accident": "Yes",
                            "Accident Count": accident_data["Accident Count"],
                            "NAICS Code": naics_code,
                            **accident
                        }
                        results.append(row)
                        all_results.append(row)
                
                print(f"Processed {pdf_file.name}")
                logging.info(f"Processed {pdf_file.name} (EPA ID: {epa_id}), accidents found: {len(accident_data.get('Accident Details', []))}")
            
            processed_files += 1
            if processed_files % batch_size == 0:
                print(f"Processed {processed_files}/{total_files} files...")
                pd.DataFrame(results).to_csv(output_csv, mode='a', header=not os.path.exists(output_csv), index=False)
                log_file.write(f"Batch saved at {processed_files}/{total_files} files at {time.ctime()}\n")
                results = []

        except Exception as e:
            print(f"Error processing {pdf_file.name}: {e}")
            log_file.write(f"Error processing {pdf_file.name}: {e} at {time.ctime()}\n")
            logging.error(f"Error processing {pdf_file.name}: {e}")
            continue

# Save any remaining results
if results:
    pd.DataFrame(results).to_csv(output_csv, mode='a', header=not os.path.exists(output_csv), index=False)

# Final logging
with open(error_log, "a") as log_file:
    if results:
        log_file.write(f"Final batch saved at {time.ctime()}\n")
    end_time = time.time()
    log_file.write(f"Processing ended at {time.ctime()} with {processed_files}/{total_files} files processed\n")
    log_file.write(f"Total time: {end_time - start_time:.2f} seconds\n")
    logging.info(f"Total accidents extracted: {len(all_results)}, Expected: {total_expected_accidents}")

# Final summary
total_accidents_extracted = len(all_results)
print(f"Total processing time: {end_time - start_time:.2f} seconds")
print(f"Total processed: {processed_files}/{total_files}")
print(f"Total accident records extracted: {total_accidents_extracted}")
print(f"Expected accident records (from CSV): {total_expected_accidents}")
print(f"Match: {total_accidents_extracted == total_expected_accidents}")
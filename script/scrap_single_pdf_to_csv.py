import fitz
import re
import pandas as pd
import logging
import time

# Set up logging
logging.basicConfig(filename="accident_history_log_single.txt", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Define paths
pdf_file = r""
output_csv = r""
error_log = r"error_log_single.txt"

# Function to extract EPA Facility ID from filename
def extract_epa_facility_id_from_filename(filename):
    match = re.search(r"(\d+)\.pdf$", filename)
    return match.group(1) if match else None

# Function to extract NAICS Code before Section 6
def extract_process_naics(text_before_section_6):
    naics_code = re.search(r"NAICS Code:\s*(\d+)", text_before_section_6, re.IGNORECASE)
    return naics_code.group(1) if naics_code else None

# Function to parse Accident History
def parse_accident_history(section_6_text, epa_id):
    accidents = []
    if re.search(r"No records found|No accidents reported", section_6_text, re.IGNORECASE):
        logging.warning(f"No accidents found in section_6_text for EPA ID {epa_id}: {section_6_text[:100]}...")
        return {
            "Has Accident": "No",
            "Accident Count": 0,
            "Accident Details": None
        }
    
    accident_blocks = re.finditer(
        r"(?:Accident History ID: Accident \d+|Accident \d+[\s\S]*?)([\s\S]*?)(?=(?:Accident History ID: Accident \d+|Accident \d+|\Z))",
        section_6_text,
        re.IGNORECASE
    )
    accident_count = 0
    
    for block in accident_blocks:
        block_text = block.group(0).strip()
        accident_count += 1
        accident = {}

        try:
            logging.info(f"Extracting Accident History ID for EPA ID {epa_id}")
            accident_id_match = re.search(r"Accident History ID: Accident (\d+)|Accident (\d+)", block_text, re.IGNORECASE)
            accident["Accident History ID"] = accident_id_match.group(1) or accident_id_match.group(2) if accident_id_match else f"Unknown_{accident_count}"

            accident["Facility Accident ID"] = f"{epa_id}_{accident['Accident History ID']}"
            
            logging.info(f"Extracting Date of Accident for EPA ID {epa_id}")
            date_match = re.search(r"Date of Accident:\s*([A-Za-z]+\s*\d{4})", block_text, re.IGNORECASE)
            accident["Date of Accident"] = date_match.group(1) if date_match else None
            
            logging.info(f"Extracting Time Accident Began for EPA ID {epa_id}")
            time_match = re.search(r"Time Accident Began \(HH:MM\):\s*(\d{2}:\d{2})", block_text, re.IGNORECASE)
            accident["Time Accident Began"] = time_match.group(1) if time_match else None
            
            logging.info(f"Extracting NAICS Code of Process Involved for EPA ID {epa_id}")
            naics_match = re.search(r"NAICS Code of Process Involved:\s*(\d+)", block_text, re.IGNORECASE)
            accident["NAICS Code of Process Involved"] = naics_match.group(1) if naics_match else None
            
            logging.info(f"Extracting NAICS Description for EPA ID {epa_id}")
            naics_desc_match = re.search(r"NAICS Description:\s*(.*)", block_text, re.IGNORECASE)
            accident["NAICS Description"] = naics_desc_match.group(1).strip() if naics_desc_match else None
            
            logging.info(f"Extracting Release Duration for EPA ID {epa_id}")
            release_duration_match = re.search(r"Release Duration:\s*(\d{3})\s*Hours\s*(\d{2})\s*Minutes", block_text, re.IGNORECASE)
            accident["Release Duration (Hours)"] = release_duration_match.group(1) if release_duration_match else None
            accident["Release Duration (Minutes)"] = release_duration_match.group(2) if release_duration_match else None

            # Initiating Event
            logging.info(f"Extracting Initiating Event for EPA ID {epa_id}")
            initiating_event_match = re.search(r"Initiating Event:\s*(.+)", block_text, re.IGNORECASE)
            accident["Initiating Event"] = initiating_event_match.group(1).strip() if initiating_event_match else None

            # Release Event
            logging.info(f"Extracting Gas Release for EPA ID {epa_id}")
            accident["Gas Release"] = re.search(r"Gas Release:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Gas Release:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Liquid Spill/Evaporation for EPA ID {epa_id}")
            accident["Liquid Spill/Evaporation"] = re.search(r"Liquid Spill/Evaporation:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Liquid Spill/Evaporation:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Fire for EPA ID {epa_id}")
            accident["Fire"] = re.search(r"Fire:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Fire:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Explosion for EPA ID {epa_id}")
            accident["Explosion"] = re.search(r"Explosion:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Explosion:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Uncontrolled/Runaway Reaction for EPA ID {epa_id}")
            accident["Uncontrolled/Runaway Reaction"] = re.search(r"Uncontrolled/Runaway Reaction:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Uncontrolled/Runaway Reaction:", block_text, re.IGNORECASE) else None

            # Release Source
            logging.info(f"Extracting Storage Vessel for EPA ID {epa_id}")
            accident["Storage Vessel"] = re.search(r"Storage Vessel:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Storage Vessel:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Piping for EPA ID {epa_id}")
            accident["Piping"] = re.search(r"Piping:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Piping:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Process Vessel for EPA ID {epa_id}")
            accident["Process Vessel"] = re.search(r"Process Vessel:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Process Vessel:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Transfer Hose for EPA ID {epa_id}")
            accident["Transfer Hose"] = re.search(r"Transfer Hose:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Transfer Hose:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Valve for EPA ID {epa_id}")
            accident["Valve"] = re.search(r"Valve:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Valve:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Pump for EPA ID {epa_id}")
            accident["Pump"] = re.search(r"Pump:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Pump:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Joint for EPA ID {epa_id}")
            accident["Joint"] = re.search(r"Joint:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Joint:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Other Release Source for EPA ID {epa_id}")
            other_source_match = re.search(r"Other Release Source:\s*(.*)", block_text, re.IGNORECASE)
            accident["Other Release Source"] = other_source_match.group(1).strip() if other_source_match else None

            # Weather Conditions
            logging.info(f"Extracting Wind Speed for EPA ID {epa_id}")
            wind_speed_match = re.search(r"Wind Speed:\s*(\d+\.?\d*)", block_text, re.IGNORECASE)
            accident["Wind Speed"] = wind_speed_match.group(1) if wind_speed_match else None
            
            logging.info(f"Extracting Wind Speed Units for EPA ID {epa_id}")
            units_match = re.search(r"Units:\s*(.*)", block_text, re.IGNORECASE)
            accident["Wind Speed Units"] = units_match.group(1).strip() if units_match else None
            
            logging.info(f"Extracting Wind Direction for EPA ID {epa_id}")
            direction_match = re.search(r"Direction:\s*(.*)", block_text, re.IGNORECASE)
            accident["Wind Direction"] = direction_match.group(1).strip() if direction_match else None
            
            logging.info(f"Extracting Temperature for EPA ID {epa_id}")
            temp_match = re.search(r"Temperature:\s*(\d+)", block_text, re.IGNORECASE)
            accident["Temperature"] = temp_match.group(1) if temp_match else None
            
            logging.info(f"Extracting Atmospheric Stability Class for EPA ID {epa_id}")
            stability_match = re.search(r"Atmospheric Stability Class:\s*([A-Z])", block_text, re.IGNORECASE)
            accident["Atmospheric Stability Class"] = stability_match.group(1) if stability_match else None
            
            logging.info(f"Extracting Precipitation Present for EPA ID {epa_id}")
            precip_match = re.search(r"Precipitation Present:\s*(Yes|No|N/A)", block_text, re.IGNORECASE)
            accident["Precipitation Present"] = precip_match.group(1) if precip_match else None
            
            logging.info(f"Extracting Unknown Weather Conditions for EPA ID {epa_id}")
            unknown_weather_match = re.search(r"Unknown Weather Conditions:\s*(Yes|No|N/A)", block_text, re.IGNORECASE)
            accident["Unknown Weather Conditions"] = unknown_weather_match.group(1) if unknown_weather_match else None

            # On-Site Impacts
            logging.info(f"Extracting Employee/Contractor Deaths for EPA ID {epa_id}")
            match = re.search(r"Employee or Contractor Deaths:\s*(\d+|N/A)", block_text, re.IGNORECASE)
            accident["Employee/Contractor Deaths"] = match.group(1) if match and re.search(r"Employee or Contractor Deaths:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Public Responder Deaths for EPA ID {epa_id}")
            match = re.search(r"Public Responder Deaths:\s*(\d+|N/A)", block_text, re.IGNORECASE)
            accident["Public Responder Deaths"] = match.group(1) if match and re.search(r"Public Responder Deaths:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Public Deaths for EPA ID {epa_id}")
            match = re.search(r"Public Deaths:\s*(\d+|N/A)", block_text, re.IGNORECASE)
            accident["Public Deaths"] = match.group(1) if match and re.search(r"Public Deaths:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Employee/Contractor Injuries for EPA ID {epa_id}")
            match = re.search(r"Employee or Contractor Injuries:\s*(\d+|N/A)", block_text, re.IGNORECASE)
            accident["Employee/Contractor Injuries"] = match.group(1) if match and re.search(r"Employee or Contractor Injuries:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Public Responder Injuries for EPA ID {epa_id}")
            match = re.search(r"Public Responder Injuries:\s*(\d+|N/A)", block_text, re.IGNORECASE)
            accident["Public Responder Injuries"] = match.group(1) if match and re.search(r"Public Responder Injuries:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Public Injuries for EPA ID {epa_id}")
            match = re.search(r"Public Injuries:\s*(\d+|N/A)", block_text, re.IGNORECASE)
            accident["Public Injuries"] = match.group(1) if match and re.search(r"Public Injuries:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting On-Site Property Damage for EPA ID {epa_id}")
            match = re.search(r"On-Site Property Damage \(\$\):\s*(\d+|N/A)", block_text, re.IGNORECASE)
            accident["On-Site Property Damage ($)"] = match.group(1) if match else None

            # Off-Site Impacts
            logging.info(f"Extracting Off-Site Deaths for EPA ID {epa_id}")
            match = re.search(r"Deaths:\s*(\d+|N/A)", block_text, re.IGNORECASE)
            accident["Off-Site Deaths"] = match.group(1) if match and re.search(r"Deaths:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Off-Site Hospitalizations for EPA ID {epa_id}")
            match = re.search(r"Hospitalizations:\s*(\d+|N/A)", block_text, re.IGNORECASE)
            accident["Off-Site Hospitalizations"] = match.group(1) if match and re.search(r"Hospitalizations:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Off-Site Public Deaths for EPA ID {epa_id}")
            match = re.search(r"Public Deaths:\s*(\d+|N/A)", block_text, re.IGNORECASE)
            accident["Off-Site Public Deaths"] = match.group(1) if match and re.search(r"Public Deaths:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Off-Site Other Medical Treatments for EPA ID {epa_id}")
            match = re.search(r"Other Medical Treatments:\s*(\d+|N/A)", block_text, re.IGNORECASE)
            accident["Off-Site Other Medical Treatments"] = match.group(1) if match and re.search(r"Other Medical Treatments:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Evacuated for EPA ID {epa_id}")
            match = re.search(r"Evacuated:\s*(\d+|N/A)", block_text, re.IGNORECASE)
            accident["Evacuated"] = match.group(1) if match and re.search(r"Evacuated:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Sheltered-in-Place for EPA ID {epa_id}")
            match = re.search(r"Sheltered-in-Place:\s*(\d+|N/A)", block_text, re.IGNORECASE)
            accident["Sheltered-in-Place"] = match.group(1) if match and re.search(r"Sheltered-in-Place:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Off-Site Property Damage for EPA ID {epa_id}")
            match = re.search(r"Off-Site Property Damage \(\$\):\s*(\d+|N/A)", block_text, re.IGNORECASE)
            accident["Off-Site Property Damage ($)"] = match.group(1) if match else None

            # Environmental Damage
            logging.info(f"Extracting Fish or Animal Kills for EPA ID {epa_id}")
            accident["Fish or Animal Kills"] = re.search(r"Fish or Animal Kills:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Fish or Animal Kills:", block_text, re.IGNORECASE) else None

            logging.info(f"Extracting Tree, Lawn, Shrub, or Crop Damage for EPA ID {epa_id}")
            accident["Tree, Lawn, Shrub, or Crop Damage"] = re.search(r"Tree, Lawn, Shrub, or Crop Damage:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Tree, Lawn, Shrub, or Crop Damage:", block_text, re.IGNORECASE) else None

            logging.info(f"Extracting Water Contamination for EPA ID {epa_id}")
            accident["Water Contamination"] = re.search(r"Water Contamination:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Water Contamination:", block_text, re.IGNORECASE) else None

            logging.info(f"Extracting Soil Contamination for EPA ID {epa_id}")
            accident["Soil Contamination"] = re.search(r"Soil Contamination:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Soil Contamination:", block_text, re.IGNORECASE) else None

            logging.info(f"Extracting Other Environmental Damage for EPA ID {epa_id}")
            other_env_damage_match = re.search(r"Other Environmental Damage:\s*(.*)", block_text, re.IGNORECASE)
            accident["Other Environmental Damage"] = other_env_damage_match.group(1).strip() if other_env_damage_match else None

            # Contributing Factors
            logging.info(f"Extracting Contributing - Equipment Failure for EPA ID {epa_id}")
            accident["Contributing - Equipment Failure"] = re.search(r"Equipment Failure:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Equipment Failure:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Contributing - Human Error for EPA ID {epa_id}")
            accident["Contributing - Human Error"] = re.search(r"Human Error:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Human Error:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Contributing - Improper Procedures for EPA ID {epa_id}")
            accident["Contributing - Improper Procedures"] = re.search(r"Improper Procedures:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Improper Procedures:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Contributing - Overpressurization for EPA ID {epa_id}")
            accident["Contributing - Overpressurization"] = re.search(r"Overpressurization:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Overpressurization:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Contributing - Upset Condition for EPA ID {epa_id}")
            accident["Contributing - Upset Condition"] = re.search(r"Upset Condition:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Upset Condition:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Contributing - By-Pass Condition for EPA ID {epa_id}")
            accident["Contributing - By-Pass Condition"] = re.search(r"By-Pass Condition:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"By-Pass Condition:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Contributing - Maintenance Activity/Inactivity for EPA ID {epa_id}")
            accident["Contributing - Maintenance Activity/Inactivity"] = re.search(r"Maintenance Activity/Inactivity:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Maintenance Activity/Inactivity:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Contributing - Process Design Failure for EPA ID {epa_id}")
            accident["Contributing - Process Design Failure"] = re.search(r"Process Design Failure:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Process Design Failure:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Contributing - Unsuitable Equipment for EPA ID {epa_id}")
            accident["Contributing - Unsuitable Equipment"] = re.search(r"Unsuitable Equipment:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Unsuitable Equipment:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Contributing - Unusual Weather Condition for EPA ID {epa_id}")
            accident["Contributing - Unusual Weather Condition"] = re.search(r"Unusual Weather Condition:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Unusual Weather Condition:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Contributing - Management Error for EPA ID {epa_id}")
            accident["Contributing - Management Error"] = re.search(r"Management Error:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Management Error:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Contributing - Other for EPA ID {epa_id}")
            other_contrib_match = re.search(r"Other Contributing Factor:\s*(.*)", block_text, re.IGNORECASE)
            accident["Contributing - Other"] = other_contrib_match.group(1).strip() if other_contrib_match else None

            # Off-Site Responders
            logging.info(f"Extracting Off-Site Responders Notified for EPA ID {epa_id}")
            responders_match = re.search(r"Off-Site Responders Notified:\s*(Yes|No, not notified|Notified Only)", block_text, re.IGNORECASE)
            accident["Off-Site Responders Notified"] = responders_match.group(1) if responders_match else None

            # Changes Introduced
            logging.info(f"Extracting Change - Improved/Upgraded Equipment for EPA ID {epa_id}")
            accident["Change - Improved/Upgraded Equipment"] = re.search(r"Improved or Upgraded Equipment:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Improved or Upgraded Equipment:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Change - Revised Maintenance for EPA ID {epa_id}")
            accident["Change - Revised Maintenance"] = re.search(r"Revised Maintenance:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Revised Maintenance:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Change - Revised Training for EPA ID {epa_id}")
            accident["Change - Revised Training"] = re.search(r"Revised Training:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Revised Training:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Change - Revised Operating Procedures for EPA ID {epa_id}")
            accident["Change - Revised Operating Procedures"] = re.search(r"Revised Operating Procedures:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Revised Operating Procedures:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Change - New Process Controls for EPA ID {epa_id}")
            accident["Change - New Process Controls"] = re.search(r"New Process Controls:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"New Process Controls:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Change - New Mitigation Systems for EPA ID {epa_id}")
            accident["Change - New Mitigation Systems"] = re.search(r"New Mitigation Systems:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"New Mitigation Systems:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Change - Revised Emergency Response Plan for EPA ID {epa_id}")
            accident["Change - Revised Emergency Response Plan"] = re.search(r"Revised Emergency Response Plan:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Revised Emergency Response Plan:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Change - Changed Process for EPA ID {epa_id}")
            accident["Change - Changed Process"] = re.search(r"Changed Process:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Changed Process:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Change - Reduced Inventory for EPA ID {epa_id}")
            accident["Change - Reduced Inventory"] = re.search(r"Reduced Inventory:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"Reduced Inventory:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Change - None for EPA ID {epa_id}")
            accident["Change - None"] = re.search(r"None:\s*(Yes|No|N/A)", block_text, re.IGNORECASE).group(1) if re.search(r"None:", block_text, re.IGNORECASE) else None
            
            logging.info(f"Extracting Change - Other for EPA ID {epa_id}")
            other_change_match = re.search(r"Other Changes Introduced:\s*(.*)", block_text, re.IGNORECASE)
            accident["Change - Other"] = other_change_match.group(1).strip() if other_change_match else None

            accidents.append(accident)
            logging.info(f"Parsed accident {accident_count} for EPA ID {epa_id}: {accident}")

        except Exception as e:
            logging.error(f"Error in parse_accident_history for EPA ID {epa_id}: {e}")
            raise

    return {
        "Has Accident": "Yes" if accident_count > 0 else "No",
        "Accident Count": accident_count,
        "Accident Details": accidents if accidents else None
    }

# Process the single PDF
try:
    with fitz.open(pdf_file) as doc:
        epa_id = extract_epa_facility_id_from_filename(pdf_file)
        if not epa_id:
            raise ValueError(f"Invalid EPA Facility ID from filename: {pdf_file}")

        text_before_section_6 = ""
        section_6_text = ""
        in_section_6 = False
        for i in range(len(doc)):
            page_text = doc[i].get_text("text")
            if not page_text:
                logging.error(f"No text extracted from page {i+1} of {pdf_file}")
                continue
            
            if re.search(r"Section 6\. Accident History", page_text, re.IGNORECASE):
                in_section_6 = True
                section_6_start = page_text.find("Section 6. Accident History")
                section_6_text += page_text[section_6_start:] + "\n"
                continue
            
            if in_section_6:
                if re.search(r"Section 7\.|Section 8\.|Section 9\.", page_text, re.IGNORECASE):
                    matches = [(page_text.find(s), s) for s in ["Section 7.", "Section 8.", "Section 9."] if page_text.find(s) != -1]
                    if matches:
                        earliest_pos, earliest_section = min(matches, key=lambda x: x[0])
                        section_6_text += page_text[:earliest_pos]
                        logging.info(f"Section 6 ended at {earliest_section} for EPA ID {epa_id}")
                        break
                    else:
                        section_6_text += page_text + "\n"
                else:
                    section_6_text += page_text + "\n"
            else:
                text_before_section_6 += page_text + "\n"

        naics_code = extract_process_naics(text_before_section_6)
        accident_data = parse_accident_history(section_6_text, epa_id)

        results = []
        if accident_data["Has Accident"] == "Yes":
            for accident in accident_data["Accident Details"]:
                row = {
                    "EPA Facility ID": epa_id,
                    "Has Accident": "Yes",
                    "Accident Count": accident_data["Accident Count"],
                    "NAICS Code": naics_code,
                    **accident
                }
                results.append(row)

        pd.DataFrame(results).to_csv(output_csv, index=False)
        logging.info(f"Processed {pdf_file} (EPA ID: {epa_id}), accidents found: {len(results)}")

except Exception as e:
    with open(error_log, "a") as log_file:
        log_file.write(f"Error processing {pdf_file}: {e} at {time.ctime()}\n")
    logging.error(f"Error processing {pdf_file}: {e}")

print(f"Processing of {pdf_file} completed. Check {output_csv} for results.")
import sqlite3
import csv
import logging
import os

# Set up logging
logging.basicConfig(filename="facility_accident_detail_log.txt", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Define paths
db_path = r""
csv_path = r"accident_history_detailed.csv"

# Create or connect to the database
try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Drop the table if it exists (optional, remove if not needed)
    cursor.execute("DROP TABLE IF EXISTS rmp_accident_history_detail;")
    logging.info("Dropped existing rmp_accident_history_detail table (if it existed).")

    # Create the rmp_accident_history_detail table with 73 columns
    create_table_query = """
    CREATE TABLE IF NOT EXISTS rmp_accident_history_detail (
        EPA_Facility_ID TEXT,
        Accident_History_ID TEXT,
        Facility_Accident_ID TEXT PRIMARY KEY,
        Date_of_Accident TEXT,
        Time_Accident_Began TEXT,
        NAICS_Code_of_Process_Involved TEXT,
        NAICS_Description TEXT,
        Release_Duration_Hours TEXT,
        Release_Duration_Minutes TEXT,
        Gas_Release TEXT,
        Liquid_Spill_Evaporation TEXT,
        Fire TEXT,
        Explosion TEXT,
        Uncontrolled_Runaway_Reaction TEXT,
        Storage_Vessel TEXT,
        Piping TEXT,
        Process_Vessel TEXT,
        Transfer_Hose TEXT,
        Valve TEXT,
        Pump TEXT,
        Joint TEXT,
        Other_Release_Source TEXT,
        Wind_Speed TEXT,
        Wind_Speed_Units TEXT,
        Wind_Direction TEXT,
        Temperature TEXT,
        Atmospheric_Stability_Class TEXT,
        Precipitation_Present TEXT,
        Unknown_Weather_Conditions TEXT,
        Employee_Contractor_Deaths TEXT,
        Public_Responder_Deaths TEXT,
        Public_Deaths TEXT,
        Employee_Contractor_Injuries TEXT,
        Public_Responder_Injuries TEXT,
        Public_Injuries TEXT,
        On_Site_Property_Damage_Dollar INTEGER,
        Off_Site_Deaths TEXT,
        Off_Site_Hospitalizations TEXT,
        Off_Site_Public_Deaths TEXT,
        Off_Site_Other_Medical_Treatments TEXT,
        Evacuated TEXT,
        Sheltered_in_Place TEXT,
        Off_Site_Property_Damage_Dollar INTEGER,
        Fish_or_Animal_Kills TEXT,
        Tree_Lawn_Shrub_or_Crop_Damage TEXT,
        Water_Contamination TEXT,
        Soil_Contamination TEXT,
        Other_Environmental_Damage TEXT,
        Initiating_Event TEXT,
        Contributing_Equipment_Failure TEXT,
        Contributing_Human_Error TEXT,
        Contributing_Improper_Procedures TEXT,
        Contributing_Overpressurization TEXT,
        Contributing_Upset_Condition TEXT,
        Contributing_By_Pass_Condition TEXT,
        Contributing_Maintenance_Activity_Inactivity TEXT,
        Contributing_Process_Design_Failure TEXT,
        Contributing_Unsuitable_Equipment TEXT,
        Contributing_Unusual_Weather_Condition TEXT,
        Contributing_Management_Error TEXT,
        Contributing_Other TEXT,
        Off_Site_Responders_Notified TEXT,
        Change_Improved_Upgraded_Equipment TEXT,
        Change_Revised_Maintenance TEXT,
        Change_Revised_Training TEXT,
        Change_Revised_Operating_Procedures TEXT,
        Change_New_Process_Controls TEXT,
        Change_New_Mitigation_Systems TEXT,
        Change_Revised_Emergency_Response_Plan TEXT,
        Change_Changed_Process TEXT,
        Change_Reduced_Inventory TEXT,
        Change_None TEXT,
        Change_Other TEXT
    );
    """
    cursor.execute(create_table_query)
    logging.info("Created rmp_accident_history_detail table successfully.")

    # Read the CSV file using csv.DictReader
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        logging.info(f"CSV fields: {reader.fieldnames}")

        # Prepare the INSERT query with 73 columns
        insert_query = """
        INSERT INTO rmp_accident_history_detail (
            EPA_Facility_ID, Accident_History_ID, Facility_Accident_ID, Date_of_Accident, Time_Accident_Began,
            NAICS_Code_of_Process_Involved, NAICS_Description, Release_Duration_Hours, Release_Duration_Minutes,
            Gas_Release, Liquid_Spill_Evaporation, Fire, Explosion, Uncontrolled_Runaway_Reaction,
            Storage_Vessel, Piping, Process_Vessel, Transfer_Hose, Valve, Pump, Joint, Other_Release_Source,
            Wind_Speed, Wind_Speed_Units, Wind_Direction, Temperature, Atmospheric_Stability_Class,
            Precipitation_Present, Unknown_Weather_Conditions, Employee_Contractor_Deaths,
            Public_Responder_Deaths, Public_Deaths, Employee_Contractor_Injuries, Public_Responder_Injuries,
            Public_Injuries, On_Site_Property_Damage_Dollar, Off_Site_Deaths, Off_Site_Hospitalizations,
            Off_Site_Public_Deaths, Off_Site_Other_Medical_Treatments, Evacuated, Sheltered_in_Place,
            Off_Site_Property_Damage_Dollar, Fish_or_Animal_Kills, Tree_Lawn_Shrub_or_Crop_Damage,
            Water_Contamination, Soil_Contamination, Other_Environmental_Damage, Initiating_Event,
            Contributing_Equipment_Failure, Contributing_Human_Error, Contributing_Improper_Procedures,
            Contributing_Overpressurization, Contributing_Upset_Condition, Contributing_By_Pass_Condition,
            Contributing_Maintenance_Activity_Inactivity, Contributing_Process_Design_Failure,
            Contributing_Unsuitable_Equipment, Contributing_Unusual_Weather_Condition,
            Contributing_Management_Error, Contributing_Other, Off_Site_Responders_Notified,
            Change_Improved_Upgraded_Equipment, Change_Revised_Maintenance, Change_Revised_Training,
            Change_Revised_Operating_Procedures, Change_New_Process_Controls, Change_New_Mitigation_Systems,
            Change_Revised_Emergency_Response_Plan, Change_Changed_Process, Change_Reduced_Inventory,
            Change_None, Change_Other
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """

        # Collect rows to insert after modifying Accident_History_ID
        rows_to_insert = []
        for row in reader:
            # Modify Accident_History_ID to prepend "Accident"
            accident_history_id = row["Accident History ID"]
            row["Accident History ID"] = f"Accident {accident_history_id}" if accident_history_id else None

            # Create a tuple of values in the order of the INSERT query, excluding dropped columns
            row_values = (
                row["EPA Facility ID"],
                row["Accident History ID"],
                row["Facility Accident ID"],
                row["Date of Accident"],
                row["Time Accident Began"],
                row["NAICS Code of Process Involved"],
                row["NAICS Description"],
                row["Release Duration (Hours)"],
                row["Release Duration (Minutes)"],
                row["Gas Release"],
                row["Liquid Spill/Evaporation"],
                row["Fire"],
                row["Explosion"],
                row["Uncontrolled/Runaway Reaction"],
                row["Storage Vessel"],
                row["Piping"],
                row["Process Vessel"],
                row["Transfer Hose"],
                row["Valve"],
                row["Pump"],
                row["Joint"],
                row["Other Release Source"],
                row["Wind Speed"],
                row["Wind Speed Units"],
                row["Wind Direction"],
                row["Temperature"],
                row["Atmospheric Stability Class"],
                row["Precipitation Present"],
                row["Unknown Weather Conditions"],
                row["Employee/Contractor Deaths"],
                row["Public Responder Deaths"],
                row["Public Deaths"],
                row["Employee/Contractor Injuries"],
                row["Public Responder Injuries"],
                row["Public Injuries"],
                row["On-Site Property Damage ($)"],
                row["Off-Site Deaths"],
                row["Off-Site Hospitalizations"],
                row["Off-Site Public Deaths"],
                row["Off-Site Other Medical Treatments"],
                row["Evacuated"],
                row["Sheltered-in-Place"],
                row["Off-Site Property Damage ($)"],
                row["Fish or Animal Kills"],
                row["Tree, Lawn, Shrub, or Crop Damage"],
                row["Water Contamination"],
                row["Soil Contamination"],
                row["Other Environmental Damage"],
                row["Initiating Event"],
                row["Contributing - Equipment Failure"],
                row["Contributing - Human Error"],
                row["Contributing - Improper Procedures"],
                row["Contributing - Overpressurization"],
                row["Contributing - Upset Condition"],
                row["Contributing - By-Pass Condition"],
                row["Contributing - Maintenance Activity/Inactivity"],
                row["Contributing - Process Design Failure"],
                row["Contributing - Unsuitable Equipment"],
                row["Contributing - Unusual Weather Condition"],
                row["Contributing - Management Error"],
                row["Contributing - Other"],
                row["Off-Site Responders Notified"],
                row["Change - Improved/Upgraded Equipment"],
                row["Change - Revised Maintenance"],
                row["Change - Revised Training"],
                row["Change - Revised Operating Procedures"],
                row["Change - New Process Controls"],
                row["Change - New Mitigation Systems"],
                row["Change - Revised Emergency Response Plan"],
                row["Change - Changed Process"],
                row["Change - Reduced Inventory"],
                row["Change - None"],
                row["Change - Other"]
            )
            rows_to_insert.append(row_values)

        # Insert all rows
        cursor.executemany(insert_query, rows_to_insert)
        conn.commit()
        logging.info(f"Inserted {len(rows_to_insert)} rows into rmp_accident_history_detail table.")

    # Verify the number of rows
    cursor.execute("SELECT COUNT(*) FROM rmp_accident_history_detail;")
    row_count = cursor.fetchone()[0]
    logging.info(f"Verified {row_count} rows in rmp_accident_history_detail table.")
    if row_count == 1554:
        print("Successfully inserted 1554 rows into rmp_accident_history_detail table.")
    else:
        print(f"Warning: Expected 1554 rows, but found {row_count} rows.")

except sqlite3.Error as e:
    logging.error(f"SQLite error: {e}")
    print(f"Error: Failed to create table or insert data. Check {db_path} and log.")
except Exception as e:
    logging.error(f"General error: {e}")
    print(f"Error: An unexpected issue occurred. Check log.")
finally:
    if conn:
        conn.close()
        logging.info("Database connection closed.")
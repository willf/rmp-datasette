# -*- coding: utf-8 -*-
"""
Created on Fri May  9 12:13:35 2025

@author: MOGIC
"""
import sqlite3
import pandas as pd
import os
from datetime import datetime

# Define the database file and CSV file paths
db_file = r"C:\MS Data Science - WMU\EDGI\datasette-spatialite\risk-management-plans.db"
csv_dir = r"C:\MS Data Science - WMU\EDGI\rmp-datasette"  # Adjust this path as needed
csv_files = {
    "rmp_facility": os.path.join(csv_dir, "rmp_facility.csv"),
    "rmp_chemical": os.path.join(csv_dir, "rmp_chemical.csv"),
    "rmp_facility_chemicals": os.path.join(csv_dir, "rmp_facility_chemicals.csv"),
    "rmp_naics": os.path.join(csv_dir, "rmp_naics.csv"),
    "rmp_facility_naics": os.path.join(csv_dir, "rmp_facility_naics.csv"),
    "rmp_facility_accidents": os.path.join(csv_dir, "rmp_facility_accidents.csv"),
    "rmp_accident_chemicals": os.path.join(csv_dir, "rmp_accident_chemicals.csv")
}

# Function to convert date to Month Year format
def convert_to_mm_yyyy(date_str):
    if pd.isna(date_str) or not date_str:  # Handle NaN or empty strings
        return None
    try:
        # Handle various input formats (e.g., 6/1/2004, 2004-06-01, etc.)
        date_obj = datetime.strptime(str(date_str).strip(), '%m/%d/%Y')
        return date_obj.strftime('%B %Y')
    except ValueError:
        try:
            # Try an alternative format if the first fails (e.g., YYYY-MM-DD)
            date_obj = datetime.strptime(str(date_str).strip(), '%Y-%m-%d')
            return date_obj.strftime('%B %Y')
        except ValueError:
            # Return None if conversion fails
            print(f"Warning: Could not convert date '{date_str}' to Month Year format, returning None.")
            return None

# Create a connection to the SQLite database
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Enable foreign key constraints
cursor.execute("PRAGMA foreign_keys = ON;")

# Create indexes to improve subquery performance
try:
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_rmp_facility_naics_facility_id ON rmp_facility_naics(facility_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_rmp_facility_chemicals_facility_id ON rmp_facility_chemicals(facility_id);")
    print("Created indexes on facility_id for performance.")
except sqlite3.Error as e:
    print(f"Error creating indexes: {e}")

# Create tables
cursor.executescript("""
    DROP TABLE IF EXISTS rmp_facility;
    CREATE TABLE rmp_facility (
        epa_facility_id TEXT PRIMARY KEY,
        report TEXT,
        facility_name TEXT,
        facility_address TEXT,
        city TEXT,
        state TEXT,
        county TEXT,
        zip TEXT,
        facility_duns TEXT,
        latitude REAL,
        longitude REAL,
        receipt_date TEXT,
        report_created_date TEXT
    );

    DROP TABLE IF EXISTS rmp_chemical;
    CREATE TABLE rmp_chemical (
        chemical_id INTEGER PRIMARY KEY,
        chemical_name TEXT,
        cas_number TEXT,
        flammable_toxic TEXT
    );

    DROP TABLE IF EXISTS rmp_facility_chemicals;
    CREATE TABLE rmp_facility_chemicals (
        facility_chemical_id INTEGER PRIMARY KEY,
        facility_id TEXT,
        chemical_id INTEGER,
        program_level TEXT,
        FOREIGN KEY (facility_id) REFERENCES rmp_facility(epa_facility_id),
        FOREIGN KEY (chemical_id) REFERENCES rmp_chemical(chemical_id)
    );

    DROP TABLE IF EXISTS rmp_naics;
    CREATE TABLE rmp_naics (
        naics_code TEXT PRIMARY KEY,
        naics_description TEXT
    );

    DROP TABLE IF EXISTS rmp_facility_naics;
    CREATE TABLE rmp_facility_naics (
        facility_naics_id INTEGER PRIMARY KEY,
        facility_id TEXT,
        naics_code TEXT,
        FOREIGN KEY (facility_id) REFERENCES rmp_facility(epa_facility_id),
        FOREIGN KEY (naics_code) REFERENCES rmp_naics(naics_code)
    );

    DROP TABLE IF EXISTS rmp_facility_accidents;
    CREATE TABLE rmp_facility_accidents (
        facility_accident_id TEXT PRIMARY KEY,
        accident_id TEXT,
        facility_id TEXT,
        date_of_accident TEXT,
        time_accident_began TEXT,
        release_duration TEXT,
        naics_code TEXT,
        FOREIGN KEY (facility_id) REFERENCES rmp_facility(epa_facility_id),
        FOREIGN KEY (naics_code) REFERENCES rmp_naics(naics_code)
    );

    DROP TABLE IF EXISTS rmp_accident_chemicals;
    CREATE TABLE rmp_accident_chemicals (
        accident_chemical_id INTEGER PRIMARY KEY,
        facility_accident_chemical_id TEXT,
        facility_accident_id TEXT,
        quantity_released_lbs TEXT,
        percent_weight TEXT,
        chemical_id INTEGER,
        FOREIGN KEY (facility_accident_id) REFERENCES rmp_facility_accidents(facility_accident_id),
        FOREIGN KEY (chemical_id) REFERENCES rmp_chemical(chemical_id)
    );
""")

# Function to import CSV data into a table with type enforcement and column renaming
def import_csv_to_table(csv_file, table_name):
    if os.path.exists(csv_file):
        # Define dtypes for columns to enforce TEXT for facility_id and other appropriate types
        dtypes = {}
        if table_name in ["rmp_facility_chemicals", "rmp_facility_naics", "rmp_facility_accidents"]:
            dtypes["facility_id"] = str  # Force facility_id to be read as a string
        if table_name == "rmp_facility":
            dtypes["EPA Facility ID"] = str  # Force EPA Facility ID to be read as a string
            dtypes["Latitude"] = float  # Force Latitude to be read as a float
            dtypes["Longitude"] = float  # Force Longitude to be read as a float
        if table_name in ["rmp_facility_naics", "rmp_facility_accidents"]:
            dtypes["naics_code"] = str  # Ensure naics_code is also a string
        if table_name == "rmp_facility_accidents":
            dtypes["facility_accident_id"] = str
        if table_name == "rmp_accident_chemicals":
            dtypes["facility_accident_id"] = str
            dtypes["facility_accident_chemical_id"] = str
            dtypes["chemical_id"] = int
        if table_name == "rmp_chemical":
            dtypes["chemical_id"] = int

        # Read the CSV with specified dtypes
        df = pd.read_csv(csv_file, dtype=dtypes)

        # Preprocess IDs to ensure consistency
        if table_name == "rmp_facility":
            df["EPA Facility ID"] = df["EPA Facility ID"].astype(str).str.strip()
        if table_name in ["rmp_facility_naics", "rmp_facility_chemicals", "rmp_facility_accidents"]:
            df["facility_id"] = df["facility_id"].astype(str).str.strip()

        # Rename columns and preprocess data
        if table_name == "rmp_facility":
            # Drop Chemicals and NAICS_Names columns
            df = df.drop(columns=["Chemicals", "NAICS_Names"], errors='ignore')
            
            # Convert Report Received Date to Month Year format
            df["Report Received Date"] = df["Report Received Date"].apply(convert_to_mm_yyyy)
            
            # Rename columns to match the database schema
            df = df.rename(columns={
                "EPA Facility ID": "epa_facility_id",
                "Report": "report",
                "Facility Name": "facility_name",
                "Facility Address": "facility_address",
                "City": "city",
                "State": "state",
                "County": "county",
                "Zip": "zip",
                "Facility DUNS": "facility_duns",
                "Latitude": "latitude",
                "Longitude": "longitude",
                "Report Received Date": "receipt_date",
                "Report Created Date": "report_created_date"
            })

        # Import the data into the table
        df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"Imported data into {table_name} from {csv_file}")
    else:
        print(f"Warning: {csv_file} not found, skipping import for {table_name}")

# Import data from CSV files
for table_name, csv_file in csv_files.items():
    import_csv_to_table(csv_file, table_name)

print(f"Database {db_file} created and populated successfully.")

# Creating facility_view table.
cursor.execute("DROP TABLE IF EXISTS facility_view;")

cursor.execute("""
CREATE TABLE facility_view (
    epa_facility_id TEXT PRIMARY KEY,
    report TEXT,
    facility_name TEXT,
    facility_address TEXT,
    city TEXT,
    state TEXT,
    county TEXT,
    zip TEXT,
    facility_duns TEXT,
    receipt_date TEXT,
    report_created_date TEXT,
    naics_codes TEXT,
    chemical_names TEXT,
    latitude REAL,
    longitude REAL
);
""")

# 2. Insert data into the table
cursor.execute("""
INSERT INTO facility_view
SELECT 
    rmp_facility.epa_facility_id,
    rmp_facility.report,
    rmp_facility.facility_name,
    rmp_facility.facility_address,
    rmp_facility.city,
    rmp_facility.state,
    rmp_facility.county,
    rmp_facility.zip,
    rmp_facility.facility_duns,
    rmp_facility.receipt_date,
    rmp_facility.report_created_date,
    (
        SELECT GROUP_CONCAT(DISTINCT naics_code)
        FROM rmp_facility_naics
        WHERE rmp_facility_naics.facility_id = rmp_facility.epa_facility_id
    ) AS naics_codes,
    (
        SELECT GROUP_CONCAT(chemical_name, ', ')
        FROM (
            SELECT DISTINCT rc.chemical_name
            FROM rmp_facility_chemicals rfc
            JOIN rmp_chemical rc ON rfc.chemical_id = rc.chemical_id
            WHERE rfc.facility_id = rmp_facility.epa_facility_id
        )
    ) AS chemical_names,
    rmp_facility.latitude,
    rmp_facility.longitude
FROM rmp_facility
GROUP BY 
    rmp_facility.epa_facility_id,
    rmp_facility.report,
    rmp_facility.facility_name,
    rmp_facility.facility_address,
    rmp_facility.city,
    rmp_facility.state,
    rmp_facility.county,
    rmp_facility.zip,
    rmp_facility.facility_duns,
    rmp_facility.receipt_date,
    rmp_facility.report_created_date;
""")

# Commit changes and close the connection
conn.commit()

# Create the facility_accidents_view table with primary key
cursor.execute("DROP TABLE IF EXISTS facility_accidents_view;")

cursor.execute("""
CREATE TABLE facility_accidents_view (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    facility_accident_id TEXT,
    accident_id TEXT,
    facility_id TEXT,
    facility_name TEXT,
    facility_address TEXT,
    city TEXT,
    state TEXT,
    county TEXT,
    date_of_accident TEXT,
    time_accident_began TEXT,
    release_duration TEXT,
    naics_code TEXT,
    chemical_names TEXT,
    latitude REAL,
    longitude REAL
);
""")

# 2. Insert data into the table (omit 'id' to let SQLite auto-generate)
cursor.execute("""
INSERT INTO facility_accidents_view (
    facility_accident_id,
    accident_id,
    facility_id,
    facility_name,
    facility_address,
    city,
    state,
    county,
    date_of_accident,
    time_accident_began,
    release_duration,
    naics_code,
    chemical_names,
    latitude,
    longitude
)
SELECT 
    rfa.facility_accident_id,
    rfa.accident_id,
    rfa.facility_id,
    rf.facility_name,
    rf.facility_address,
    rf.city,
    rf.state,
    rf.county,
    rfa.date_of_accident,
    rfa.time_accident_began,
    rfa.release_duration,
    rfa.naics_code,
    (
        SELECT GROUP_CONCAT(chemical_name, ', ')
        FROM (
            SELECT DISTINCT rc.chemical_name
            FROM rmp_facility_chemicals rfc
            JOIN rmp_chemical rc ON rfc.chemical_id = rc.chemical_id
            WHERE rfc.facility_id = rfa.facility_id
        )
    ) AS chemical_names,
    rf.latitude,
    rf.longitude
FROM rmp_facility_accidents rfa
JOIN rmp_facility rf ON rfa.facility_id = rf.epa_facility_id
GROUP BY 
    rfa.facility_accident_id,
    rfa.accident_id,
    rfa.facility_id,
    rf.facility_name,
    rf.facility_address,
    rf.city,
    rf.state,
    rf.county,
    rfa.date_of_accident,
    rfa.time_accident_began,
    rfa.release_duration,
    rfa.naics_code,
    rf.latitude,
    rf.longitude;
""")

conn.commit()
print("facility_accidents_view table with integer primary key created successfully.")

# Creating accident_chemicals_view
cursor.execute("DROP TABLE IF EXISTS accident_chemicals_view;")
# Create accident_chemicals_view
cursor.execute("""
CREATE TABLE accident_chemicals_view (
    accident_chemical_id INTEGER PRIMARY KEY,
    accident_id TEXT,
    facility_id TEXT,
    facility_name TEXT,
    facility_address TEXT,
    city TEXT,
    state TEXT,
    county TEXT,
    date_of_accident TEXT,
    chemical_name TEXT,
    quantity_released_lbs TEXT,
    percent_weight TEXT
);
""")

# 2. Insert data into the table with LEFT JOINs
cursor.execute("""
INSERT INTO accident_chemicals_view
SELECT 
    rac.accident_chemical_id,
    rfa.accident_id,
    rfa.facility_id,
    rf.facility_name,
    rf.facility_address,
    rf.city,
    rf.state,
    rf.county,
    rfa.date_of_accident,
    rc.chemical_name,
    rac.quantity_released_lbs,
    rac.percent_weight
FROM rmp_accident_chemicals rac
LEFT JOIN rmp_facility_accidents rfa ON rac.facility_accident_id = rfa.facility_accident_id
LEFT JOIN rmp_facility rf ON rfa.facility_id = rf.epa_facility_id
LEFT JOIN rmp_chemical rc ON rac.chemical_id = rc.chemical_id
GROUP BY 
    rac.accident_chemical_id,
    rfa.accident_id,
    rfa.facility_id,
    rf.facility_name,
    rf.facility_address,
    rf.city,
    rf.state,
    rf.county,
    rfa.date_of_accident,
    rc.chemical_name,
    rac.quantity_released_lbs,
    rac.percent_weight;
""")

conn.commit()
conn.close()
print("accident_chemicals_view table with primary key created successfully.")

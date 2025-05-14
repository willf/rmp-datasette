# -*- coding: utf-8 -*-
"""
Created on Fri May  9 12:13:35 2025

@author: MOGIC
"""
import sqlite3 

conn = sqlite3.connect(r"risk-management-plans.db")
cursor = conn.cursor()

# 1. Drop if exists, then create the table with schema and primary key
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

conn.commit()
print("facility_view table with primary key created successfully.")

# Create the facility_accidents_view table with schema and primary key
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


# Create the accident_chemicals_view table with schema and primary key
cursor.execute("DROP TABLE IF EXISTS accident_chemicals_view;")

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
print("accident_chemicals_view table with primary key created successfully.")

# Create the accident_chemicals_fts using FTS5 virtual table
cursor.execute("DROP TABLE IF EXISTS accident_chemicals_fts;")
cursor.execute("""
CREATE VIRTUAL TABLE accident_chemicals_fts USING fts5(
    accident_id,
    facility_id,
    facility_name,
    facility_address,
    city,
    state,
    county,
    date_of_accident,
    chemical_name,
    quantity_released_lbs,
    percent_weight,
    content='accident_chemicals_view',
    content_rowid='accident_chemical_id',
    tokenize='unicode61'
);
""")

# 2. Populate the FTS table with data from accident_chemicals_view
cursor.execute("""
INSERT INTO accident_chemicals_fts (
    rowid,
    accident_id,
    facility_id,
    facility_name,
    facility_address,
    city,
    state,
    county,
    date_of_accident,
    chemical_name,
    quantity_released_lbs,
    percent_weight
)
SELECT 
    accident_chemical_id,
    accident_id,
    facility_id,
    facility_name,
    facility_address,
    city,
    state,
    county,
    date_of_accident,
    chemical_name,
    quantity_released_lbs,
    percent_weight
FROM accident_chemicals_view;
""")

# 3. Optimize the FTS table
cursor.execute("INSERT INTO accident_chemicals_fts(accident_chemicals_fts) VALUES('optimize');")

conn.commit()
print("accident_chemicals_fts full-text search table created and populated successfully.")

# 1. Create the facility_accidents_fts using FTS5 virtual table
cursor.execute("DROP TABLE IF EXISTS facility_accidents_fts;")
cursor.execute("""
CREATE VIRTUAL TABLE facility_accidents_fts USING fts5(
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
    content='facility_accidents_view',
    content_rowid='id',
    tokenize='unicode61'
);
""")

# 2. Populate the FTS table
cursor.execute("""
INSERT INTO facility_accidents_fts (
    rowid,
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
    chemical_names
)
SELECT 
    id,
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
    chemical_names
FROM facility_accidents_view;
""")

# 3. Optimize the FTS table
cursor.execute("INSERT INTO facility_accidents_fts(facility_accidents_fts) VALUES('optimize');")

conn.commit()
conn.close()
print("facility_accidents_fts full-text search table created and populated successfully.")
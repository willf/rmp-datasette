# -*- coding: utf-8 -*-
"""
Created on Mon May 5 09:47:52 2025

@author: MOGIC
"""

import logging
import pdfplumber
import os
import re
import pandas as pd
from datetime import datetime

# Suppress warnings more aggressively
logging.getLogger("pdfplumber").setLevel(logging.ERROR)
logging.getLogger().setLevel(logging.ERROR)

# Define paths
base_dir = r"C:\MS Data Science - WMU\EDGI\datasette-spatialite\epa-risk-management-plans\reports"
output_dir = r"C:\MS Data Science - WMU\EDGI\datasette-spatialite"
intermediate_csv = os.path.join(output_dir, "rmp_dates_metadata_intermediate.csv")

# Regular expressions for date patterns
receipt_date_pattern = r"Receipt Date:\s*([A-Za-z]+)\s+(\d{4})"
footer_date_pattern = r"Data displayed is accurate as of 12:00 AM \(EST\)\s+([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})"

# Function to parse Receipt Date (e.g., "April 2020")
def parse_receipt_date(text):
    match = re.search(receipt_date_pattern, text)
    if match:
        month, year = match.groups()
        try:
            return datetime.strptime(f"{month} {year}", "%B %Y").date()
        except ValueError:
            return None
    return None

# Function to parse footer date (e.g., "Nov 29, 2024")
def parse_footer_date(text):
    match = re.search(footer_date_pattern, text)
    if match:
        month, day, year = match.groups()
        try:
            return datetime.strptime(f"{month} {day}, {year}", "%b %d, %Y").date()
        except ValueError:
            return None
    return None

# Function to extract dates from a PDF
def extract_dates(pdf_path):
    try:
        with pdfplumber.open(pdf_path) as pdf:
            first_page = pdf.pages[0]
            first_text = first_page.extract_text() or ""
            received_date = parse_receipt_date(first_text)

            last_page = pdf.pages[-1]
            last_text = last_page.extract_text() or ""
            created_date = parse_footer_date(last_text)

        return received_date, created_date
    except Exception as e:
        print(f"Error processing {pdf_path}: {e}")
        return None, None

# Load intermediate results if they exist
if os.path.exists(intermediate_csv):
    df = pd.read_csv(intermediate_csv)
    metadata = df.to_dict("records")
    processed_files = set(df["EPA Facility ID"].astype(str))
else:
    metadata = []
    processed_files = set()

# Iterate through state folders
total_files = sum(len([f for f in os.listdir(os.path.join(base_dir, state)) if f.endswith(".pdf")]) 
                  for state in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, state)))
current_file = 0

for state_folder in os.listdir(base_dir):
    state_path = os.path.join(base_dir, state_folder)
    if not os.path.isdir(state_path):
        continue

    for pdf_file in os.listdir(state_path):
        if not pdf_file.endswith(".pdf"):
            continue

        facility_id = pdf_file.replace(".pdf", "")
        if facility_id in processed_files:
            continue  # Skip already processed files

        pdf_path = os.path.join(state_path, pdf_file)
        current_file += 1
        print(f"Processing {current_file}/{total_files}: {state_folder}/{pdf_file}")

        # Extract dates
        received_date, created_date = extract_dates(pdf_path)

        # Append to metadata
        metadata.append({
            "EPA Facility ID": facility_id,
            "State": state_folder,
            "Report Received Date": received_date,
            "Report Created Date": created_date
        })

        # Save intermediate results every 100 files
        if current_file % 100 == 0:
            df = pd.DataFrame(metadata)
            df.to_csv(intermediate_csv, index=False)
            print(f"Saved intermediate CSV at {current_file} files")

# Final save
df = pd.DataFrame(metadata)
df.to_csv(os.path.join(output_dir, "rmp_dates_metadata.csv"), index=False)
print("Saved consolidated CSV to rmp_dates_metadata.csv")

# Save one CSV per state
for state in df["State"].unique():
    state_df = df[df["State"] == state]
    state_df.to_csv(os.path.join(output_dir, f"{state}_dates_metadata.csv"), index=False)
    print(f"Saved {state}_dates_metadata.csv")

denormalized_df = pd.read_csv(r"C:\MS Data Science - WMU\EDGI\rmp\denormalized_old.csv")

denormalized_df["EPA Facility ID"] = denormalized_df["EPA Facility ID"].astype(str)
df["EPA Facility ID"] = df["EPA Facility ID"].astype(str)

merged_df = pd.merge(denormalized_df, df, on="EPA Facility ID", how="left")

merged_df.count()

output_dir = r"C:\MS Data Science - WMU\EDGI\rmp"
merged_df.to_csv(os.path.join(output_dir, "rmp_dates_metadata_merged.csv"), index=False)

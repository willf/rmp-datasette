# EPA Risk Management Plans (rmp) screening tools project 📊

This repository contains data from the U.S. Environmental Protection Agency (EPA) Risk Management Plans (RMPs), detailing facilities handling hazardous substances, their chemicals, accidents, and NAICS codes. Extracted from 21,561 PDF reports stored in state subfolders (e.g., `reports/AK`, `reports/AL`), the data is processed into a SQLite database (`risk-management-plans.db`) and visualized using [Datasette](https://datasette.io/).

## Overview

The EPA mandates RMPs for facilities using hazardous substances to ensure safety. This project, part of the [Environmental Data & Governance Initiative (EDGI)](https://envirodatagov.org/), scrapes RMP PDF reports, structures the data, and makes it accessible via a user-friendly Datasette interface. Key data includes facility details, chemical inventories, accident histories, and industry classifications.

## Data Source

Data is sourced from the EPA's [RMP Search Tool](https://cdxapps.epa.gov/olem-rmp-pds/). The 21,561 PDFs, organized by state in the `reports` folder, were processed using:
- `scrap_pdf_rmp_reports_to_csv.py`: Extracts data into CSV files (`rmp_chemical.csv`, `rmp_facility_chemicals.csv`, etc.).
- `create_sqlite_rmp_db_from_csv.py`: Builds the `risk-management-plans.db` SQLite database with tables and views.

## Data Structure

The SQLite database includes:

| Table/View                | Description                                                                 |
|---------------------------|-----------------------------------------------------------------------------|
| `rmp_facility`            | Facility details (EPA ID, name, address, coordinates, report dates)         |
| `rmp_chemical`            | Unique chemicals (ID, name, CAS number, flammable/toxic)                    |
| `rmp_facility_chemicals`  | Links facilities to chemicals with program levels                           |
| `rmp_naics`               | NAICS codes and descriptions                                               |
| `rmp_facility_naics`      | Links facilities to NAICS codes                                            |
| `rmp_facility_accidents`  | Accident details (ID, date, time, release duration, NAICS code)             |
| `rmp_accident_chemicals`  | Chemicals in accidents (quantity released, percent weight)                  |
| `facility_view`           | Aggregated facility data with NAICS codes and chemical names                |
| `facility_accidents_view` | Accident data with facility details and chemical names                      |
| `accident_chemicals_view` | Chemicals released in accidents with facility and accident details          |

Views (`facility_view`, `facility_accidents_view`, `accident_chemicals_view`) are optimized for Datasette, with full-text search via `facility_fts`, `facility_accidents_fts`, and `accident_chemicals_fts`.

## Installation

To explore the data with Datasette:

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/edgi-govdata-archiving/epa-risk-management-plans.git
   cd epa-risk-management-plans

2. **Install Dependencies: Requires Python 3.8+**:
pip install datasette==0.65.1 sqlite-utils>=3.35 datasette-cluster-map datasette-render-markdown markupsafe

3. **Set Up the Database: Use the provided risk-management-plans.db. To regenerate from CSVs**:
python create_sqlite_rmp_db_from_csv.py

4. **Run Datasette** :
datasette risk-management-plans.db --metadata metadata.json 

Access at http://localhost:8001.

## Exploring the Data
Datasette offers:

- Facets: Filter by state, county, NAICS codes, chemical names, or accident dates.
- Search: Full-text search on facilities, accidents, and chemicals.
- Maps: Visualize facility locations with datasette-cluster-map.
- Markdown: Render report column as Markdown via datasette-render-markdown.
The metadata.json configures titles, descriptions, and facets for facility_view, facility_accidents_view, and accident_chemicals_view.

## Scripts
- scrap_pdf_rmp_reports_to_csv.py: Converts PDFs to CSVs, handling chemicals, NAICS, and accidents.
- create_sqlite_rmp_db_from_csv.py: Creates the SQLite database with tables and views.

## License
Data is licensed under the Open Data Commons Open Database License (ODbL).

## Contributing
Contributions are welcome! Submit issues or pull requests on GitHub. Contact the EDGI team via the issue tracker.

## Acknowledgments
Built by the EDGI team. Thanks to the EPA for the RMP data and the open-source community for tools like Datasette.
# EPA Risk Management Plans (rmp) Screening Tools Project 📊

This repository contains data from the U.S. Environmental Protection Agency (EPA) Risk Management Plans (RMPs), detailing facilities handling hazardous substances, their chemicals, accidents, and NAICS codes. Extracted from 21,561 PDF reports stored in state subfolders (e.g., `reports/AK`, `reports/AL`), the data is processed into a SQLite database (`risk-management-plans.db`) and visualized using [Datasette](https://datasette.io/).

## Overview

The EPA mandates RMPs for facilities using hazardous substances to ensure safety. This project, part of the [Environmental Data & Governance Initiative (EDGI)](https://envirodatagov.org/), scrapes RMP PDF reports, structures the data, and makes it accessible via a user-friendly Datasette interface. Key data includes facility details, chemical inventories, accident histories, and industry classifications.

## Data Source

Data is sourced from the EPA's [RMP Search Tool](https://cdxapps.epa.gov/olem-rmp-pds/). The 21,561 PDFs, organized by state in the `reports` folder, were processed using:
- `scrap_pdf_rmp_reports_to_csv.py`: Extracts data into CSV files (`rmp_chemical.csv`, `rmp_facility_chemicals.csv`, etc.), stored in the `data` folder.
- `scrap_accidents_details_to_csv.py`: Extracts 1,554 accident details from 1,129 facilities, saved as `rmp_accident_details.csv` in the `data` folder.
- `create_sqlite_rmp_db_from_csv.py`: Builds the `risk-management-plans.db` SQLite database with tables and views.

## File Structure

```
rmp-datasette/ 
├── rmp/
│   ├── metadata.json
│   ├── risk-management-plans.db
│   ├── plugins/
│   │   └── render_links.py
│   ├── templates/
│   │   └── index.html
├── data/
│   ├── accident_details_log.txt
│   ├── accident_history_log.txt
│   ├── fac_acc_chem_log.txt
│   ├── risk-management-plans.db
│   ├── rmp_accident_chemicals.csv
│   ├── rmp_accident_detailed.csv
│   ├── rmp_facility_accidents.csv
│   ├── rmp_chemical.csv
│   ├── rmp_facility.csv
│   ├── rmp_facility_chemicals.csv
│   ├── rmp_facility_naics.csv
│   └── rmp_naics.csv
├── script/
    ├── create_accident_detail_sqlite.py
    ├── create_sqlite_rmp_db_from_csv.py
    ├── create_sqlite_views_and_fts_tables.py
    ├── scrap_accidents_details_to_csv.py
    ├── scrap_pdf_rmp_reports_to_csv.py
    └── scrap_single_pdf_to_csv.py
```

## Data Structure

The SQLite database includes:

| Table/View                | Description                                                                 |
|---------------------------|-----------------------------------------------------------------------------|
| `rmp_facility`            | Facility details (EPA ID, name, address, coordinates, report dates)         |
| `rmp_chemical`            | Unique chemicals (ID, name, CAS number, flammable/toxic)                    |
| `rmp_facility_chemicals`  | Links facilities to chemicals with program levels                           |
| `rmp_naics`               | NAICS codes and descriptions                                                |
| `rmp_facility_naics`      | Links facilities to NAICS codes                                             |
| `rmp_facility_accidents`  | Accident details (ID, date, time, release duration, NAICS code)             |
| `rmp_accident_chemicals`  | Chemicals in accidents (quantity released, percent weight)                  |
| `tbl_accident_details`    | Detailed information on 1,554 accidents (sourced from `rmp_accident_details.csv`) |
| `facility_view`           | Aggregated facility data with NAICS codes and chemical names                |
| `facility_accidents_view` | Accident data with facility details and chemical names                      |
| `accident_chemicals_view` | Chemicals released in accidents with facility and accident details          |

Views (`facility_view`, `facility_accidents_view`, `accident_chemicals_view`) are optimized for Datasette, with full-text search via `facility_fts`, `facility_accidents_fts`, and `accident_chemicals_fts`. The `rmp_accident_details.csv` dataset, containing 1,554 accident details from 1,129 facilities, is integrated into `risk-management-plans.db` as the `tbl_accident_details` table.

## Installation

To explore the data with Datasette:

1. **Clone the Repository**:
```bash
   git clone https://github.com/Munkh976/rmp-datasette.git
   cd rmp-datasette
```
2. **Install Dependencies: Requires Python 3.8+**:
```bash
   pip install datasette==0.65.1 sqlite-utils>=3.35 datasette-cluster-map datasette-render-markdown datasette-template-sql markupsafe==2.1.5
```
3. **Set Up the Database: Use the provided risk-management-plans.db. To regenerate from CSVs**:
```bash
   python script/create_sqlite_rmp_db_from_csv.py
   python script/create_accident_detail_sqlite.py
   python script/create_sqlite_views_and_fts_tables.py
```
4. **Run Datasette with Custom Template and Homepage**:
```bash
datasette rmp/risk-management-plans.db \
  --setting sql_time_limit_ms 2500 \
  --metadata rmp/metadata.json \
  --plugins-dir rmp/plugins \
  --template-dir rmp/templates \
  --static static:rmp/static
```
Then open your browser to: http://localhost:8001

## Custom Homepage

The `index.html` file under `templates/` serves as a custom homepage with:
- A visual header and introductory description
- Dynamic links to key views: Facilities, Accidents, Chemicals
- Statistics showing total counts from the database
- Navigation cards to sub-sections like detailed accidents, regulated chemicals, and NAICS codes
- Light/Dark mode toggle with persistent theme preference

## Exploring the Data
Datasette offers:

- **Facets**: Filter by state, county, NAICS codes, chemical names, or accident dates.
- **Search**: Full-text search on facilities, accidents, and chemicals.
- **Maps**: Visualize facility locations with datasette-cluster-map.
- **Markdown**: Render report column as Markdown via datasette-render-markdown.
- **Custom Links**: The `render_links.py` plugin (in `plugins/`) adds hyperlinks to fields like `facility_id` and `accident_id`, linking to related rows in `rmp_facility` and `facility_accidents_view`.

## Scripts
- `scrap_pdf_rmp_reports_to_csv.py`: Converts PDFs to CSVs, handling chemicals, NAICS, and accidents.
- `create_sqlite_rmp_db_from_csv.py`: Creates the SQLite database with tables and views.
- `scrap_accidents_details_to_csv.py`: Extracts accident details from PDFs into `rmp_accident_details.csv`.

## Plugins
- `render_links.py` (in `plugins/`): A custom Datasette plugin that enhances navigation by linking identifiers to detailed records in related views.

## License
Data is licensed under the Open Data Commons Open Database License (ODbL).

## Contributing
Contributions are welcome! Submit issues or pull requests on GitHub. Contact the EDGI team via the issue tracker.

## Acknowledgments
Built by the EDGI team. Thanks to the EPA for the RMP data and the open-source community for tools like Datasette.

## Working Files
- GitHub Repository: https://github.com/Munkh976/rmp-datasette

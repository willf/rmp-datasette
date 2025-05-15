# EPA Risk Management Plans (rmp) Screening Tools Project 📊

This repository contains data from the U.S. Environmental Protection Agency (EPA) Risk Management Plans (RMPs), detailing facilities handling hazardous substances, their chemicals, accidents, and NAICS codes. Extracted from 21,561 PDF reports stored in state subfolders (e.g., `reports/AK`, `reports/AL`), the data is processed into a SQLite database (`risk-management-plans.db`) and visualized using [Datasette](https://datasette.io/).

## Overview

The EPA mandates RMPs for facilities using hazardous substances to ensure safety. This project, part of the [Environmental Data & Governance Initiative (EDGI)](https://envirodatagov.org/), scrapes RMP PDF reports, structures the data, and makes it accessible via a user-friendly Datasette interface. Key data includes facility details, chemical inventories, accident histories, and industry classifications.

## Data Source

Data is sourced from the EPA's [RMP Search Tool](https://cdxapps.epa.gov/olem-rmp-pds/). The 21,561 PDFs, organized by state in the `reports` folder, were processed using:
- `scrap_pdf_rmp_reports_to_csv.py`: Extracts data into CSV files (`rmp_chemical.csv`, `rmp_facility_chemicals.csv`, etc.), stored in the `Data` folder.
- `scrap_accidents_details_to_csv.py`: Extracts 1,554 accident details from 1,129 facilities, saved as `rmp_accident_details.csv` in the `Data` folder.
- `create_sqlite_rmp_db_from_csv.py`: Builds the `risk-management-plans.db` SQLite database with tables and views.

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

Views (`facility_view`, `facility_accidents_view`, `accident_chemicals_view`) are optimized for Datasette, with full-text search via `facility_fts`, `facility_accidents_fts`, and `accident_chemicals_fts`. The `rmp_accident_details.csv` dataset, containing 1,554 accident details from 1,129 facilities, is integrated into `risk-management-plans.db` as the `tbl_accident_details` table, which feeds into `rmp_facility_accidents` and `rmp_accident_chemicals`.

## Installation

To explore the data with Datasette:

1. **Clone the Repository**:
```bash
   git clone https://github.com/Munkh976/rmp-datasette.git
   cd rmp-datasette
```
2. **Install Dependencies: Requires Python 3.8+**:
```bash
   pip install datasette==0.65.1 sqlite-utils>=3.35 datasette-cluster-map datasette-render-markdown markupsafe
```
3. **Set Up the Database: Use the provided risk-management-plans.db. To regenerate from CSVs**:
```bash
   python create_sqlite_rmp_db_from_csv.py
   python create_accident_detail_sqlite.py
   python create_sqlite_views_and_fts_tables.py
```
4. **Run Datasette** :
```bash
datasette risk-management-plans.db --metadata metadata.json --setting sql_time_limit_ms 2500 --metadata metadata.json --plugins-dir plugins
```
Access at http://localhost:8001.

## Exploring the Data
Datasette offers:

- Facets: Filter by state, county, NAICS codes, chemical names, or accident dates.
- Search: Full-text search on facilities, accidents, and chemicals.
- Maps: Visualize facility locations with datasette-cluster-map.
- Markdown: Render report column as Markdown via datasette-render-markdown.
- Custom Links: The render_links.py plugin (located in rmp/plugins) adds hyperlinks to columns like facility_id and accident_id, linking to related records in rmp_facility and facility_accidents_view.
The metadata.json configures titles, descriptions, and facets for facility_view, facility_accidents_view, and accident_chemicals_view.

## Scripts
- scrap_pdf_rmp_reports_to_csv.py: Converts PDFs to CSVs, handling chemicals, NAICS, and accidents.
- create_sqlite_rmp_db_from_csv.py: Creates the SQLite database with tables and views.
- scrap_accidents_details_to_csv.py: Extracts accident details from PDFs into rmp_accident_details.csv.

## Plugins
- render_links.py (in rmp/plugins): A custom Datasette plugin that enhances navigation by adding hyperlinks to facility_id and accident_id columns in facility_accidents_view and accident_chemicals_view, linking to related records in rmp_facility and other views.

## License
Data is licensed under the Open Data Commons Open Database License (ODbL).

## Contributing
Contributions are welcome! Submit issues or pull requests on GitHub. Contact the EDGI team via the issue tracker.

## Acknowledgments
Built by the EDGI team. Thanks to the EPA for the RMP data and the open-source community for tools like Datasette.

## Working Files
- GitHub Repository: https://github.com/Munkh976/rmp-datasette

---

### Explanation of Updates
1. **Added `tbl_accident_details` Table**:
   - Added a new row in the Data Structure table for `tbl_accident_details`, describing it as containing detailed information on 1,554 accidents, sourced from `rmp_accident_details.csv`.
   - Updated the accompanying paragraph to clarify that `rmp_accident_details.csv` is integrated into `risk-management-plans.db` as the `tbl_accident_details` table, which then feeds into `rmp_facility_accidents` and `rmp_accident_chemicals`.

2. **Maintained Existing Updates**:
   - Kept the `render_links.py` plugin section and the `Data` folder references for CSV files as previously added.
   - Ensured consistency in formatting and style with the rest of the `README.md`.

3. **Clarified Relationships**:
   - Emphasized that `tbl_accident_details` serves as a detailed source table for accident-related data, which is then structured into `rmp_facility_accidents` and `rmp_accident_chemicals` for relational use in the database.

### Next Steps
- **Push to GitHub**:
  - Save the updated `README.md` in the root of your `rmp-datasette` repository.
  - Commit and push:
    ```bash
    git add README.md
    git commit -m "Updated README.md with tbl_accident_details table"
    git push origin main

- Verify in Datasette:
   - Run Datasette to ensure the tbl_accident_details table is correctly represented:
   ```bash 
   datasette risk-management-plans.db --metadata metadata.json --plugins-dir=rmp/plugins
   ```
   - Check http://localhost:8001 to confirm the table is accessible.
- Feedback:
   - Let me know if you need further adjustments, such as adding more details about tbl_accident_details (e.g., its schema) or other repository updates.
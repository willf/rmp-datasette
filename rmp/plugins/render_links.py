# -*- coding: utf-8 -*-
"""
Created on Mon May 12 12:13:00 2025

@author: MOGIC
"""
from datasette import hookimpl
from markupsafe import Markup

@hookimpl
def render_cell(datasette, value, column, table, database, row):
    if database != "risk-management-plans":
        return None

    # Handle facility_id, naics_code, facility_accident_id, and accident_id for facility_accidents_view
    if table == "facility_accidents_view":
        if column == "facility_id":
            return Markup(f'<a href="/risk-management-plans/rmp_facility/{value}">{value}</a>')
        if column == "naics_code":
            return Markup(f'<a href="/risk-management-plans/rmp_naics/{value}">{value}</a>')
        if column == "facility_accident_id":
            return Markup(f'<a href="/risk-management-plans/facility_accidents_view/{value}">{value}</a>')
        if column == "accident_id":
            # Use the facility_accident_id from the same row to link to facility_accidents_view
            facility_accident_id = row["facility_accident_id"]
            return Markup(f'<a href="/risk-management-plans/facility_accidents_view/{facility_accident_id}">{value}</a>')

    # Handle epa_facility_id in facility_view
    if table == "facility_view" and column == "epa_facility_id" and value:
        return Markup(f'<a href="/risk-management-plans/rmp_facility/{value}">{value}</a>')

    # Handle naics_codes in facility_view (comma-separated list)
    if table == "facility_view" and column == "naics_codes" and value:
        # Split the comma-separated list
        codes = [code.strip() for code in value.split(",")]
        # Create a link for each naics_code
        links = [f'<a href="/risk-management-plans/rmp_naics/{code}">{code}</a>' for code in codes]
        # Join the links with commas
        return Markup(", ".join(links))

    # Handle chemical_ids in facility_view (comma-separated list)
    if table == "facility_view" and column == "chemical_ids" and value:
        # Split the comma-separated list
        ids = [id.strip() for id in value.split(",")]
        # Create a link for each chemical_id to rmp_facility_chemicals
        links = [f'<a href="/risk-management-plans/rmp_facility_chemicals/{id}">{id}</a>' for id in ids]
        # Join the links with commas
        return Markup(", ".join(links))

    return None
from datasette.app import Datasette
import os

ds = Datasette(
    files=["risk-management-plans.db"],
    metadata_path="metadata.json",
    extra_options={
        "--plugins-dir": "plugins/",
        "--template-dir": "templates/"
    }
)
app = ds.app()
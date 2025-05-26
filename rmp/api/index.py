from datasette.app import Datasette
import os

ds = Datasette(
    files=["rmp/risk-management-plans.db"],
    metadata_path="rmp/metadata.json",
    extra_options={
        "--plugins-dir": "rmp/plugins/",
        "--template-dir": "rmp/templates/"
    }
)
app = ds.app()
import os

def post_fork(server, worker):
    from datasette.app import Datasette
    datasette = Datasette(
        files=["/data/risk-management-plans.db"],
        metadata_path="/app/metadata.json",
        extra_options={
            "--plugins-dir": "/app/plugins",
            "--template-dir": "/app/templates",
            "--setting": "max_returned_rows 1000"
        }
    )
    server.app = datasette.app()
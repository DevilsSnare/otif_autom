from imports import *
os.chdir(os.path.dirname(os.path.abspath(__file__)))
from ingestion_tables import main as ingestion_tables_main
from ingestion_excels import main as ingestion_excels_main
from main import main as cal_main
from dod import main as dod_main
from sharepoint import SharepointClient
from tqdm import tqdm
import datetime
from contextlib import redirect_stdout
from io import StringIO

def load_creds(path):
    creds = {}
    with open(path, 'r') as f:
        for line in f:
            if '=' in line:
                key, value = line.strip().split('=', 1)
                creds[key.strip()] = value.strip()
    return creds


start_time = datetime.datetime.now()
print("-" * 60)
print(f"OTIF Pipeline Started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
print("-" * 60)

steps = [
    "Load credentials",
    "Ingest tables",
    "Ingest excels",
    "Calculate final dataframe",
    "Calculate DoD view",
    "Upload to SharePoint"
]

for step in tqdm(steps, desc="Overall Progress", unit="step"):
    if step == "Load credentials":
        creds = load_creds('creds.txt')

    elif step == "Ingest tables":
        dfs_tables = ingestion_tables_main(creds)

    elif step == "Ingest excels":
        dfs_excels = ingestion_excels_main(creds)

    elif step == "Calculate final dataframe":
        final_df = cal_main(dfs_tables, dfs_excels)
    
    elif step == "Calculate DoD view":
        final_df_with_dod = dod_main(final_df, dfs_tables['dod_data'])

    elif step == "Upload to SharePoint":
        root_url = "https://razrgroup.sharepoint.com/sites/Razor"
        library_path = "/sites/Razor/Shared%20Documents/Chetan_Locale/OTIF/Export"
        file_name = "OTIF_DWH_Import_V3.xlsx"

        sharepoint = SharepointClient(root_url)
        sharepoint.init_session()
        sharepoint.update_sharepoint_excel(
            site_url=root_url,
            library=library_path,
            df=final_df_with_dod,
            file=file_name,
            sheet_name="Data",
            start_cell="A2"
        )

end_time = datetime.datetime.now()
duration = end_time - start_time
print("-" * 60)
print(f"OTIF Pipeline Completed at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total Duration: {duration}")
print("-" * 60)

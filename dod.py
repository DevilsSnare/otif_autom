from imports import *
# import importlib.util
# from pathlib import Path

from tat_calculator import run_tat_calculation as tat_cal


# def module_from_file(module_name, relative_path):
#     base_path = Path(__file__).parent.resolve()
#     file_path = base_path / relative_path

#     spec = importlib.util.spec_from_file_location(module_name, file_path)
#     module = importlib.util.module_from_spec(spec)
#     spec.loader.exec_module(module)
#     return module

# tat_cal = module_from_file("main", "tat-calculator/run_tat_calculation.py")

def main(final_df, dod_data):
    sheet_name = tat_cal.main(dod_data)
    df_dod = pd.read_excel(f'tat_calculator\outputs\excel_exports\{sheet_name}')
    print(sheet_name)
    return df_dod
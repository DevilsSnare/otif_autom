from imports import *
from run_tat_calculation import main as tat_main
import os


def main(final_df, buffer_mapping):
    dod_df = tat_main()

    dod = pd.read_excel(f'{dod_df}', sheet_name='Final_Timestamps')

    dod['A. Anti PO Line'] = "Status A"
    dod['B. Compliance Blocked'] = "Status B"
    dod['C. Shipped'] = "Status C"
    dod['D. Master Data Blocker'] = "Status D"

    dod['Current Status'] = dod['PO_ID'].map(final_df.set_index('po_razin_id')['Current Status']).fillna("")

    def xlookup_current_status(row):
        current_status = row["Current Status"]
        if current_status in row:
            return row[current_status]
        else:
            return ""

    dod['Relevant Timestamp'] = dod.apply(xlookup_current_status, axis=1)

    today = pd.to_datetime(datetime.today().date())

    buffer_map = dict(zip(buffer_mapping['Stage'], buffer_mapping['Days']))

    def compute_days(row):
        value = row['Relevant Timestamp']
        status = row['Current Status']
        buffer = buffer_map.get(status, 0)
        try:
            date_val = pd.to_datetime(value, errors='coerce')
            if pd.isna(date_val):
                return None
            return (today - date_val.normalize()).days - buffer
        except:
            return None

    dod['Days'] = dod.apply(compute_days, axis=1)
    dod['Days Bucket'] = dod['Days'].apply(lambda x: "Status Missing" if pd.isna(x) else "On-Track" if x<=0 else "01-03" if x<=3 else "04-08" if x<=8 else "09-15" if x<=15 else "15+")

    final_df['Days'] = final_df['po_razin_id'].map(dod.drop_duplicates(subset="PO_ID", keep="first").set_index('PO_ID')['Days']).fillna(1)
    final_df['Days Bucket'] = final_df['po_razin_id'].map(dod.drop_duplicates(subset="PO_ID", keep="first").set_index('PO_ID')['Days Bucket']).fillna("01-03")

    return final_df
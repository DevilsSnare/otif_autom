from imports import *
from sharepoint import SharepointClient
import concurrent.futures
import time
from datetime import datetime
import pandas as pd

def fetch_from_sharepoint_safe(root_url_param, relative_url_param, tracker, sheet, large_file=False):
    """
    Wrapper to safely call Sharepoint fetch functions with retries and return None on failure.
    """
    for attempt in range(3):
        try:
            sharepoint = SharepointClient(root_url_param)
            sharepoint.init_session()
            if large_file:
                df = sharepoint.fetch_sharepoint_excel_large_files(relative_url_param + tracker, sheet)
            else:
                df = sharepoint.fetch_sharepoint_excel(relative_url_param + tracker, sheet)
            return df
        except Exception as e:
            # print(f"Attempt {attempt + 1} failed for {tracker} - {sheet}: {e}")
            if attempt < 2:
                time.sleep(5)
            else:
                print(f"Failed to fetch {tracker} - {sheet} after multiple retries.")
                return pd.DataFrame() # Return empty DataFrame on persistent failure

def process_telex_ffw(df):
    if df.empty:
        return pd.DataFrame()
    telex_ffw = df[["Shipment Number", "Telex Released/Not Released", "Standard Remarks"]].copy()
    telex_ffw = telex_ffw[telex_ffw["Shipment Number"].notna() & (telex_ffw["Shipment Number"] != "")]
    telex_ffw['Final Status'] = telex_ffw['Telex Released/Not Released'].str.strip()
    telex_ffw['Final Blocker Status'] = telex_ffw['Standard Remarks'].apply(
        lambda x: "No FFW Telex Blocker Mentioned" if x == "" else x
    )
    return telex_ffw

def process_fob_date(df):
    if df.empty:
        return pd.DataFrame()
    fob_date = df[["BATCH ID", "CFS/CY Cut off", "Expected Date at CFS/CY", "ETD Load Port", "Blocker"]].copy()
    fob_date = fob_date[fob_date["BATCH ID"].notna() & (fob_date["BATCH ID"] != "")]
    fob_date["Final Date"] = fob_date["CFS/CY Cut off"].combine_first(fob_date["Expected Date at CFS/CY"])
    fob_date["Pickup Status"] = fob_date["BATCH ID"].apply(lambda x: "Not Picked" if pd.notna(x) and x != "" else "Picked")
    return fob_date

def process_packaging_data(df):
    if df.empty:
        return pd.DataFrame()
    packaging_data = df[["PORAZIN", "L2 Bucket 6 Status"]].copy()
    packaging_data = packaging_data[packaging_data["PORAZIN"].notna() & (packaging_data["PORAZIN"] != "")]
    packaging_data_map = pd.DataFrame({
        "Status": [
            "EAN Pending", "SCM Check Pending", "Compliance Check Pending",
            "Label Creation Pending", "Compliance Approval Pending",
            "NPD 1st PO", "Labels Not Required"
        ],
        "Blocker": ["Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "No"],
        "L2": [
            "06a. EAN Pending", "06b. SCM Check Pending", "06c. Compliance Check Pending",
            "06d. Label Creation Pending", "06e. Compliance Approval Pending",
            "06f. NPD 1st PO", ""
        ]
    })
    packaging_data['Final Status'] = packaging_data['L2 Bucket 6 Status'].map(packaging_data_map.set_index("Status")["Blocker"]).fillna("Yes")
    packaging_data['Packaging Standard Status'] = packaging_data['L2 Bucket 6 Status'].map(packaging_data_map.set_index("Status")["L2"]).fillna("06b. SCM Check Pending")
    packaging_data['Check'] = packaging_data['L2 Bucket 6 Status'].map(packaging_data_map.set_index("Status")["Blocker"]).fillna("")
    return packaging_data

def process_transparency_data(df):
    if df.empty:
        return pd.DataFrame()
    transparency_data = df[["PO&RAZIN", "Status"]].copy()
    transparency_data = transparency_data[transparency_data["PO&RAZIN"].notna() & (transparency_data["PO&RAZIN"] != "")]
    transparency_data['Transparency Pending'] = transparency_data['Status'].apply(lambda x: "Yes" if x=="Pending" else "No")
    return transparency_data

def process_transparency_master(df):
    if df.empty:
        return pd.DataFrame()
    transparency_master = df[["ASIN"]].copy()
    transparency_master = transparency_master[transparency_master["ASIN"].notna() & (transparency_master["ASIN"] != "")]
    transparency_master["Transparency Check"] = "Yes"
    return transparency_master

def process_qc(df):
    if df.empty:
        return pd.DataFrame()
    qc = df[["PO RAZIN ID", "QC Status Category"]].copy()
    qc = qc[qc["PO RAZIN ID"].notna() & (qc["PO RAZIN ID"] != "")]
    qc['Final Status2'] = qc['QC Status Category'].apply(lambda x: "No QC Blocker Mentioned" if x=="" else x)
    return qc

def process_payrun(df):
    if df.empty:
        return pd.DataFrame()
    payrun = df[["Invoice No.", "PO No.", "Final_Verdict"]].copy()
    payrun = payrun[payrun["Invoice No."].notna() & (payrun["Invoice No."] != "")]
    payrun['Inv#'] = "Bill #" + payrun["Invoice No."].astype(str)
    payrun['Status'] = payrun["Final_Verdict"].str.strip()
    return payrun

def process_compliance(df):
    if df.empty:
        return pd.DataFrame()
    compliance = df[["PO&RAZIN&ID", "Blocker Status", "Comments", "SM Resolved"]].copy()
    compliance = compliance[compliance["PO&RAZIN&ID"].notna() & (compliance["PO&RAZIN&ID"] != "")]
    compliance["Final Status"] = compliance.apply(
        lambda row: "Compliance Blocker Resolved" if (row["SM Resolved"] == "Yes")
        else ("No Compliance Blocker Mentioned" if row["Blocker Status"] != "" else row["Blocker Status"]),
        axis=1
    )
    return compliance

def process_ffw_status(df):
    if df.empty:
        return pd.DataFrame()
    ffw_status = df.copy()
    ffw_status.columns = ffw_status.iloc[0]
    ffw_status = ffw_status[1:].reset_index(drop=True)
    ffw_status = ffw_status.rename(columns={"SubStatus": "SubStatus.1"})
    ffw_status = ffw_status[["Batch ID", "High level stage", "Batch milestone (Automatic)", "Blocker Reason"]].copy()
    ffw_status = ffw_status[ffw_status["Batch ID"].notna() & (ffw_status["Batch ID"] != "")]
    ffw_status["Final Blocker Reason"] = ffw_status["Batch milestone (Automatic)"].apply(lambda x: "No FFW Comment Mentioned" if x == "" else x)
    return ffw_status

def process_prd(df):
    if df.empty:
        return pd.DataFrame()
    prd = df.copy()
    prd.columns = prd.iloc[0]
    prd = prd[1:].reset_index(drop=True)
    prd = prd[["otif_id", "SM: PRD STATUS", "SM Comments"]].copy()
    prd = prd[prd["otif_id"].notna() & (prd["otif_id"] != "")]
    prd['Final Status'] = prd['SM Comments'].apply(lambda x: "No PRD Blocker Mentioned" if x=="" else x)
    return prd

def process_cprd(df):
    if df.empty:
        return pd.DataFrame()
    cprd = df[["po_razin_id", "Standard Comments", "SM Comments"]].copy()
    cprd = cprd[cprd["po_razin_id"].notna() & (cprd["po_razin_id"] != "")]
    cprd['Final Status'] = cprd['Standard Comments'].apply(lambda x: "No CPRD Blocker Mentioned" if x=="" else x)
    return cprd

def process_spd_blockers(df):
    if df.empty:
        return pd.DataFrame()
    spd_blockers = df[["batch_id", "Delay Reason", "Additional Comments"]].copy()
    spd_blockers = spd_blockers[spd_blockers["batch_id"].notna() & (spd_blockers["batch_id"] != "")]
    spd_blockers["Final Status"] = spd_blockers["Delay Reason"].apply(lambda x: "No SPD Blocker Mentioned" if pd.isna(x) or x == "" or x == "0" else x)
    return spd_blockers

def process_ffw_blockers(df):
    if df.empty:
        return pd.DataFrame()
    ffw_blockers = df[["Batch ID", "FFW Blocker", "SM_Resolved Status"]].copy()
    ffw_blockers = ffw_blockers[ffw_blockers["Batch ID"].notna() & (ffw_blockers["Batch ID"] != "")]
    ffw_blockers["Final Status"] = ffw_blockers.apply(
        lambda row: "Yes" if row["FFW Blocker"] == "" or str(row["SM_Resolved Status"]).startswith("Yes") else "No",
        axis=1
    )
    return ffw_blockers

def process_telex_supplier(df):
    if df.empty:
        return pd.DataFrame()
    telex_supplier = df[["shipment number", "SM Action"]].copy()
    telex_supplier = telex_supplier[telex_supplier["shipment number"].notna() & (telex_supplier["shipment number"] != "")]
    telex_supplier['Final Status'] = telex_supplier["SM Action"].apply(lambda x: "Released" if x == "Green1:Released by Supplier(Copy BOL available on VP)" else "Not Released")
    telex_supplier['Final Blocker Status'] = telex_supplier["SM Action"].apply(lambda x: "No Telex Blocker Mentioned" if x == "" else x)
    return telex_supplier

def process_prepayment(df):
    if df.empty:
        return pd.DataFrame()
    prepayment = df[["document number", "Auto_ PI status", "PI upload blocker"]].copy()
    prepayment = prepayment[prepayment["document number"].notna() & (prepayment["document number"] != "")]
    prepayment['Final Status'] = prepayment['PI upload blocker'].apply(lambda x: "No PI Blocker Mentioned" if x=="" else x)
    return prepayment

def process_g2(df):
    if df.empty:
        return pd.DataFrame()
    g2 = df[["otif_id", "SM Confirm Ready for Batching", "Final Dispute/Blocker"]].copy()
    g2 = g2[g2["otif_id"].notna() & (g2["otif_id"] != "")]
    g2["Final Status"] = g2["Final Dispute/Blocker"].apply(lambda x: "No G2 Blocker Mentioned" if pd.isna(x) or x == "" or x==" " or x == 0 else x)
    return g2

def process_g4(df):
    if df.empty:
        return pd.DataFrame()
    g4 = df[["batch_id", "SM G4 Status", "Final Dispute/Blocker"]].copy()
    g4 = g4[g4["batch_id"].notna() & (g4["batch_id"] != "")]
    g4["Final Status"] = g4["Final Dispute/Blocker"].apply(lambda x: "No G4 Blocker Mentioned" if pd.isna(x) or x == "" or x==" " or x == 0 else x)
    return g4


def main(creds):
    today = datetime.now()
    results = {}

    file_path = "static/default_mappings.xlsx"

    # Load static mapping files - these are fast and don't need threading
    results['status_mapping'] = pd.read_excel(file_path, sheet_name="Status", engine="openpyxl")
    results['blockers_mapping'] = pd.read_excel(file_path, sheet_name="Blockers", engine="openpyxl")
    results['payment_terms_mapping'] = pd.read_excel(file_path, sheet_name="Payment Terms", engine="openpyxl")
    results['cm_sm_vendor_mapping'] = pd.read_excel(file_path, sheet_name="CM-SM-Vendor", engine="openpyxl")
    results['memo_mapping'] = pd.read_excel(file_path, sheet_name="Memo-Summary", engine="openpyxl")
    results['team_priority_mapping'] = pd.read_excel(file_path, sheet_name="Team-Priority", engine="openpyxl")
    results['asin_priority_mapping'] = pd.read_excel(file_path, sheet_name="ASIN-Priority", engine="openpyxl")
    results['asin_static_payment_status'] = pd.read_csv("static/asin_static_payment_status.csv")


    sharepoint_tasks = []

    # FFW Reporting
    root_url_ffw_reporting = "https://razrgroup.sharepoint.com/teams/logistics-group"
    relative_url_ffw_reporting = "/teams/logistics-group/Freigegebene%20Dokumente/Freight%20Operations/"
    sharepoint_tasks.append({
        'name': 'telex_ffw_raw',
        'func': fetch_from_sharepoint_safe,
        'args': (root_url_ffw_reporting, relative_url_ffw_reporting, "FFW Reporting.xlsx", "INBSHIP Level", True)
    })

    # Procurement Trackers / Temporary
    root_url_temp = "https://razrgroup.sharepoint.com/sites/Razor"
    relative_url_temp = "/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/Temporary/"
    sharepoint_tasks.append({
        'name': 'fob_date_raw',
        'func': fetch_from_sharepoint_safe,
        'args': (root_url_temp, relative_url_temp, "Razor Mohawk Tracker_0224.xlsx", "FOB CN-US")
    })

    # Procurement Trackers / Manually_Updated
    root_url_manual = "https://razrgroup.sharepoint.com/sites/Razor"
    relative_url_manual = "/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/Manually_Updated/"
    sharepoint_tasks.append({
        'name': 'packaging_data_raw',
        'func': fetch_from_sharepoint_safe,
        'args': (root_url_manual, relative_url_manual, "Active Packaging Tracker Sheet.xlsx", "PO Label Status")
    })
    sharepoint_tasks.append({
        'name': 'transparency_data_raw',
        'func': fetch_from_sharepoint_safe,
        'args': (root_url_manual, relative_url_manual, "20240822_Ad_hoc_edit_requests.xlsx", "Transparency Label Requests")
    })
    sharepoint_tasks.append({
        'name': 'transparency_master_raw',
        'func': fetch_from_sharepoint_safe,
        'args': (root_url_manual, relative_url_manual, "TransparencyProducts Razor + Perch.xlsx", "Products")
    })
    sharepoint_tasks.append({
        'name': 'qc_raw',
        'func': fetch_from_sharepoint_safe,
        'args': (root_url_manual, relative_url_manual, "Pending QC status --2025.xlsx", "Pending QC")
    })
    sharepoint_tasks.append({
        'name': 'payrun_raw',
        'func': fetch_from_sharepoint_safe,
        'args': (root_url_manual, relative_url_manual, "Approved_Invoice_Master_Tracker.xlsx", "Current_Week_Payrun")
    })
    sharepoint_tasks.append({
        'name': 'compliance_raw',
        'func': fetch_from_sharepoint_safe,
        'args': (root_url_manual, relative_url_manual, "Compliance L2 Status.xlsx", "Compliance")
    })
    sharepoint_tasks.append({
        'name': 'ffw_status_raw',
        'func': fetch_from_sharepoint_safe,
        'args': (root_url_manual, relative_url_manual, "FF E2E Tracker_V3.xlsx", "Main Sheet")
    })

    # Procurement Trackers / Automated
    root_url_automated = "https://razrgroup.sharepoint.com/sites/Razor"

    sharepoint_tasks.append({
        'name': 'prd_raw',
        'func': fetch_from_sharepoint_safe,
        'args': (root_url_automated, f"/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/PRD/", "PRD_Tracker.xlsx", f"PRD - {today.day:02d}.{today.month:02d}.{today.year}")
    })
    sharepoint_tasks.append({
        'name': 'cprd_raw',
        'func': fetch_from_sharepoint_safe,
        'args': (root_url_automated, f"/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/CPRD/", "CPRD Tracker.xlsx", f"CPRD - {today.day:02d}.{today.month:02d}.{today.year}")
    })
    sharepoint_tasks.append({
        'name': 'spd_blockers_raw',
        'func': fetch_from_sharepoint_safe,
        'args': (root_url_automated, f"/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/Pickup/", "Pending Pickup Tracker.xlsx", f"SPD - {today.day:02d}.{today.month:02d}.{today.year}")
    })
    sharepoint_tasks.append({
        'name': 'ffw_blockers_raw',
        'func': fetch_from_sharepoint_safe,
        'args': (root_url_automated, f"/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/Pickup/", "Pending Pickup Tracker.xlsx", "FFW Blockers")
    })
    sharepoint_tasks.append({
        'name': 'telex_supplier_raw',
        'func': fetch_from_sharepoint_safe,
        'args': (root_url_automated, f"/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/Telex/", "Telex Release Tracker.xlsx", f"TLX - {today.day:02d}.{today.month:02d}.{today.year}")
    })
    sharepoint_tasks.append({
        'name': 'prepayment_raw',
        'func': fetch_from_sharepoint_safe,
        'args': (root_url_automated, f"/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/Payment/", "Prepayment Tracker.xlsx", f"PP - {today.day:02d}.{today.month:02d}.{today.year}")
    })
    sharepoint_tasks.append({
        'name': 'g2_raw',
        'func': fetch_from_sharepoint_safe,
        'args': (root_url_automated, f"/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/G2&G4/", "G2 & G4 Signoff Tracking.xlsx", f"G2 - {today.day:02d}.{today.month:02d}.{today.year}")
    })
    sharepoint_tasks.append({
        'name': 'g4_raw',
        'func': fetch_from_sharepoint_safe,
        'args': (root_url_automated, f"/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/G2&G4/", "G2 & G4 Signoff Tracking.xlsx", f"G4 - {today.day:02d}.{today.month:02d}.{today.year}")
    })

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_data = {executor.submit(task['func'], *task['args']): task for task in sharepoint_tasks}
        for future in concurrent.futures.as_completed(future_to_data):
            task = future_to_data[future]
            try:
                data = future.result()
                results[task['name']] = data
            except Exception as exc:
                print(f'{task["name"]} generated an exception: {exc}')
                results[task['name']] = pd.DataFrame() # Ensure an empty DataFrame is returned on error

    # Post-processing of DataFrames
    results['telex_ffw'] = process_telex_ffw(results.get('telex_ffw_raw', pd.DataFrame()))
    results['fob_date'] = process_fob_date(results.get('fob_date_raw', pd.DataFrame()))
    results['packaging_data'] = process_packaging_data(results.get('packaging_data_raw', pd.DataFrame()))
    results['transparency_data'] = process_transparency_data(results.get('transparency_data_raw', pd.DataFrame()))
    results['transparency_master'] = process_transparency_master(results.get('transparency_master_raw', pd.DataFrame()))
    results['qc'] = process_qc(results.get('qc_raw', pd.DataFrame()))
    results['payrun'] = process_payrun(results.get('payrun_raw', pd.DataFrame()))
    results['compliance'] = process_compliance(results.get('compliance_raw', pd.DataFrame()))
    results['ffw_status'] = process_ffw_status(results.get('ffw_status_raw', pd.DataFrame()))
    results['prd'] = process_prd(results.get('prd_raw', pd.DataFrame()))
    results['cprd'] = process_cprd(results.get('cprd_raw', pd.DataFrame()))
    results['spd_blockers'] = process_spd_blockers(results.get('spd_blockers_raw', pd.DataFrame()))
    results['ffw_blockers'] = process_ffw_blockers(results.get('ffw_blockers_raw', pd.DataFrame()))
    results['telex_supplier'] = process_telex_supplier(results.get('telex_supplier_raw', pd.DataFrame()))
    results['prepayment'] = process_prepayment(results.get('prepayment_raw', pd.DataFrame()))
    results['g2'] = process_g2(results.get('g2_raw', pd.DataFrame()))
    results['g4'] = process_g4(results.get('g4_raw', pd.DataFrame()))

    # Remove raw dataframes if no longer needed to save memory
    keys_to_remove = [k for k in results if k.endswith('_raw')]
    for key in keys_to_remove:
        del results[key]

    return results
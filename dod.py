from imports import *
import run_tat_calculation as tat_cal
import os

# Change to the script's directory


def main(final_df):
    dod_df = tat_cal.main()

    dod = pd.read_excel(f'{dod_df}', sheet_name='delay_days')

    dod['A. Anti PO Line'] = 0
    dod['B. Compliance Blocked'] = 0
    dod['C. Shipped'] = 0
    dod['D. Master Data Blocker'] = 0

    dod['Current Status'] = dod['PO_ID'].map(final_df.set_index('po_razin_id')['Current Status']).fillna("")

    columns = [
        'A. Anti PO Line','B. Compliance Blocked','C. Shipped','D. Master Data Blocker','01. PO Approval Pending','02. Supplier Confirmation Pending',
        '03. PI Upload Pending','04. PI Approval Pending','05. PI Payment Pending','06. Packaging Pending','07. Transparency Label Pending','08. PRD Pending',
        '09. Under Production','10. PRD Confirmation Pending','11. IM Sign-Off Pending','12. Ready for Batching Pending','13. Batch Creation Pending',
        '14. SM Sign-Off Pending','15. CI Approval Pending','16. CI Payment Pending','17. QC Schedule Pending','18. FFW Booking Missing',
        '19. Supplier Pickup Date Pending','20. Pre Pickup Check','21. FOB Pickup Pending','22. Non FOB Pickup Pending','23. INB Creation Pending',
        '24. Mark In-Transit Pending','25. BL Approval Pending','26. BL Payment Pending - In Transit','27. BL Payment Pending - Arrived',
        '28. Telex Release Pending','29. Stock Delivery Pending','30. Stock Receiving Pending'
    ]
    def max_status_value(row):
        current_status = row["Current Status"]
        try:
            col_index = columns.index(current_status)
        except ValueError:
            return 1
        
        value = row[columns[col_index]]
        
        return max(value, 1)

    dod['Days'] = dod.apply(max_status_value, axis=1)

    def categorize_days(days):
        if days == "On-Track":
            return "On-Track"
        elif days <= 3:
            return "01-03"
        elif days <= 8:
            return "04-08"
        elif days <= 15:
            return "09-15"
        else:
            return "15+"

    dod['Days Bucket'] = dod['Days'].apply(categorize_days)

    final_df['Days Bucket'] = final_df['po_razin_id'].map(dod.drop_duplicates(subset="PO_ID", keep="first").set_index('PO_ID')['Days Bucket']).fillna("")

    return final_df
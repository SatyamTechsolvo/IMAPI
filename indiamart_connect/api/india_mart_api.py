import re
import frappe
from frappe.utils import now_datetime
from frappe.integrations.utils import make_get_request
import json
from frappe import _
from datetime import datetime, timedelta
import time

def format_date_for_api(date_input):
    if isinstance(date_input, datetime):
        return date_input.strftime("%d-%m-%Y %H:%M:%S")
    else:
        dt = datetime.strptime(date_input, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%d-%m-%Y %H:%M:%S")

def get_india_mart_leads():
    india_mart_url = get_india_mart_url()
    
    # Define the date range for the last 365 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    # Loop through the date range in 7-day increments
    while start_date < end_date:
        batch_end_date = min(start_date + timedelta(days=7), end_date)
        
        # Format dates for the API
        formatted_start_date = format_date_for_api(start_date)
        formatted_end_date = format_date_for_api(batch_end_date)
        
        india_mart_url_with_dates = (
            f"{india_mart_url}&start_time={formatted_start_date}&end_time={formatted_end_date}"
        )     
        # Implementing rate limiting
        time.sleep(300)  # Wait for 5 minutes before the API call
        
        results = make_get_request(india_mart_url_with_dates)
        create_india_mart_logs(india_mart_url_with_dates, results)
        frappe.db.commit()
        
        if results.get("CODE") == 200:
            create_india_mart_leads(results.get("RESPONSE"))
        
        start_date = batch_end_date

def get_india_mart_url():
    settings = frappe.get_doc("IndiaMART Settings", "IndiaMART Settings")
    return f"{settings.get('url')}?glusr_crm_key={settings.get_password('api_key')}"

def update_last_fetch_time(to_datetime):
    frappe.db.set_value(
        "IndiaMART Settings",
        "IndiaMART Settings",
        "last_fetch_updated_time",
        to_datetime,
    )

def create_india_mart_logs(url, results):
    frappe.get_doc(
        dict(
            doctype="IndiaMART API Logs",
            url=url,
            datetime=now_datetime(),
            status=results.get("STATUS"),
            failure_message=results.get("MESSAGE"),
            response=json.dumps(results.get("RESPONSE")),
        )
    ).insert(ignore_permissions=True)

def clean_html(text):
    clean_text = re.sub(r'<.*?>', '', text)
    return clean_text

def create_india_mart_leads(leads):
    for lead in leads:
        if (
            not frappe.db.exists("Lead", {"custom_india_mart_id": lead.get("UNIQUE_QUERY_ID")})
            and (not lead.get("SENDER_EMAIL") or not frappe.db.exists("Lead", {"email_id": lead.get("SENDER_EMAIL")}))
        ):
            lead_doc = frappe.get_doc(dict(
                doctype="Lead",
                lead_name=lead.get("SENDER_NAME"),
                email_id=lead.get("SENDER_EMAIL"),
                phone=lead.get("SENDER_MOBILE"),
                custom_requirement=lead.get("SUBJECT"),
                custom_indiamart_id=lead.get("UNIQUE_QUERY_ID"),
                city=lead.get("SENDER_CITY"),
                state=lead.get("SENDER_STATE"),
                country="India" if (lead.get("SENDER_COUNTRY_ISO") == "IN") else "",
                source="India Mart",
                custom_lead_company=lead.get("SENDER_COMPANY"),
                custom_pin_code=lead.get("SENDER_PINCODE"),
                custom_product=lead.get("QUERY_PRODUCT_NAME"),
                custom_product_details=clean_html(lead.get("QUERY_MESSAGE")),
                custom_query_type=lead.get("QUERY_TYPE")
            ))
            lead_doc.insert(ignore_permissions=True)

def india_mart_cron_job():
    get_india_mart_leads()

@frappe.whitelist()
def india_mart_manually_sync():
    get_india_mart_leads()
    frappe.msgprint(_("Lead sync for IndiaMART added to the queue"))

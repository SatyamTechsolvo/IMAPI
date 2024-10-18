import re
import frappe
from frappe.utils import now_datetime, format_datetime, add_to_date
from frappe.integrations.utils import make_get_request
import json
from frappe import _


def get_india_mart_leads(from_datetime, to_datetime):
    india_mart_url = get_india_mart_url()
    # from_datetime = format_datetime(from_datetime, "dd-mm-yyyy hh:mm:ss")
    # to_datetime = format_datetime(to_datetime, "dd-mm-yyyy hh:mm:ss")
    india_mart_url = (
        f"{india_mart_url}&start_time={from_datetime}&end_time={to_datetime}"
    )
    results = make_get_request(india_mart_url)
    create_india_mart_logs(india_mart_url, results)
    frappe.db.commit()
    if results.get("CODE") == 200:
        create_india_mart_leads(results.get("RESPONSE"))
        update_last_fetch_time(to_datetime)


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
            and not frappe.db.exists("Lead", {"email_id": lead.get("SENDER_EMAIL")})
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
    from_time = frappe.db.get_value(
        "IndiaMART Settings", "IndiaMART Settings", "last_fetch_updated_time"
    )
    to_time = now_datetime()
    get_india_mart_leads(from_time, to_time)


@frappe.whitelist()
def india_mart_manually_sync():
    from_datetime = frappe.db.get_value(
        "IndiaMART Settings", "IndiaMART Settings", "last_fetch_updated_time"
    )
    if not from_datetime:
        frappe.throw(_("Last sync fetch time required to manually sync"))
    to_datetime = now_datetime()
    get_india_mart_leads(from_datetime, to_datetime)
    frappe.msgprint(_("Lead sync for IndiaMART added to the queue"))

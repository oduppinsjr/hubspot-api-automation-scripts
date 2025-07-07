import csv
import requests

# HubSpot Access Token
ACCESS_TOKEN = '[ACCESS_TOKEN]'

# CSV file path: must contain columns contact_id, company_id
CSV_FILE = 'contacts_primary_company.csv'

BASE_URL = 'https://api.hubapi.com'

HEADERS = {
    'Authorization': f'Bearer {ACCESS_TOKEN}',
    'Content-Type': 'application/json'
}

PRIMARY_LABEL_STR = "primary"

def get_contact_company_associations(contact_id):
    """
    Retrieve associated companies for a given contact.
    """
    url = f"{BASE_URL}/crm/v4/objects/contacts/{contact_id}/associations/companies"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code == 200:
        return resp.json().get('results', [])
    elif resp.status_code == 404:
        return []
    else:
        raise Exception(f"Error fetching associations ({resp.status_code}): {resp.text}")

def check_association_exists(contact_id, company_id):
    """
    Check if a contact is associated with a company, and return labels if so.
    """
    associations = get_contact_company_associations(contact_id)
    for assoc in associations:
        if str(assoc.get('toObjectId')) == str(company_id):
            return assoc.get('labels', [])
    return None

def set_primary_label(contact_id, company_id):
    """
    Create or update a contact-company association with type 279 and 'primary' label.
    """
    ASSOCIATION_TYPE_ID = 1
    url = f"{BASE_URL}/crm/v4/objects/contacts/{contact_id}/associations/companies/{company_id}"
    payload = [
        {
            "associationCategory": "HUBSPOT_DEFINED",
            "associationTypeId": ASSOCIATION_TYPE_ID,
            "labels": [PRIMARY_LABEL_STR]
        }
    ]
    resp = requests.put(url, json=payload, headers=HEADERS)
    return resp


def main():
    with open(CSV_FILE, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            contact_id = row.get('contact_id')
            company_id = row.get('company_id')

            if not contact_id or not company_id:
                print(f"Skipping row with missing IDs: {row}")
                continue

            try:
                labels = check_association_exists(contact_id, company_id)
                if labels is None:
                    print(f"❌ No association exists for contact {contact_id} and company {company_id}")
                    continue

                if PRIMARY_LABEL_STR in labels:
                    print(f"✅ Association already primary for contact {contact_id} and company {company_id}")
                    continue

                resp = set_primary_label(contact_id, company_id)
                if resp.status_code in (200, 201, 204):
                    print(f"✅ Successfully set primary for contact {contact_id} and company {company_id}")
                else:
                    print(f"❌ Failed to set primary for contact {contact_id} and company {company_id}: {resp.status_code} {resp.text}")

            except Exception as e:
                print(f"❌ Exception for contact {contact_id} and company {company_id}: {e}")

if __name__ == "__main__":
    main()
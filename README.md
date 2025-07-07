# 📄 HubSpot Automation Scripts

A collection of internal-use Python scripts built for operational automation and data management within HubSpot CRM environments.

These scripts solve real-world business data problems — from merging duplicate records to cleaning up properties and automating admin tasks via the HubSpot API.

---

## 📂 Repository Contents

| Script Name               | Description                                                                 |
|:--------------------------|:----------------------------------------------------------------------------|
| `company_merge.py`         | Merges duplicate companies in HubSpot by reading pairs from a CSV file. Generates a new merged company record. Solved a deduplication issue after a Freshdesk import. |

---

## 📌 `company_merge.py` Details

**Purpose:**  
To merge duplicate HubSpot company records in bulk using the HubSpot API.

**How it works:**  
- Accepts a CSV file where:
  - **Column A** = HubSpot ID of Company 1  
  - **Column B** = HubSpot ID of Company 2  
- Merges each pair into a new record
- The old records remain untouched (or optionally can be removed via API later)
- HubSpot generates a new ID for the merged company

**Dependencies:**
- `requests`
- `pandas`
- HubSpot private app access token (passed securely via environment variable or config file)

**Security Note:**  
No access tokens are stored in this repository. Users must configure their own API credentials locally.

---

## 📌 Usage

```bash
python company_merge.py

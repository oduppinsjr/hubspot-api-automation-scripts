from hubspot import HubSpot
from hubspot.crm.companies import ApiException, SimplePublicObjectInput
from hubspot.crm.deals import BasicApi as DealsApi
from hubspot.crm.contacts import BasicApi as ContactsApi
from hubspot.crm.companies import BasicApi as CompaniesApi
from hubspot.crm.tickets import BasicApi as TicketsApi
from hubspot.crm.associations.v4.exceptions import ApiException as AssocApiException
import pandas as pd
from datetime import datetime

# HubSpot Access Token
ACCESS_TOKEN = '[API TOKEN]' 
client = HubSpot(access_token=ACCESS_TOKEN)

# Read CSV file with primary and duplicate company IDs
csv_path = 'company_merge_list_2.csv'
df = pd.read_csv(csv_path)

# Fetch all company properties dynamically
all_properties = [prop.name for prop in client.crm.properties.core_api.get_all('companies').results]

def get_readonly_properties():
    props_metadata = client.crm.properties.core_api.get_all('companies').results
    readonly_props = {
        p.name for p in props_metadata
        if getattr(p, 'readOnlyValue', False) or getattr(p, 'calculated', False)
    }
    return readonly_props

excluded_props = get_readonly_properties()

cannot_update_props = [
    'hs_analytics_first_timestamp', 'hs_analytics_last_timestamp',
    'hs_analytics_first_visit_timestamp', 'hs_analytics_last_visit_timestamp',
    'hs_last_sales_activity_timestamp', 'hs_last_sales_activity_date',
    'createdate', 'lastmodifieddate', 'hs_object_id',
    'hs_last_sales_activity_type', 'recent_conversion_event_name',
    'first_contact_createdate', 'num_conversion_events',
    'recent_conversion_date', 'hs_analytics_num_page_views',
    'first_conversion_date', 'hs_object_source_detail_1',
    'first_conversion_event_name', 'hs_analytics_num_visits',
    'notes_last_updated','num_contacted_notes','notes_last_contacted',
    'hs_notes_last_activity','hs_sales_email_last_replied','num_notes',
    'hs_last_logged_call_date','hs_updated_by_user_id','hs_last_logged_outgoing_email_date',
    'hs_merged_object_ids','hs_last_open_task_date','createdate', 'days_to_close', 'engagements_last_meeting_booked', 'engagements_last_meeting_booked_campaign', 'engagements_last_meeting_booked_medium', 'engagements_last_meeting_booked_source', 'first_contact_createdate', 'first_contact_createdate_timestamp_earliest_value_78b50eea', 'first_conversion_date', 'first_conversion_event_name', 'first_deal_created_date', 'hs_all_accessible_team_ids', 'hs_all_owner_ids', 'hs_all_team_ids', 'hs_analytics_first_timestamp', 'hs_analytics_first_touch_converting_campaign', 'hs_analytics_first_visit_timestamp', 'hs_analytics_last_timestamp', 'hs_analytics_last_timestamp_timestamp_latest_value_4e16365a', 'hs_analytics_last_touch_converting_campaign', 'hs_analytics_last_touch_converting_campaign_timestamp_latest_value_81a64e30', 'hs_analytics_last_visit_timestamp', 'hs_analytics_last_visit_timestamp_timestamp_latest_value_999a0fce', 'hs_analytics_latest_source', 'hs_analytics_latest_source_data_1', 'hs_analytics_latest_source_data_2', 'hs_analytics_latest_source_timestamp', 'hs_analytics_num_page_views', 'hs_analytics_num_page_views_cardinality_sum_e46e85b0', 'hs_analytics_num_visits', 'hs_analytics_num_visits_cardinality_sum_53d952a6', 'hs_analytics_source_data_1', 'hs_analytics_source_data_2', 'hs_annual_revenue_currency_code', 'hs_avatar_filemanager_key', 'hs_created_by_user_id', 'hs_createdate', 'hs_customer_success_ticket_sentiment', 'hs_date_entered_101563556', 'hs_date_entered_101571225', 'hs_date_entered_168011589', 'hs_date_entered_168011590', 'hs_date_entered_168017547', 'hs_date_entered_50419354', 'hs_date_entered_customer', 'hs_date_entered_evangelist', 'hs_date_entered_lead', 'hs_date_entered_marketingqualifiedlead', 'hs_date_entered_opportunity', 'hs_date_entered_other', 'hs_date_entered_salesqualifiedlead', 'hs_date_entered_subscriber', 'hs_date_exited_101563556', 'hs_date_exited_101571225', 'hs_date_exited_168011589', 'hs_date_exited_168011590', 'hs_date_exited_168017547', 'hs_date_exited_50419354', 'hs_date_exited_customer', 'hs_date_exited_evangelist', 'hs_date_exited_lead', 'hs_date_exited_marketingqualifiedlead', 'hs_date_exited_opportunity', 'hs_date_exited_other', 'hs_date_exited_salesqualifiedlead', 'hs_date_exited_subscriber', 'hs_intent_page_views_last_30_days', 'hs_intent_visitors_last_30_days', 'hs_is_enriched', 'hs_is_intent_monitored', 'hs_last_booked_meeting_date', 'hs_last_logged_call_date', 'hs_last_logged_outgoing_email_date', 'hs_last_open_task_date', 'hs_last_sales_activity_date', 'hs_last_sales_activity_timestamp', 'hs_last_sales_activity_type', 'hs_lastmodifieddate', 'hs_latest_createdate_of_active_subscriptions', 'hs_latest_meeting_activity', 'hs_merged_object_ids', 'hs_most_recent_de_anonymized_visit', 'hs_notes_last_activity', 'hs_notes_next_activity', 'hs_notes_next_activity_type', 'hs_num_blockers', 'hs_num_child_companies', 'hs_num_contacts_with_buying_roles', 'hs_num_decision_makers', 'hs_num_open_deals', 'hs_object_id', 'hs_object_source', 'hs_object_source_detail_1', 'hs_object_source_detail_2', 'hs_object_source_detail_3', 'hs_object_source_id', 'hs_object_source_label', 'hs_object_source_user_id', 'hs_parent_company_id', 'hs_predictivecontactscore_v2', 'hs_predictivecontactscore_v2_next_max_max_d4e58c1e', 'hs_read_only', 'hs_sales_email_last_replied', 'hs_source_object_id', 'hs_target_account_probability', 'hs_task_label', 'hs_time_in_101563556', 'hs_time_in_101571225', 'hs_time_in_168011589', 'hs_time_in_168011590', 'hs_time_in_168017547', 'hs_time_in_50419354', 'hs_time_in_customer', 'hs_time_in_evangelist', 'hs_time_in_lead', 'hs_time_in_marketingqualifiedlead', 'hs_time_in_opportunity', 'hs_time_in_other', 'hs_time_in_salesqualifiedlead', 'hs_time_in_subscriber', 'hs_total_deal_value', 'hs_unique_creation_key', 'hs_updated_by_user_id', 'hs_user_ids_of_all_notification_followers', 'hs_user_ids_of_all_notification_unfollowers', 'hs_user_ids_of_all_owners', 'hs_was_imported', 'hubspot_owner_assigneddate', 'hubspot_team_id', 'hubspotscore', 'notes_last_contacted', 'notes_last_updated', 'notes_next_activity_date', 'num_associated_contacts', 'num_associated_deals', 'num_contacted_notes', 'num_conversion_events', 'num_conversion_events_cardinality_sum_d095f14b', 'num_notes', 'recent_conversion_date', 'recent_conversion_date_timestamp_latest_value_72856da1', 'recent_conversion_event_name', 'recent_conversion_event_name_timestamp_latest_value_66c820bf', 'recent_deal_amount', 'recent_deal_close_date', 'total_revenue'
]

object_types = ['contacts', 'deals', 'tickets', 'notes', 'calls', 'emails', 'feedback_submissions']

association_type_ids = {
    ('contacts', 'companies'): 3,
    ('deals', 'companies'): 6,
    ('tickets', 'companies'): 16,
    ('calls', 'companies'): 21,
    ('emails', 'companies'): 22,
    ('notes', 'companies'): 27,
    ('feedback_submissions', 'companies'): 49
}

def parse_date(date_str):
    if not date_str:
        return None
    try:
        if 'T' in date_str:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return datetime.fromtimestamp(int(date_str) / 1000)
    except Exception:
        return None

def object_exists(object_type, object_id):
    try:
        if object_type == "deals":
            client.crm.deals.basic_api.get_by_id(object_id)
        elif object_type == "contacts":
            client.crm.contacts.basic_api.get_by_id(object_id)
        elif object_type == "companies":
            client.crm.companies.basic_api.get_by_id(object_id)
        elif object_type == "tickets":
            client.crm.tickets.basic_api.get_by_id(object_id)
        elif object_type in ["calls", "emails", "feedback_submissions"]:
            # Use generic CRM objects API for these object types
            client.crm.objects.basic_api.get_by_id(object_type, object_id)
        else:
            return False
        return True
    except ApiException as e:
        if e.status == 404:
            return False
        else:
            raise

# --- New function to pick the clean company name ---
def choose_clean_name(name_primary, name_duplicate):
    name_primary_lower = name_primary.lower() if name_primary else ''
    name_duplicate_lower = name_duplicate.lower() if name_duplicate else ''

    #primary_has_com = '.com' in name_primary_lower
    #duplicate_has_com = '.com' in name_duplicate_lower

    # If primary name contains .com and duplicate doesn't, pick duplicate name
    #if primary_has_com and not duplicate_has_com:
    #    return name_duplicate
    # If duplicate name contains .com and primary doesn't, keep primary name
    #elif duplicate_has_com and not primary_has_com:
    return name_primary
    # If both contain .com or neither contain .com, keep primary name
    #else:
    #    return name_primary

for index, row in df.iterrows():
    primary_id = str(row['Primary Company ID'])
    duplicate_id = str(row['Duplicate Company ID'])

    print(f"\n📌 Processing duplicate company {duplicate_id} into primary company {primary_id}")

    try:
        try:
            dup_company = client.crm.companies.basic_api.get_by_id(duplicate_id, properties=all_properties)
        except ApiException as e:
            if e.status == 404:
                print(f"⚠️ Duplicate company {duplicate_id} not found — skipping.")
                continue
            else:
                print(f"❌ Error retrieving duplicate company {duplicate_id}: {e}")
                continue

        try:
            primary_company = client.crm.companies.basic_api.get_by_id(primary_id, properties=all_properties)
        except ApiException as e:
            if e.status == 404:
                print(f"⚠️ Primary company {primary_id} not found — skipping.")
                continue
            else:
                print(f"❌ Error retrieving primary company {primary_id}: {e}")
                continue

        dup_created = parse_date(dup_company.properties.get('createdate'))
        primary_created = parse_date(primary_company.properties.get('createdate'))

        #if dup_created and primary_created and dup_created < primary_created:
        #    primary_id, duplicate_id = duplicate_id, primary_id
        #    primary_company, dup_company = dup_company, primary_company
        #    print(f"🔄 Swapped IDs: now merging company {duplicate_id} into {primary_id}")

        merged_props = {}

        primary_name = primary_company.properties.get('name')
        dup_name = dup_company.properties.get('name')
        clean_name = choose_clean_name(primary_name, dup_name)
        if clean_name and clean_name != primary_name:
            merged_props['name'] = clean_name

        for prop in all_properties:
            if prop in excluded_props or prop in cannot_update_props:
                continue
            if prop == 'name':
                continue
            dup_val = dup_company.properties.get(prop)
            primary_val = primary_company.properties.get(prop)
            if dup_val and (not primary_val or primary_val == ''):
                merged_props[prop] = dup_val

        update_successful = False
        association_move_failed = False

        if merged_props:
            try:
                client.crm.companies.basic_api.update(
                    primary_id,
                    SimplePublicObjectInput(properties=merged_props)
                )
                print(f"✅ Updated properties on company {primary_id}")
                update_successful = True
            except ApiException as e:
                print(f"❌ Failed to update company {primary_id}: {e}")
        else:
            print("ℹ️ No properties to update.")
            update_successful = True  # no updates needed is still considered a success

        if not update_successful:
            print(f"⚠️ Skipping associations and deletion for {duplicate_id} because property update failed.")
            continue  # safely skip to next merge

        for object_type in object_types:
            try:
                associations_response = client.crm.associations.v4.basic_api.get_page(
                    object_type=object_type,
                    object_id=duplicate_id,
                    to_object_type='companies',
                    limit=100
                )
                associations = associations_response.results
            except AssocApiException as e:
                print(f"❌ Failed to retrieve {object_type} associations for {duplicate_id}: {e}")
                association_move_failed = True
                break

            if associations:
                print(f"🔗 Found {len(associations)} {object_type} associations to move")
            else:
                print(f"ℹ️ No {object_type} associations found for {duplicate_id}")

            for assoc in associations:
                from_object_id = assoc.to_object_id
                assoc_key = (object_type, 'companies')
                assoc_type_id = association_type_ids.get(assoc_key)

                if not assoc_type_id:
                    print(f"⚠️ Association type ID not found for {object_type} -> companies, skipping.")
                    continue

                if not object_exists(object_type, from_object_id):
                    print(f"⚠️ Skipping association: {object_type} ID {from_object_id} does not exist.")
                    continue

                try:
                    client.crm.associations.v4.basic_api.create(
                        object_type=object_type,
                        object_id=from_object_id,
                        to_object_type='companies',
                        to_object_id=primary_id,
                        association_spec=[{
                            "associationCategory": "HUBSPOT_DEFINED",
                            "associationTypeId": assoc_type_id
                        }]
                    )
                    print(f"🔗 Re-associated {object_type} {from_object_id} to company {primary_id}")
                except AssocApiException as e:
                    print(f"❌ Failed to associate {object_type} {from_object_id} to company {primary_id}: {e}")
                    association_move_failed = True
                    break

            if association_move_failed:
                break

        if update_successful and not association_move_failed:
            try:
                client.crm.companies.basic_api.archive(duplicate_id)
                print(f"🗑️ Deleted duplicate company {duplicate_id}")
            except ApiException as e:
                print(f"❌ Failed to delete duplicate company {duplicate_id}: {e}")
        else:
            print(f"⚠️ Skipping deletion of {duplicate_id} due to prior failure(s).")

    except ApiException as e:
        print(f"❌ API Exception processing companies {primary_id} and {duplicate_id}: {e}")

print("\n✅ All merge operations completed.")

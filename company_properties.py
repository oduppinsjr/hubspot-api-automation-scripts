from hubspot import HubSpot
from hubspot.crm.properties import ApiException

ACCESS_TOKEN = '[ACCESS_TOKEN]'
client = HubSpot(access_token=ACCESS_TOKEN)

try:
    properties = client.crm.properties.core_api.get_all('companies').results
    readonly_props = []
    for p in properties:
        is_readonly = getattr(p.modification_metadata, 'read_only_value', False)
        if is_readonly or p.calculated:
            readonly_props.append(p.name)

    print(readonly_props)
except ApiException as e:
    print(f"API Error: {e}")
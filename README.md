# Download data from CQC API

Simple set of objects and functions to download all locations and provider data from the CCQ API and save to pickled files.

## Usage
i) download all the location ids and save to disk
```python
from common.get_Location_info import CQCAPI

cqcapi = CQCAPI()
cqcapi.get_all_locations()
```
ii) iterate through all the locations ids and get the details
```python
from common.get_Location_info import CQCAPI

cqcapi = CQCAPI()
cqcapi.load_from_local_file()
cqcapi.retrieve_location_details()
```
iii) iterate over the location details and get the provider information
```python
from common.get_provider_info import CQCProviderAPI

cqcproviderapi = CQCProviderAPI()
cqcproviderapi.get_all_prodivers()
```
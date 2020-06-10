"""this maps the API names to the original inpuit csv file"""
cols = dict(
    name='Name',
    postalAddressLine1='AddressLine1',
    postalAddressTownCity='AddressLine2',
    address='Address',
    postalCode='Postcode',
    mainPhoneNumber='Phone Number',
    gacServiceTypes='Service Types',
    lastInspection='Date of latest check',
    specialisms='Specialisms/services',  # looks like regulatedActivities but there is a value for 'Specialisms'
    brandName='Provider name',  # Need to confirm this
    localAuthority='Local Authority',
    region='Region',
    locationURL='Location URL',
    locationId='CQC Location (for office use only)',
    providerId='CQC Provider ID (for office use only)'
)

ratings_cols = dict(
    locationId='Org ID',
    name='Org Name',
    type='Org Type'
)


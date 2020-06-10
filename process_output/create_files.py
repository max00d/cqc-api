"""processes the raw data that has been downloaded from CQC API and produced the required output csv files"""
import pickle
import pandas as pd

from pathlib import Path
from process_output.cqc_columns_names import cols, ratings_cols


def produce_details(cols_to_create, location_resp, providers, location_id):
    """
    create detailed files
    """
    out_resp = {}
    for k, v in cols_to_create.items():
        if v == 'Service Types':
            # extract the name from the list of dicts in gaServiceTypes
            if location_resp.get(k):
                service_types = ''
                for i, st in enumerate(location_resp[k]):
                    service_types += st['name']
                    if (i + 1) != len(location_resp[k]):
                        service_types += ' | '
                out_resp[v] = service_types
        elif v == 'Date of latest check':
            if location_resp.get(k):
                tmp_date = location_resp[k]['date']
                tmp_date = tmp_date.replace('-', '/') + ' - 00:00'
                out_resp[v] = tmp_date
        elif v == 'Specialisms/services':
            if location_resp.get(k):
                specialisms, services = '', ''
                for idx, val in enumerate(location_resp[k]):
                    specialisms += val['name']
                    if (idx + 1) != len(location_resp[k]):
                        specialisms += ' | '
                for idx, val in enumerate(location_resp['regulatedActivities']):
                    services += val['name']
                    if (idx + 1) != len(location_resp['regulatedActivities']):
                        services += ' | '
                out_resp[v] = specialisms + ' | ' + services

        elif k == 'locationURL':
            out_resp[v] = 'https://www.cqc.org.uk/location/' + location_resp['locationId']

        elif v == 'Provider name':
            # use function to check for provider name using Id
            if providers.get(location_id):
                prov = providers[location_id]
                if prov.get('name'):
                    out_resp[v] = prov['name']

        elif v == 'Address':
            out_resp[v] = out_resp['AddressLine1'] + ', ' + out_resp['AddressLine2']
            del out_resp['AddressLine1']
            del out_resp['AddressLine2']
        else:
            if location_resp.get(k):
                out_resp[v] = location_resp[k]

    return out_resp


def produce_ratings(cols_to_create, resp, report_type='location'):
    """
    create ratings file
    """
    ratings_resp = []
    body = {}
    for k, v in cols_to_create.items():
        if resp.get(k):
            body[v] = resp[k]

    body['Report Type'] = report_type

    if resp.get('currentRatings'):
        current_ratings = resp['currentRatings']
        # current
        current_body = body.copy()

        if current_ratings['overall'].get('reportDate'):
            current_body['Publication Date'] = current_ratings['overall']['reportDate']

        current_body['Overall'] = current_ratings['overall']['rating']
        for report in current_ratings['overall']['keyQuestionRatings']:
            if report.get('name') and report.get('rating'):
                tmp_key = report['name']
                current_body[tmp_key] = report['rating']

        ratings_resp.append(current_body)

    if resp.get('historicRatings'):
        historic_ratings = resp['historicRatings']
        for report in historic_ratings:
            hist_body = body.copy()

            if report.get('reportDate'):
                hist_body['Publication Date'] = report['reportDate']

            if report.get('overall'):
                if report['overall'].get('rating'):
                    hist_body['Overall'] = report['overall']['rating']

                if report['overall'].get('keyQuestionRatings'):

                    for val in report['overall']['keyQuestionRatings']:
                        tmp_key = val['name']
                        hist_body[tmp_key] = val['rating']

            ratings_resp.append(hist_body)

    return ratings_resp


def main(location_file='location_ratings', provider_file='providers_info_all', rename_cols=cols,
         ratings_columns=ratings_cols):
    """read in pickled raw data and produce the detailed and summary files"""
    # open pickled json api responses
    pth = Path(__file__).parent.parent
    raw_locations = pickle.load(open(f'{pth}/{location_file}.pickle', 'rb'))
    raw_providers = pickle.load(open(f'{pth}/{provider_file}.pickle', 'rb'))

    # providers = get_provider_details()
    providers = raw_providers

    # place holders for processed files
    output_file_details = []
    ratings_file = []

    no_locations = len(raw_locations)

    # loop through the raw responses dictionary
    for i, k in enumerate(raw_locations):
        # check registrationStatus and only include responses that are 'Registered'
        # first check that this key is in the dict
        if not raw_locations[k].get('registrationStatus'):
            continue
        # now check the status
        if raw_locations[k]['registrationStatus'] != 'Registered':
            continue

        # create file with details
        details_resp = produce_details(rename_cols, raw_locations[k], providers, k)
        # print(details_resp)
        output_file_details.append(details_resp)

        ratings_info = produce_ratings(ratings_columns, raw_locations[k])
        ratings_file += ratings_info

        if i % 1000 == 0:
            print(f'at location {i} of {no_locations}, {i / no_locations * 100:.2f}% complete')

    # use pandas to convert to dataframe and save as csv
    df_details = pd.DataFrame(output_file_details)
    df_ratings = pd.DataFrame(ratings_file)
    df_details.to_csv(f'{pth}/details_updated.csv', index=False)
    df_ratings.to_csv(f'{pth}/ratings_updated.csv', index=False)

    return raw_locations, output_file_details, ratings_file


if __name__ == '__main__':
    raw, details, ratings = main()

"""
full location information is available from https://api.cqc.org.uk/public/v1/locations
get each location entry parssing the response and the procedding to the next nextPageUri
returns 1k repsonses per page so c.94 pages in total
stores in pickle file to use additional option of using local file or using this gile
"""
import requests
import pickle
import multiprocessing
import time

from pathlib import Path


class CQCAPI:
    base_url = 'https://api.cqc.org.uk/public/v1'
    providers = '/providers'
    locations = '/locations'

    pth = Path(__file__).parent.parent

    def __init__(self):
        """
        args:
        use_local: use local file to get all providers to query
        """
        self.all_locations = []
        self.locations_results_all = None

    def get_all_locations(self):
        """get all locatons in a json dump"""
        found_all = False
        next_uri = self.locations
        itr = 1

        # first run
        resp = requests.get(self.base_url + next_uri)
        resp = resp.json()
        total_locations = resp['totalPages']
        self.all_locations += resp['locations']

        while not found_all:
            if resp.get('nextPageUri'):
                next_uri = resp['nextPageUri']
                have_resp = False
                while not have_resp:
                    try:
                        resp = requests.get(self.base_url + next_uri)
                        print(f'at location {itr} of {total_locations}')
                        resp = resp.json()
                        self.all_locations += resp['locations']
                        have_resp = True
                        itr += 1
                    except requests.exceptions.ConnectionError as e:
                        print(f'Getting all locations - Max retries attempted @ {itr} of {total_locations}, '
                              f'waiting for 10 seconds')
            else:
                found_all = True

        self.save_to_pickle(self.all_locations, 'all_locations')

    def save_to_pickle(self, obj, fn):
        """
        save the results of the location to disk"
        :param obj: name of the data object to save
        :param fn: name of the pickle file
        """
        with open(f'{self.pth}/{fn}.pickle', 'wb') as f:
            pickle.dump(obj, f)

    def load_from_local_file(self, fn='all_locations'):
        """load the all_location from disk rather than downloading from API"""
        self.all_locations = pickle.load(open(f'{self.pth}/{fn}.pickle', 'rb'))

    def _handle_location_split(self, location_ids, running_dict, total_ids, start_id, retry_wait=15):
        """
        for each set of location_ids iterate through and get the response - if we get banned then wait and retry until
        we get the response
        :param location_ids: list of ids that are to be processed in this 'chunk'
        :param running_dict: dictionay to write results that is shared across the processes
        :param total_ids:  total no ids this is responsible - used to show progress
        :param start_id: start id
        :param retry_wait: length of time in seconds that is waited after being ip banned
        :return: all responses from this process that are written to the shared sict
        """
        for id in location_ids:
            request_url = f'{self.base_url}/locations/{id}'
            have_resp = False
            try:
                print(f'getting response from {id}, no: {start_id} of {total_ids}, {start_id / total_ids * 100:.1f}%')
                loc_resp = requests.get(request_url)
                running_dict[id] = loc_resp.json()
                have_resp = True
                start_id += 1
            except requests.exceptions.ConnectionError as e:
                print(f'Max retries attempted @ {id}, waiting for {retry_wait} seconds')
                time.sleep(retry_wait)

    def retrieve_location_details(self, chunk_size=4_000):
        """
        for each location is in the self.all_locations file query each on in order and store the results
        this is relatively crude and using multiproccing to hit the API and get the data - this will flood the API
        and a temporary block will be put againts the ip address.  So wait and then retry until we get a response.

        chunk_size: controls the number of ids that each process is responsible for (proxy for number of processes to
                    use)
        """
        manager = multiprocessing.Manager()
        running_dict = manager.dict()
        jobs = []
        for i in range(0, len(self.all_locations), chunk_size):
            chunk_id = [l['locationId'] for l in self.all_locations[i:i + chunk_size]]
            p = multiprocessing.Process(target=self._handle_location_split,
                                        args=(chunk_id, running_dict, len(chunk_id), 1))
            jobs.append(p)

        for job in jobs:
            job.start()
        for job in jobs:
            job.join()

        self.locations_results_all = running_dict.copy()

        self.save_to_pickle(self.locations_results_all, 'location_ratings')


if __name__ == '__main__':
    cqc = CQCAPI()
    cqc.load_from_local_file()
    cqc.retrieve_location_details()

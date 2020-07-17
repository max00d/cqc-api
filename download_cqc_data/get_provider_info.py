"""
get the details of providers - this has to be done this way so that the summary data that is produced is up to date

depandancies:
CQCAPI.retrieve_location_details() has been run and the file is on disk
"""
import multiprocessing
import requests
import time
import pickle

from download_cqc_data.get_Location_info import CQCAPI


class CQCProviderAPI(CQCAPI):

    def __init__(self):
        super().__init__()
        self.all_providers = []
        self.locations_results_all = None
        self.provider_results = []

    def _load_location_results(self, fn='location_ratings'):
        """
        load the location results produced from CQCAPI.retrieve_location_details()
        read these in and get all the provider info by location - very long-winded and will produce redundancy in the
        data that could be optimsed.  Required to get the most up to data when processing the responses
        """
        self.locations_results_all = pickle.load(open(f'{self.pth}/{fn}.pickle', 'rb'))
        print(len(self.locations_results_all))

    def _handle_prodivder_split(self, process_no, id_list, running_dict, total_ids, start_id, retry_wait=15,
                                print_every=20):
        """

        :param process_no:
        :param id_list:
        :param running_dict:
        :param total_ids:
        :param start_id:
        :param retry_wait:
        :param print_every:
        :return:
        """
        for idx, id in enumerate(id_list):
            request_url = f'{self.base_url}/providers/{id[0]}'
            have_resp = False
            while not have_resp:
                try:
                    if (idx + 1) % print_every == 0:
                        print(f'process {process_no}: getting response from {id}, no: {start_id} of {total_ids}, '
                              f'{start_id / total_ids * 100:.1f}%')
                    loc_resp = requests.get(request_url)
                    if loc_resp.json().get('statusCode'):
                        print(loc_resp.json())
                        spl = loc_resp.json().get('message').split()
                        print(f'sleeping for {int(spl[len(spl) - 2])} seconds')
                        time.sleep(int(spl[len(spl) - 2]))
                    else:
                        running_dict[id[1]] = loc_resp.json()
                        have_resp = True
                        start_id += 1
                except requests.exceptions.ConnectionError as e:
                    print(f'Max retries attempted @ {id}, waiting for {retry_wait} seconds')
                    time.sleep(retry_wait)

    def get_all_providers(self, chunk_size=10_000):
        """
        get provider details at the level of location - load the results of CQCAPI.retrieve_location_details()
        loads pickled file into memory and iterated through to get the prover details
        :param chunk_size: proxy for number of processed to use - length / chunk_size = np; higher val == less
        :return: saves all provider info into a pickled file
        """
        self._load_location_results()
        for idx, k in enumerate(self.locations_results_all.keys()):
            if self.locations_results_all[k].get('providerId') and self.locations_results_all[k].get('locationId'):
                d = dict(providerId=self.locations_results_all[k]['providerId'],
                         locationId=self.locations_results_all[k]['locationId'])
                l = [self.locations_results_all[k]['providerId'],
                     self.locations_results_all[k]['locationId']]
                self.all_providers.append(l)
        manager = multiprocessing.Manager()
        running_dict = manager.dict()
        jobs = []

        for c, i in enumerate(range(0, len(self.all_providers), chunk_size)):
            chunk_id = self.all_providers[i:i + chunk_size]
            p = multiprocessing.Process(target=self._handle_prodivder_split,
                                        args=(c, chunk_id, running_dict, len(chunk_id), 1))
            jobs.append(p)

        for job in jobs:
            job.start()
        for job in jobs:
            job.join()

        self.provider_results = running_dict.copy()

        self.save_to_pickle(self.provider_results, 'providers_info_all')


if __name__ == '__main__':
    api = CQCProviderAPI()
    api.get_all_providers()
    # api.get_all_providers()

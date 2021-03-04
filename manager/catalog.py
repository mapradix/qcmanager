#!/usr/bin/env python3

import requests
import json

if __name__ == "__main__":
    # for testing purposes only
    import os
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from manager.exceptions import CatalogError
from manager.logger import Logger


class QCCatalogPoller:
    """Catalog access (IF-CAT-API).
    
    :param dict config: catalog config section
    """
    def __init__(self, config):
        self._url = config.get('url')
        if self._url:
            try:
                self._user = config['user']
                self._password = config['password']
            except KeyError as e:
                raise CatalogError(
                    "Catalog user or password not defined"
                )
            
        # list of registered datasets
        self._datasets = {}

    def get_datasets(self, parent_identifier):
        """Get list of datasets by parent identifier.

        :param str parent_identifier: parent identifier

        :return list: list of datasets (IP products)
        """
        url = '{}/series/{}/datasets'.format(
            self._url, parent_identifier
        )
        Logger.debug("Catalog URL {}".format(url))

        ret = requests.get(url,
                           auth=(self._user, self._password),
                           params={'maximumRecords': 1000}
        )

        response = json.loads(ret.text)
        found = False
        
        datasets = []
        for item in response['features']:
            datasets.append(
                item['properties']['identifier']
            )
        Logger.debug("Datasets in catalog for {} ({}): {}".format(
            parent_identifier, len(datasets), ','.join(datasets)
        ))
            
        return datasets

    def query(self, dataset, parent_identifier):
        """Query dataset.

        :param str dataset: dataset name to be queried
        :param str parent_indentifier: parent idenfifier to be queried

        :return dict: catalog response
        """
        if not self._url:
            Logger.debug("Catalog URL not defined in configuration. "
                         "No connection established")
            return

        url = '{}/series/{}/datasets/{}'.format(
            self._url, parent_identifier, dataset
        )
        Logger.debug("Catalog URL {}".format(url))

        ret = requests.get(url,
                           auth=(self._user, self._password)
        )
        Logger.debug("Catalog return code: {}".format(ret.status_code))

        return json.loads(ret.text)
        
    def send(self, json_data):
        """Update/insert dataset in catalog.

        Raise CatalogError on failure.

        :param dict json_data: JSON data to be sent
        """
        if not self._url:
            Logger.debug("Catalog URL not defined in configuration. "
                         "No connection established")
            return

        parent_identifier = json_data['properties']['parentIdentifier']
        if parent_identifier not in self._datasets:
            # get list of registered datasets if not defined
            self._datasets[parent_identifier] = self.get_datasets(parent_identifier)

        # update or insert dataset
        dataset = json_data['properties']['identifier']
        update = dataset in self._datasets[parent_identifier]

        if update:
            # already exists -> update
            url = '{}/series/{}/datasets/{}'.format(
                self._url, parent_identifier, dataset
            )
            requests_fn = requests.put
        else:
            url = '{}/series/{}/datasets'.format(
                self._url, parent_identifier
            )
            requests_fn = requests.post
        Logger.debug("Catalog URL (update={}) {}".format(update, url))
        ret = requests_fn(url,
                          auth=(self._user, self._password),
                          headers={'Content-type': 'application/geo+json'},
                          data=json.dumps(json_data)
        )
        Logger.debug("Catalog response: {}".format(ret.text))
        try:
            ret.raise_for_status()
            Logger.info("Catalog response ({}) success".format(
                'update' if update else 'insert'
            ))
        except requests.exceptions.HTTPError as e:
            raise CatalogError(
                "Catalog response failure: {}".format(e)
            )


# testing purposes
if __name__ == "__main__":
    def check_response(resp):
        dataset = os.path.splitext(
            os.path.basename(resp)
        )[0]
        Logger.info("Processing {}...".format(dataset))
        with open(resp) as fd:
            data = json.load(fd)
        parent_identifier = data['properties']['parentIdentifier']
        response = catalog.query(dataset, parent_identifier)
        if not args.quiet:
            print(
                json.dumps(
                    response,
                    indent=4, sort_keys=True),
                file=sys.stderr
            )
        datasets = catalog.get_datasets(parent_identifier)
        Logger.debug("Dataset in series: {}".format(dataset in datasets))

    def insert_dataset(resp):
        print ("Processing {}...".format(resp))
        with open(resp) as fd:
            data = json.load(fd)
        catalog.send(
            json_data=data
        )

    ###
    ### MAIN
    ###
    # for testing purposes only
    import glob
    import argparse

    from manager.config import QCConfigParser
    from manager.catalog import QCCatalogPoller

    parser = argparse.ArgumentParser(description="Test catalog access.")
    parser.add_argument(
        '-c', '--config',
        help="Use case config file(s)",
        type=str,
        default='config.yaml,use_cases/tuc1_imd_2018_010m/tuc1_imd_2018_010m_prague.yaml,mxserver.yaml'
    )
    parser.add_argument(
        '-d', '--directory',
        help="Directory with JSON files",
        type=str
    )
    parser.add_argument(
        '-q', '--quiet',
        action='store_true',
        help="Do not print responses",
    )
    parser.add_argument(
        '-i', '--insert',
        action='store_true',
        help="Enable insert/update mode (WRITE ACCESS)",
    )
    parser.add_argument(
        '-p', '--parent_identifier',
        type=str,
        help="Parent identifier to be checked (eg. EOP:SCIHUB:S2:UC1)",
    )
    args = parser.parse_args()

    # check options
    files = None
    if args.directory:
        if args.parent_identifier:
            sys.exit("Options --directory and --parent_identifier are mutually exclusive")
        if not os.path.isdir(args.directory):
            sys.exit("Input directory not exists")

        files = glob.glob(os.path.join(args.directory, '*.json'))
        if not files:
            sys.exit("No JSON files found")
    elif not args.parent_identifier:
        sys.exit("Either --directory or --parent_identifier must be given")
    elif args.parent_identifier and args.insert:
        sys.exit("Options --insert and --parent_identifier are mutually exclusive")

    # read configuration
    config = QCConfigParser(
        args.config.split(',')
    )
    # establish catalog poller
    catalog = QCCatalogPoller(
        config['catalog']
    )

    if files:
        process_response = insert_dataset if args.insert else check_response
        # loop over JSON files -> check catalog response
        for resp in files:
            process_response(resp)
    else:
        catalog.get_datasets(args.parent_identifier)


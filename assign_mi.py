"""
Assigns a patient specific MI number for a give process ID
Extends and inherits logging config from RecordWriter
"""

__author__ = "William Lyman"
__version__ = "1.0 for Thermo Fischer COVID-19 Interpretive Software v1.1"

import argparse
from pullRecords import RecordWriter
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as ET
import requests
import logging
import yaml
from filelock import FileLock
from yaml import load
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


class assignMI(RecordWriter):
    def __init__(self, process_id, mi_file_path, environment='dev',
                 config_file="config/config.yaml"):

        self.env = environment
        self.config_file = config_file
        self.cfg = None
        self.process_id = process_id
        self.environment = environment
        self._read_config()
        self.my_auth = HTTPBasicAuth(self.cfg[self.env]['username'],
                                     self.cfg[self.env]['pass'])
        self.mi_file_path = mi_file_path
        self.mi_number = None
   
    def process_xml(self, response):
        padded = str(self.mi_number).zfill(5)
        root = ET.fromstring(response.content)  
        udf = '{http://genologics.com/ri/userdefined}field'
        found = False
        for x in root.findall(udf):
            if x.attrib['name'] == "Customer Sample Name":
                x.attrib['type'] = 'String'
                x.text = "MI20-{}".format(padded)
                found = True
                break
        if not found:
            x = ET.Element(udf)
            x.attrib['name'] = "Customer Sample Name"
            x.attrib['type'] = 'String'
            x.text = "MI20-{}".format(padded)
            root.append(x)
        self.mi_number += 1
        return root

    def add_record(self, sample_list):
        for sample in sample_list:
            response = \
                requests.get("{}samples/{}".
                             format(self.cfg[self.env]['clarity_url'], sample),
                             auth=self.my_auth)
            new_root = self.process_xml(response)
            xmlstr = ET.tostring(new_root, encoding='utf8', method='xml')
            headers = {'Content-Type': 'application/xml'}
            response2 = requests.put(
                self.cfg[self.env]['clarity_url'] + 'samples/' + sample,
                data=xmlstr,
                headers=headers,
                auth=self.my_auth
            )

            if 200 != response2.status_code:
                msg = 'Error sample: {} {}'.format(sample, response2.content)
                logging.debug(msg)

    def read_mi(self):
        fname = open(self.mi_file_path, "r")   # with caused issues with lock
        mi_file = load(fname.read(), Loader=Loader)
        self.mi_number = (mi_file['mi_number'])
        fname.close()

    def write_mmi(self):
        fname = open(self.mi_file_path, 'w')
        ydict = {'mi_number': self.mi_number}
        ydump = yaml.dump(ydict, fname)
        fname.close()


def main():
    parser = argparse.ArgumentParser(
        description="For a process id get sample information from Clarity and "
                    "then prepare these records for the docusign reports.")
    parser.add_argument('process_id', type=str,
                        help='Name of the process id in Clarity')
    parser.add_argument('--environment', type=str, default='dev',
                        help='The environment value must be "dev" or "prod".',
                        choices=['dev', 'prod'])
    parser.add_argument('--config_file', type=str,
                        default='./config/config.yaml',
                        help='Full path to the configuration file. '
                             'Default is config/config.yaml')
    parser.add_argument('--mi_file', type=str,
                        default='./config/mi_file.yaml',
                        help='Full path to the mi_lock file. '
                             'Default is ./config/mi_file.yaml')
    args = parser.parse_args()
    lock = FileLock(args.mi_file + ".lock")
    with lock:
        am = assignMI(process_id=args.process_id,
                      environment=args.environment,
                      config_file=args.config_file,
                      mi_file_path=args.mi_file)
        am.read_mi()
        am.mi_number += 1
        sample_list = (am.get_sample_urls())
        sample_set=list(set(sample_list))
        sample_set.sort()
        
        am.add_record(sample_list)
        am.write_mmi()


if __name__ == '__main__':
    main()

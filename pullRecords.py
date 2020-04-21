"""
Assigns a patient specific MI number for a give process ID
"""

__author__ = "William Lyman"
__version__ = "1.0 for Thermo Fischer COVID-19 Interpretive Software v1.1"


import argparse
import datetime
import json
import logging
import os
import requests
from requests.auth import HTTPBasicAuth
import sys
import xml.etree.ElementTree as ET
import xmltodict
from yaml import load

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


# TODO: stealing Jake's logging and exception handling.  We should consolidate
# TODO: logging and the all_handler to a shared util function that is run at
# TODO: the top of our code. Thoughts @JakeEmerson @WilliamLyman?
# added a check to make sure logs dir exists, create if it doesn't (if we can).
if not os.path.exists('logs'):
    try:
        os.mkdir('logs')
    except PermissionError as pe:
        sys.stderr.write("Error creating log dir in {}: {}".
                         format(os.getcwd(), pe))

# Set up a happy little log file
logging.basicConfig(
    filename='logs/pullRecords.log', filemode='a',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


def all_handler(exc_type, exc_value, traceback, logger=logging):
    """
    The logfile all handler captures the output from uncaught exceptions.
    It handles all the things. Sort of like the Allfather.
    """
    logging.error("Logging an uncaught exception",
                  exc_info=(exc_type, exc_value, traceback))


# Install exception handler
sys.excepthook = all_handler

class RecordWriter:
    """
    Given a Clarity process id, pull sample information and generate output
    to be used by the docusign process.
    """

    def __init__(self, process_id, output_path, version, environment='dev',
                 config_file="config/config.yaml"):
        """
        Initialization of the RecordWriter
        :param process_id: process id in Clarity
        :param output_path: the output file path for writing results
        :param version: version of output (v1 or v2)
        :param environment: The environment value must be "dev" or "prod".
        :param config_file: Full path to the configuration file.
            Default is config/config.yaml
        """
        self.process_id = process_id
        self.output_path = output_path
        self.version = version
        self.env = environment
        self.config_file = config_file
        self.log_file_path = None
        self.cfg = None  # configuration object read from the yaml file
        # name the file will be written to.  Assigned in get_sample_json
        self.outfile = None

        # make sure the config file is actually there
        if not os.path.exists(self.config_file):
            msg = "The config file '{}' does not exist.".\
                format(self.config_file)
            logging.error(msg)
            raise EnvironmentError(msg)

        self._read_config()

        self.my_auth = HTTPBasicAuth(self.cfg[self.env]['username'],
                                     self.cfg[self.env]['pass'])

    def _read_config(self):
        """
        Read in the config from the default or user-supplied yaml file
        """
        with open(self.config_file, 'r') as fname:
            self.cfg = load(fname, Loader=Loader)

        self.log_file_path = self.cfg['log_file_path']
        full_path = os.path.abspath(self.log_file_path)

        if os.path.exists(full_path) and os.path.isfile(full_path):
            # make a new handler with the updated file
            newf = logging.FileHandler(full_path, 'a')

        elif os.path.exists(full_path) and os.path.isdir(full_path):
            # if the default file exists in the directory, keep using that
            default_log = os.path.join(full_path, 'pullRecords.log')
            if os.path.exists(default_log):
                newf = logging.FileHandler(default_log, 'a')

            # otherwise, create the default log file in a new handler
            else:
                newf = logging.FileHandler(default_log, 'w')

        else:
            msg = 'The log file could not be created or found. ' \
                  'Please supply log file path in your config file.'
            raise EnvironmentError(msg)

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        newf.setFormatter(formatter)

        lg = logging.getLogger()
        hd = lg.handlers[0]
        lg.removeHandler(hd)
        lg.addHandler(newf)
        # set up the all_handler again because we replaced the default handler
        sys.excepthook = all_handler

        if 'debug' == self.cfg[self.env]['log_level']:
            logging.getLogger().setLevel(logging.DEBUG)
        elif 'warn' == self.cfg[self.env]['log_level']:
            logging.getLogger().setLevel(logging.WARN)
        elif 'info' == self.cfg[self.env]['log_level']:
            logging.getLogger().setLevel(logging.INFO)
        elif 'error' == self.cfg[self.env]['log_level']:
            logging.getLogger().setLevel(logging.ERROR)
        elif 'critical' == self.cfg[self.env]['log_level']:
            logging.getLogger().setLevel(logging.CRITICAL)

        logging.debug('Log path: {}'.format(self.log_file_path))

    def _get_sample_ids(self, urls):
        """
        private class method that gets each sample that is associated with
        a process id.
        :param urls: list of URLs pulled from clarity for associated samples
        :return: list of sample ids
        """
        sample_list = []
        for url in urls:
            response = requests.get(url, auth=self.my_auth)
            if response.status_code != 200:
                msg = "Call to {} failed".format(url)
                logging.error(msg)
                raise EnvironmentError(msg)

            root = ET.fromstring(response.content)
            for child in root:
                if 'limsid' in child.attrib:
                    logging.debug(child.attrib['limsid'])
                    sample_list.append(child.attrib['limsid'])
        return sample_list

    def get_sample_urls(self):
        """
        takes the process id which is associated with this RecordWriter
        queries clarity, and returns the list of samples
        :return: list of samples
        """
        sample_url = "{}processes/{}".format(self.cfg[self.env]['clarity_url'],
                                             self.process_id)
        response = requests.get(sample_url, auth=self.my_auth)
        if response.status_code != 200:
            msg = "Call to {sample_url} failed".format(sample_url)
            try:
                result = (xmltodict.parse(response.content))
                if "Process not found:" in result['exc:exception']['message']:
                    msg = result['exc:exception']['message']
            finally:
                logging.error(msg)
                raise EnvironmentError(msg)

        root = ET.fromstring(response.content)
        sample_urls = []
        for child in root.iter('input'):
            if 'post-process-uri' in child.attrib:
                logging.debug(child.attrib['post-process-uri'])
                sample_urls.append(child.attrib['post-process-uri'])

        sample_list = (self._get_sample_ids(sample_urls))
        return sample_list

    def get_sample_json(self, sample_list):
        """
        takes a list of samples and generates a JSON object with details for
        reporting purposes.  The JSON is written to a file in the output path
        provided, and the JSON is also emitted from the function
        :param sample_list: list of samples associated with the process id
        :return: a JSON object for reporting purposes
        """
        if self.version.lower() == "v1":
            required_fields = ['Physician Phone #', 'Patient Name',
                               'Customer Sample Name', 'Date of Birth', 'Sex',
                               'Specimen Site', 'Receipt Date', 'Received Time',
                               'Physician', 'Physician Institution',
                               'Collection Date', 'Collection Time',
                               'Final.Result', 'Batch ID', 'Batch QC Result',
                               'ORF1ab', 'ORF1ab_STATUS', 'N_Protein',
                               'N_Protein_STATUS', 'S_Protein',
                               'S_Protein_STATUS', 'MS2', 'MS2_STATUS',
                               'Status', 'Medical Record Number']
        elif self.version.lower() == "v2":
            required_fields = ['Physician Phone #', 'Patient Name',
                               'Customer Sample Name', 'Date of Birth', 'Sex',
                               'Specimen Site', 'Receipt Date', 'Received Time',
                               'Physician', 'Physician Institution',
                               'Collection Date', 'Collection Time',
                               'Final.Result', 'Batch ID', 'Batch QC Result',
                               'Status', 'Medical Record Number']
        else:
            raise Exception("The version passed is not valid: {}".
                            format(self.version))

        json_list = []
        for sample in sample_list:
            response = \
                requests.get("{}samples/{}".
                             format(self.cfg[self.env]['clarity_url'], sample),
                             auth=self.my_auth)
            result_json = (xmltodict.parse(response.content))
            for value in (result_json['smp:sample']['udf:field']):
                if value['@name'] in required_fields and \
                        (value['#text'].lower() == 'none' or
                         value['#text'] == ''):
                    msg = "The following required field is none or empty: " \
                          "{} for Sample: {}".format(value['@name'], sample)
                    logging.error(msg)
                    raise EnvironmentError(msg)

            this_version={"@type": "String", "@name": "Template_version", "#text": self.version}
            result_json['smp:sample']['udf:field'].append(this_version)
            json_list.append(result_json)

        time_stamp = datetime.datetime.now()
        self.outfile = "{}{}.txt".\
            format(self.output_path, time_stamp.strftime('%d_%B_%Y_%m%s'))
        json_file = open(self.outfile, 'w')
        json_file.write(json.dumps(json_list))
        json_file.close()

        print("Job was successful!")           #Gets returned to Clarity
        return json_list


def main():
    parser = argparse.ArgumentParser(
        description="For a process id get sample information from Clarity and "
                    "then prepare these records for the docusign reports.")
    parser.add_argument('process_id', type=str,
                        help='Name of the process id in Clarity')
    parser.add_argument('output_path', type=str,
                        help='Where to write results.')
    parser.add_argument('version', type=str, default='v2',
                        help='Specify version to process data with',
                        choices=['v1', 'v2'])
    parser.add_argument('--environment', type=str, default='dev',
                        help='The environment value must be "dev" or "prod".',
                        choices=['dev', 'prod'])
    parser.add_argument('--config_file', type=str,
                        default='./config/config.yaml',
                        help='Full path to the configuration file. '
                             'Default is config/config.yaml')
    args = parser.parse_args()

    rw = RecordWriter(
        process_id=args.process_id,
        output_path=args.output_path,
        version=args.version,
        environment=args.environment,
        config_file=args.config_file
    )

    sample_list = rw.get_sample_urls()
    rw.get_sample_json(sample_list)


if __name__ == '__main__':
    main()

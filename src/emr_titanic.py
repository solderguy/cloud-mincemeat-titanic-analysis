#!/usr/bin/env python
"""
CREDENTIALS
  Module: emr_titanic.py
  Author: John Soper
  Date: Apr 5, 2014
  Rev: 2

SUMMARY
    Analysis of Mortality Characteristics of Titanic Passengers
    This python script is called by gui_titanic.py and runs embedded in it
    It's main function is to run a map-only Hadoop streaming job in the
    Python language on Amazon Web Service's Elastic MapReduce
    It can also run on the command line with default values

REQUIREMENTS
  A .boto file in your home directory with the following fields updated to
  those of your AWS account:
	[Credentials]
	aws_access_key_id = xxxxxxxxxx
	aws_secret_access_key = xxxxxxxxxxxxxxxxxxxxx
  If this is not to your liking, you can pass the credentials in some other way

  Some other infrastructure I needed to do, you might or might not have to:
     sudo apt-get install python-pip
     sudo pip install boto
     sudo apt-get install python-tk

"""

import sys
import os
import glob
from time import sleep
from uuid import getnode as get_mac
import boto
from boto.emr import connect_to_region
from boto.emr.step import StreamingStep


class EmrProcessing(object):
    """ Self contained AWS Elastic MapReduce and S3 code"""
    bucket_name = None
    bucket = None

    def __init__(self):
        self.s3_handle = None
        self.jobid = None
        self.conn = None
        self.region = None
        self.region_name = None
        self.verbose_mode = None
        self.model_choice = None

    def parse_user_selections(self):
        """ Applies command line arguments (or defaults) to program logic"""
        if "model2" in sys.argv:
            self.model_choice = "model2"
        else:
            self.model_choice = "model1"

        if "Virginia" in sys.argv:
            self.region = "Virginia"
            self.region_name = 'us-east-1'
        elif "California" in sys.argv:
            self.region = "California"
            self.region_name = 'us-west-1'
        else:
            self.region = "Oregon"
            self.region_name = 'us-west-2'

        if self.verbose_mode:
            print "** will run the Machine Learning %s" % self.model_choice
            print "\n** Running on %s Elastic Map Reduce server" % self.region

    @staticmethod
    def clear_local_output_directory():
        """ Makes sure no stale files are in local directory"""
        output_path = '../output/*'
        files = glob.glob(output_path)
        for single_file in files:
            os.remove(single_file)

    @staticmethod
    def print_local_output_files_stats():
        """ Lets user see generated file names and sizes at a glance"""
        print "\n\nFILES CREATED:"
        for filename in os.listdir('../output'):
            filesize = os.path.getsize('../output/' + filename)
            print str(filesize) + "\t" + filename
        print "\n"

    @staticmethod
    def generate_unique_name():
        """ Makes name from MAC address - unique to each network card"""
        return 'titanic-' + str(get_mac())

    def empty_bucket(self):
        """ Clears and files from the S3 bucket"""
        self.s3_handle = boto.connect_s3()
        EmrProcessing.bucket_name = self.generate_unique_name()
        EmrProcessing.bucket = \
            self.s3_handle.create_bucket(EmrProcessing.bucket_name)
        EmrProcessing.bucket.delete_keys([key.name \
            for key in EmrProcessing.bucket])

    def create_and_fill_bucket(self):
        """ Creates bucket if needed and transfers mapper file"""
        EmrProcessing.bucket = \
            self.s3_handle.create_bucket(EmrProcessing.bucket_name)
        key = EmrProcessing.bucket.new_key('input/test.csv')
        input_file_path = '../data/test.csv'
        key.set_contents_from_filename(input_file_path)
        key.set_acl('public-read')

        key = EmrProcessing.bucket.new_key('mapper/mapper.py')
        input_file_path = '../src/mapper/mapper.py'
        key.set_contents_from_filename(input_file_path)
        key.set_acl('public-read')

    def setup_and_run_job(self):
        """ Runs the Elastic MapReduce job on AWS"""
        step = StreamingStep(name='Titanic Machine Learning',
            mapper='s3n://'  + EmrProcessing.bucket_name + '/mapper/mapper.py',
            reducer='org.apache.hadoop.mapred.lib.IdentityReducer',
            input='s3n://'  + EmrProcessing.bucket_name + '/input/',
            output='s3n://' + EmrProcessing.bucket_name + '/output/')
        self.conn = connect_to_region(self.region_name)
        self.jobid = self.conn.run_jobflow(name='Titanic Devp',
            log_uri='s3://' + EmrProcessing.bucket_name +
                '/jobflow_logs', steps=[step])

    def wait_until_job_completes(self):
        """ Provides EMR status updates at 10 second intervals """
        while True:
            jobflow = self.conn.describe_jobflow(self.jobid)
            if self.verbose_mode:
                print jobflow.state
            if (jobflow.state == 'COMPLETED' or jobflow.state == 'TERMINATED'
                or jobflow.state == 'FAILED'):
                break
            sleep(10)

    def download_output_files(self):
        """ Create local copy of EMR results"""
        bucket_list = self.bucket.list("output/part")
        for bucket_entry in bucket_list:
            key_string = str(bucket_entry.key)
            # check if file exists locally, if not: download it
            if not os.path.exists(key_string):
                bucket_entry.get_contents_to_filename("../" + key_string)
            else:
                print "output file already exists, please delete"

    @staticmethod
    def update_mapper_file(model_name):
        """ Alters one array in mapper file to the selected model"""
        # only update female array, males always die :(
        with open('../src/mapper/mapper.py', 'w') as output_file:
            with open('../src/mapper/mapper_template.py', 'r') as input_file:
                for line in input_file:
                    if "female_survival_table =" in line:
                        insert_string = "    female_survival_table = "
                        if model_name == "model2":
                            insert_string += ("[[0, 0, 1, 1]," +
                                " [0, 1, 1, 1], [1, 1, 0, 0]]\n")
                        else:
                            insert_string += ("[[1, 1, 1, 1]," +
                                " [1, 1, 1, 1], [1, 1, 1, 1]]\n")
                        output_file.write(insert_string)
                    else:
                        output_file.write(line)

    @staticmethod
    def post_process_output_file():
        """ Cleans up EMR output into Kaggle csv format"""
        parsed_data = []
        unparseable_data = []

        with open('../output/part-00000', 'r') as input_file:
            for line in input_file:
                line = line.strip()
                try:
                    csv_splits = line.split(',')
                    csv_splits[0] = int(csv_splits[0])
                    # parsed_data is a list of lists
                    parsed_data.append(csv_splits)
                except ValueError:
                    unparseable_data.append(line)
        parsed_data.sort()

        with open('../output/titanic_test_data.csv', 'w') as output_file:
            # start with lines that couldn't be parsed
            # hopefully this will only be the original header
            for line in unparseable_data:
                output_file.write("%s\n" % line)
            for line in parsed_data:
                output_file.write("%d,%s\n" % (line[0], line[1]))


def main():
    """ Program flow, runs with or without arguments"""
    my_emr = EmrProcessing()

    if "-s" in sys.argv:
        my_emr.verbose_mode = False
    else:
        my_emr.verbose_mode = True
        print "\nStarting Titanic Data Analysis"
    my_emr.parse_user_selections()

    # Setup
    my_emr.clear_local_output_directory()
    my_emr.update_mapper_file("model2")

    # S3 activities
    my_emr.empty_bucket()
    my_emr.create_and_fill_bucket()

    # EMR activities
    my_emr.setup_and_run_job()
    my_emr.wait_until_job_completes()

    # Cleanup
    my_emr.download_output_files()
    my_emr.post_process_output_file()
    if my_emr.verbose_mode:
        my_emr.print_local_output_files_stats()


class FlushFile(object): # pylint: disable=R0903
    """Write-only flushing wrapper for file-type objects."""
    def __init__(self, single_file):
        self.single_file = single_file

    def write(self, data_to_write):
        """Stdout write support code"""
        self.single_file.write(data_to_write)
        self.single_file.flush()


if __name__ == "__main__":
    # Replace stdout with an automatically flushing version
    sys.stdout = FlushFile(sys.__stdout__)
    sys.stderr = FlushFile(sys.__stderr__)

    main()

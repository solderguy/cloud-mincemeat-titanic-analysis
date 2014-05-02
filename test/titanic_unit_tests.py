#!/usr/bin/env python

"""
CREDENTIALS
  Module: titanic_unit_tests.py
  Author: John Soper
  Date: Apr 29, 2014
  Rev: 3

SUMMARY
    Unit tests for as many emr_titanic.py methods as possible
"""

import unittest
import os.path
import sys
import difflib
from cStringIO import StringIO

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_DIR)

#  Eclipse will show a FAILURE below, but that is just an artifact
#  Add a space, do Project>Clean, and save file to remove
import src.emr_titanic as emr_titanic

# pylint: disable=R0904
class TestSequenceFunctions(unittest.TestCase):
    """ Main test flow"""
    my_emr = None

    def setUp(self):
        self.seq = range(10)
        self.my_emr = emr_titanic.EmrProcessing()

    def test_clear_local_out_directory(self):
        """ test if local output directory deletes"""
        self.create_simple_file("../output/one.txt")
        self.create_simple_file("../output/two.txt")
        number_of_files = len(os.listdir('../output/'))
        self.assertNotEqual(number_of_files, 0, \
            "output dir should not be empty")

        self.my_emr.clear_local_output_directory()
        number_of_files = len(os.listdir('../output/'))
        self.assertEqual(number_of_files, 0, "output dir should be empty")

    def test_print_output_files_stats(self):
        """ tests printing of output files"""
        self.create_simple_file("../output/alpha.txt")
        self.create_simple_file("../output/beta.txt")
        try:                          # redirect stdout to string
            old_stdout = sys.stdout
            sys.stdout = my_stdout = StringIO()
            self.my_emr.print_local_output_files_stats()
        finally:                      # always restore
            sys.stdout = old_stdout
        captured_output = my_stdout.getvalue()

        valid_content = False
        if  (
            "FILES CREATED" in captured_output and
            "alpha.txt" in captured_output and
            "beta.txt" in captured_output
            ): valid_content = True
        self.assertTrue(valid_content, "should have two file listings")

    def test_generate_unique_name(self):
        """ Tests for consistent name from MAC address"""
        captured_name1 = self.my_emr.generate_unique_name()
        captured_prefix = captured_name1[:8]
        self.assertTrue(captured_prefix == 'titanic-',
            "prefix should be titanic-")

        captured_suffix = captured_name1[8:]
        mac_addr = int(captured_suffix, 16)
        length = 0
        while mac_addr:
            mac_addr >>= 1
            length += 1
        valid_length = False
        if length >= 44 and length < 53:
            valid_length = True
        self.assertTrue(valid_length, "does not match MAC address length")

        captured_name2 = self.my_emr.generate_unique_name()
        identical_macs = captured_name1 == captured_name2
        self.assertTrue(identical_macs, "mac address not consistent")

    def test_update_mapper_file(self):
        """ est that one line in mapper file can be correctly altered"""
        update_string = "[[1, 1, 1, 1], [1, 1, 1, 1], [1, 1, 1, 1]]"
        self.check_mapper_update_file(update_string, "model1")
        update_string = "[[0, 0, 1, 1], [0, 1, 1, 1], [1, 1, 0, 0]]"
        self.check_mapper_update_file(update_string, "model2")

    def check_mapper_update_file(self, update_string, model_name):
        """ Helper function for previous mapper file settings"""
        mapper_file = "../src/mapper/mapper.py"
        if os.path.exists(mapper_file):
            os.remove(mapper_file)
        file_still_exists = False
        if os.path.exists(mapper_file):
            file_still_exists = True
        self.assertFalse(file_still_exists, "mapper.py was not deleted")
        self.my_emr.update_mapper_file(model_name)
        new_file_exists = False
        if os.path.exists(mapper_file):
            new_file_exists = True
        self.assertTrue(new_file_exists, "mapper.py was not created")

        with open('../src/mapper/mapper_template.py', 'r') as file1:
            with open(mapper_file, 'r') as file2:
                diff = difflib.unified_diff(file1.readlines(),
                                            file2.readlines())
        delta = ''.join(x[2:] for x in diff if x.startswith('+ '))
        target_string = "female_survival_table = " + update_string
        strings_equal = delta.strip() == target_string
        msg_string = model_name + " mapper.py line change incorrect"
        self.assertTrue(strings_equal, msg_string)


    def test_post_process_output_file(self):
        """ Tests file refactoring into proper format"""
        self.my_emr.clear_local_output_directory() # already verified
        self.create_simple_file("../output/part-00000")
        self.my_emr.post_process_output_file()
        file_exists = False
        if os.path.exists("../output/titanic_test_data.csv"):
            file_exists = True
        self.assertTrue(file_exists, "titanic_test.data.csv not created")

        with open("../output/titanic_test_data.csv", 'r') as csv_file:
            lines = csv_file.readlines()
        correctly_processed = False
        if lines[0] == "created by automated software for testing\n" and\
            lines[1] == "945,1\n" and lines[2] == "1122,0\n":
            correctly_processed = True
        self.assertTrue(correctly_processed, "output file processed wrong")

    @staticmethod
    def create_simple_file(file_name):
        """ Utility class to write small file"""
        with open(file_name, 'w') as text_file:
            # Encode some output data so it can serve double duty
            text_file.write("1122,0\n")
            text_file.write("945,1\n")
            text_file.write("created by automated software for testing\n")

#if __name__ == '__main__':
#    unittest.main()

SUITE = unittest.TestLoader().loadTestsFromTestCase(TestSequenceFunctions)
unittest.TextTestRunner(verbosity=2).run(SUITE)

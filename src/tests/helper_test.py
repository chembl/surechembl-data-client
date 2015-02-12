#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import unittest
import shutil
import datetime
import ftplib

from mock import MagicMock, ANY
from mock import call

from src.scripts.helper_funcs import retry

class HelperTests(unittest.TestCase):

    def setUp(self):
    	self.f = MagicMock( return_value=None )
    	self.fparms = ['parm1', 2, True]

    def test_retry_success(self):
        retry( 5, self.f, self.fparms )
        self.f.assert_called_with( 'parm1',2,True )

    def test_retry_success_retval(self):
    	self.f.return_value = "Success"
        retval = retry( 5, self.f, self.fparms )
        self.f.assert_called_with( 'parm1',2,True )
        self.assertEqual("Success", retval)

    def test_retry_failure_basic(self):
    	self.verify_retry_failures(5,2,0)

    def test_retry_failure_lots(self):
    	self.verify_retry_failures(10,9,0)

    def test_retry_failure_sleep(self):
    	self.verify_retry_failures(5,3,1)

    def verify_retry_failures(self, retry_count, fail_count, sleep_time):
    	self.f.side_effect = ([ KeyError('somekey')]*fail_count) + [ None ]
        retry( retry_count, self.f, self.fparms, sleep_secs=sleep_time )
        
        self.f.assert_called_with( 'parm1',2,True )
        self.assertTrue(self.f.call_count == fail_count + 1)

    def test_total_failure_exception(self):
    	self.f.side_effect = Exception('some persistent error')

    	try:
        	retry( 5, self.f, self.fparms )
        	self.fail("Exception expected")
    	except Exception, e:
    		self.assertEqual( 'some persistent error', str(e) )
	        self.f.assert_called_with( 'parm1',2,True )
	        self.assertEqual(5, self.f.call_count)   
        

def main():
    unittest.main()

if __name__ == '__main__':
    main()

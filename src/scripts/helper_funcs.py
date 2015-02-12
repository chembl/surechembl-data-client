
import logging
from time import sleep 

logger = logging.getLogger(__name__)

def retry(max_attempts, func, parms, sleep_secs=0):
	"""
	Provides a general purpose retry mechanism for Exception throwing functions
	"""

	complete = False
	attempts = 0

	# Main retry loop
	while attempts < max_attempts and not complete:

		attempts += 1

		try:
			ret_val = func(*parms)			
			complete = True
		except Exception, e:
			last_exc = e
			logger.warn( "Exception caught in retry block: [{}]".format(e) )
			logger.warn( "Sleeping for {} seconds".format(sleep_secs) )
			sleep(sleep_secs)

	# Post processing - error logging, and rethrow exception if needed
	if complete:
		logger.debug( "Retry block succeeded after {} attempts".format(attempts) )
		return ret_val
	else:
		logger.error( "Retry block unable to complete after {} attempts".format(attempts) )
		raise last_exc
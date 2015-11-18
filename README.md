# msSQLwork
Just a little helper for automating some SQL commands.

Problem::
  One or more batches of records may have been flagged as invalid.
  Each invalid batch will contain one or more transactions that did not process correctly. This will be apparent by the ProviderID column being NULL. The MessagingEvent table will have to be searched to recover the token for that transaction.
  
Solution::
	Create a list of all invalid batches (which will each be a dictionary).
	Create a 'Transactions' key in each batch and assign a list to it containing all invalid transactions.
	Get the timestamp of transaction and use to search Message table for token-only-response.
	Update transaction record with correct token and ProviderID
	Reset batch values for automatic settlement.

Details::
  Further nesting was avoided to prevent overwriting data in pymssql objects.
  About token::
  -- Get token (eg. '1186601810474155') out of token response:
  -- Event : Token only payload: 0|1CAPPROVAL|20|20|20|20|20|20|20|20|1C
  -- 1107|1C|1C|1C|1C|1C|1C|1C|1C|1BA0SP021070161186601810474155|1B
  -- Where 4155 is the 'last four' of the card number
  -- and the '016' in '07016' refers to the length of the token
Future Improvements::
	Make program interactive at command line.
	Make tkinter window for more graphical step through of program.
	

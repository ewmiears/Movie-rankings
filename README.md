# Movie-rankings
Simple script to scrape movie websites using asyncio and aggregate them into a single dataframe for analysis

This script will fetch HTML from a list of movie ranking websites, then extract the titles and ranks from them.  It will load this information into a dataframe and calculate an overall list of top movies.

This was meant to be an example of how to use asynchio to reduce the run time of a script where lots of waiting is involved.  To illustrate this, I timed the async portions and printed that out along with the list.  Additionally, in the test folder, there is a serial version of the script that uses the requests library instead.  The times can be compared.

# Problems I ran into:
- Movie titles don't match between sites.  I anticipated using colons vs dashes or even capitalization issues, but using "Star Wars" creates problems, because the full name is something like "Star Wars Episode IV: A New Hope" and there are multiple Star Wars movies.  It didn't get too out of hand, but I had to put some manual fixes in there.  Might be a good idea to create a centrailized conversion list or looking at a better way of matching titles.
- People generally disagree on things like movies.  You end up having something in the top 10 on one list, but didn't even make the other lists.  For the purposes of this project, I removed any title that was not in all lists.  For example, "The Avengers: Endgame" was just released, and is considered a top movie in one list, but other lists may be a few months old and do not have it in the list yet.

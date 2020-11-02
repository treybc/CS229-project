# Assembling DIME data:

There are three components of DIME 2018 data: candidates (aka recipients),
contributions, and donors (aka contributors).

For 2016/2018, we have three separate data sources:
`dime_recipients_all_1979_2018.rdata`, `contribDB_2018.csv` (and `contribDB_2016.csv`), and
`dime_contributors_1979_2018.csv`.
Note that two contain all recipients/donors from 1979, and so need to be filtered.

These are large files, especially the ~27GB contributions file, so we want to
get it all into sqlite so that we can handle the data while it's on disk instead
of having to load it all into memory.

The `merge_primary_data.py` file will merge DIME with the FEC primary elections data.
This is also a little messy; there are <100 not-perfectly-matched things I just threw away.

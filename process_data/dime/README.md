# Assembling DIME data:

There are three components of DIME 2018 data: candidates (aka recipients),
contributions, and donors (aka contributors).

The original 2014 DIME data comes in a nice SQLite databse with three tables,
respectively `candDB`, `contribDB`, and `donorDB`.

For 2016/2018, we have three separate data sources:
`dime_recipients_all_1979_2018.rdata`, `contribDB_2018.csv` (and `contribDB_2016.csv`), and
`dime_contributors_1979_2018.csv`.
Note that two contain all recipients/donors from 1979, and so need to be filtered.

These are large files, especially the ~27GB contributions file, so we want to
get it all into sqlite so that we can handle the data while it's on disk instead
of having to load it all into memory.

First, I used a lightly modified `csv_to_sqlite.py` included here to migrate
the really big contributions file into a new SQLite database with the command

`python csv_to_sqlite.py contribDB_2018.csv dime.sqlite3 contribDB --types types_contributions.csv`

The `types.csv` file was manually constructed from examining the data types; it's
probably not necessary.

The donors file was also too big to load into memory, so I put it into its own sqlite
file using the same script.

`python csv_to_sqlite.py dime_contributors_1979_2018.csv donors.sqlite3 donorDB --types types_donors.csv`

Then I pulled out just the rows with donations in 2018 (~4 million) and added them
to the main sqlite file:

```
con <- dbConnect(RSQLite::SQLite(), paste0(path,"dime.sqlite3"))
con2 <- dbConnect(RSQLite::SQLite(), paste0(path,"donors.sqlite3"))
donors.2018 <- dbGetQuery(con2, "select * from donorDB where amount_2018 > 0")
dbWriteTable(con, 'donorDB', donors.2018)

```

The candidates files was small enough to load in manually and add to the sqlite file:

```
load(paste(path, 'dime_recipients_all_1979_2018.rdata',sep=''))
# # name of dataframe is cands.all
cands.2018 <- cands.all[cands.all$cycle==2018, ]
dbWriteTable(con, 'candDB', cands.2018)
```

And finally clean up the contributions database a bit so the overall size is more
manageable:

```
dbExecute(con, "DELETE FROM contribDB WHERE seat != 'federal:house'")
dbExecute(con, "DELETE FROM contribDB WHERE `recipient.type` != 'CAND'")
```

After all this, we finally have all the 2018 data into `dime.sqlite3`, and can run
`process_dime.R` to export the data we care about.

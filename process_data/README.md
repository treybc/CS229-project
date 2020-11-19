# Assembling the Data

This is broken down into three main parts:

1. Running `parse_pfds.py` will download and parse all of the 2014-2020 personal
   financial disclosure report data, run the parser on them (the parser itself
   is a subprocess written in TypeScript in `./house-pfd-parser`),
   and perform some post-processing to prepare it to be merged with DIME.
   This outputs to `../data/pfd/pfd_final.csv`.

2. Running `dime/process_dime.py` will proces the DIME 2018 dataset into a condensed
   version needed for this project, deduplicate and clean up some of the data,
   and download and merge in the FEC's primary elections data. This outputs to
   `../data/dime_with_primaries.csv`.

3. `crosswalk.py` performs the name-based crosswalk between the pfd data and the
   DIME data, attempting to match candidates in both datasets together. Upon
   completing the crosswalk, it merges the two outputs from 1. and 2. and returns
   a final output file in `../data/merged_data.csv`, which is then the input
   to the `../analyze_data/` segment of the project.

Note: all data outputs go into the top-level `../data` folder, which is gitignore'd.

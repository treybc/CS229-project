import sqlite3 as sql
import pandas as pd

# NB: this is just me putting the code I ran just in the terminal into these functions


def csv_to_sqlite():
    # prereq: run csv_to_sqlite.py in folder with databases
    # Should probably use subprocess or something, but whatever
    # `python csv_to_sqlite.py contribDB_2018.csv dime.sqlite3 contribDB --types types_contributions.csv`
    # `python csv_to_sqlite.py contribDB_2016.csv dime.sqlite3 contribDB --types types_contributions.csv`
    # `python csv_to_sqlite.py contribDB_2014.csv dime.sqlite3 contribDB --types types_contributions.csv`
    # `python csv_to_sqlite.py dime_contributors_1979_2018.csv donors.sqlite3 donorDB --types types_donors.csv`
    pass


def merge_and_subset():
    # Extract only the donors active in 2014-2018 and write to main db
    print("Extracting donor data into main dime file...", end="")
    conn_donors = sql.connect("donors.sqlite3")
    donors_df = pd.read_sql(
        "SELECT * FROM donorDB where (amount_2014 > 0) OR (amount_2016 > 0) OR (amount_2018 > 0)",
        conn_donors,
    )
    conn_donors.close()

    conn = sql.connect("dime.sqlite3")
    donors_df.to_sql(name="donorDB", con=conn)
    print("Done!")

    # Remove contributions that aren't for congressional candidates:
    print("Removing contributions that aren't for congen candidates...", end="")
    c = conn.cursor()
    c.execute("DELETE FROM contribDB WHERE seat != 'federal:house'")
    c.execute("DELETE FROM contribDB WHERE `recipient.type` != 'CAND'")
    conn.commit()
    print("Done!")

    # Add in the House candidates 2014-2020
    print("Adding 2014-2020 candidates...", end="")
    candidates_df = pd.read_csv("dime_recipients_all_1979_2018.csv")
    candidates_df = candidates_df[candidates_df["seat"] == "federal:house"]
    candidates_df = candidates_df[candidates_df["cycle"] >= 2014]  # 14866 rows
    candidates_df.to_sql(name="candDB", con=conn)
    conn.commit()
    print("Done!")

    print("Cleaning up unused file space...", end="")
    c.execute("VACUUM;")
    conn.commit()
    print("Done!")
    conn.close()


def get_first_ninety_days_fundraising():
    # Try to get first contribution date
    query = """
        CREATE TABLE IF NOT EXISTS campaign_dates AS
            SELECT
            rid,
            cycle,
            sum(amount) as total_primary,
            min(date) as campaign_start,
            date(min(date), '90 days') as campaign_ninety
            FROM
            (
                SELECT * FROM
                (SELECT `bonica.rid` as rid, cycle FROM candDB) as cands
                LEFT JOIN
                (
                select * from contribDB WHERE `election.type` = 'P'
                ) as contribdb
                ON rid == contribdb.`bonica.rid` 
                  AND cands.cycle == contribdb.cycle
            ) as contribs
            GROUP BY rid, cycle
    """
    conn = sql.connect("dime.sqlite3")
    c = conn.cursor()
    c.execute(query)
    conn.commit()

    # Now do it again and aggregate everything within the first ninety days.
    query = """
    SELECT * FROM 
    candDB
    LEFT JOIN
    (
        SELECT
        campaign_dates.*, sum(amount) as total_ninety
        FROM
        (
            campaign_dates
            LEFT JOIN
            (
            select * from contribDB WHERE `election.type` = 'P'
            ) as contribP
            ON campaign_dates.rid == contribP.`bonica.rid` 
              AND contribP.date <= campaign_dates.campaign_ninety 
              AND contribP.cycle == campaign_dates.cycle
        )
        GROUP BY rid, campaign_dates.cycle
    ) as contribs
    ON candDB.`bonica.rid` == contribs.rid 
      AND candDB.cycle == contribs.cycle
    """

    dime_df = pd.read_sql(query, conn)
    dime_df.to_csv("dime_final.csv")
    conn.close()


if __name__ == "__main__":
    # TODO: configure path-setting instead of copying this file into the right
    # directory
    csv_to_sqlite()
    merge_and_subset()
    get_first_ninety_days_fundraising()

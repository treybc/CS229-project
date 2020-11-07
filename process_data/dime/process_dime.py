import sqlite3 as sql
import pandas as pd
from merge_primary_data import merge_primary_data

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
    dime_df.to_csv("dime_uncleaned.csv")
    conn.close()


def clean_duplicates():
    dime_df = pd.read_csv("dime_uncleaned.csv")
    # Drop useless columns
    dime_df = dime_df.drop(columns=["index", "cycle.1", "ICPSR", "ICPSR2"])

    # Drop duplicated rows that were only differentiated by ICPSR:
    dime_df = dime_df.drop_duplicates(
        subset=[
            "rid",
            "cycle",
            "Cand.ID",
            "total_primary",
            "total_ninety",
            "total.receipts",
        ]
    )

    # Filter out third-party / independent candidates:
    dime_df = dime_df[dime_df.party <= 200]

    # Now we inspect and fix the ~58 remaining ones:

    # George "Mike" Kelly is listed as both names...
    dime_df = dime_df[dime_df["Cand.ID"] != "H4PA03117"]
    dime_df.loc[dime_df["Cand.ID"] == "H0PA03271", "Cand.ID"] = "H4PA03117"

    # And Corey McKinnon, and several others are given dupe IDs in 2016
    def remove_bad_ids(df, bad_ids, cycle):
        return df[~((df["Cand.ID"].isin(bad_ids)) & (df["cycle"] == cycle))]

    bad_ids_2018 = [
        "H6TX32043",
        "H8NV03275",
        "H6NV03097",
        "H4NJ10085",
        "H8IA03108",
        "H8NJ04162",
        "H1CA34014",
        "H7GA06015",
        "H7CA34011",
        "H7CA34029",
        "H6CA08207",
        "H2IL02156",
    ]
    # Some people have name changes, some people just don't actually match to anything
    # because they dropped out before the primary election happened.
    bad_ids_2016 = [
        "H6CT01156",
        "H6NC03146",
        "H4FL05073",
        "H6NY06133",
        "H4NV01153",
        "H6NJ10205",
        "H6FL18154",
        "H4CA48042",
        "H6TX32076",
        "H6NE01127",
        "H6PA02189",
        "H0CA27101",
        "H6MI09203",
        "H2MO05352",
        "H6CA25185",
        "H6CA08215",
        "H2FL08063",
        "H2TX01062",
        "H6FL11142",
        "H6IN06149",
        "H2TX25269",
        "H2AZ08078",
        "H2TX36134",
        "H6NY13147",
        "H6CA42099",
        "H6FL18063",
    ]
    bad_ids_2014 = [
        "H4CO04066",
        "H4OH03071",
        "nv_TEMP_e71636807a",
        "ky_TEMP_9c9d896076",
        "H4PA04107",
        "H4AZ06102",
        "H4OK02170",
        "H4WA07057",
        "H4WA09053",
        "H6CA08207",
        "H4CA29026",
        "H2TX25269",
        "H4NJ03114",
        "H2FL20076",
    ]

    dime_df = remove_bad_ids(dime_df, bad_ids_2018, 2018)
    dime_df = remove_bad_ids(dime_df, bad_ids_2016, 2016)
    dime_df = remove_bad_ids(dime_df, bad_ids_2014, 2014)

    # Swap some people around
    def replace_cand_id(df, old_cand_id, new_cand_id, cycle):
        df.loc[
            (df["Cand.ID"] == old_cand_id) & (df.cycle == cycle), "Cand.ID"
        ] = new_cand_id

    replace_cand_id(dime_df, "H2NV04045", "H8NV03275", 2018)
    replace_cand_id(dime_df, "H4NV01153", "H6NV03097", 2018)
    replace_cand_id(dime_df, "H6NV03097", "H4NV01153", 2016)
    replace_cand_id(dime_df, "H2CA00138", "H6CA08215", 2016)
    replace_cand_id(dime_df, "H4FL23076", "H2FL08063", 2016)
    replace_cand_id(dime_df, "H6IN06206", "H6IN06149", 2016)
    replace_cand_id(dime_df, "H6AZ02189", "H2AZ08078", 2016)
    replace_cand_id(dime_df, "H8CA08120", "H6CA08120", 2018)

    # 2 different NY Maloneys both have rid cand1318... we don't really care
    # that much, since they're both incumbents, so just drop one arbitrarily.
    dime_df = dime_df[dime_df["Cand.ID"] != "H2NY14037"]

    # Fix some challenger/open-seat/incumbent discrepancies from the dupes
    dime_df.loc[
        (dime_df.rid == "cand134815") & (dime_df.cycle == 2016), "Incum.Chall"
    ] = "O"
    dime_df.loc[
        (dime_df.rid == "cand41543") & (dime_df.cycle == 2016), "Incum.Chall"
    ] = "O"
    dime_df.loc[
        (dime_df.rid == "cand139792") & (dime_df.cycle == 2018), "Incum.Chall"
    ] = "I"

    # Other random dupes
    dime_df = dime_df[dime_df["Cand.ID"] != "H8NC08109"]
    dime_df = dime_df[dime_df.name != "ARATA, LAWRENCE V MR. III"]

    # In the other directions, a couple of distinct rids are actually the same person:
    dime_df = dime_df[dime_df.rid != "cand143177"]
    dime_df = dime_df[dime_df.rid != "cand144114"]
    dime_df = dime_df[dime_df.rid != "cand145023"]
    dime_df.loc[dime_df.rid == "cand145026", "Cand.ID"] = "H8CA53092"
    dime_df = dime_df[~((dime_df.rid == "cand144802") & (dime_df.cycle == 2018))]

    dime_df.to_csv("dime_final.csv")


if __name__ == "__main__":
    # TODO: configure path-setting instead of copying this file into the right
    # directory
    csv_to_sqlite()
    merge_and_subset()
    get_first_ninety_days_fundraising()
    clean_duplicates()
    merge_primary_data()

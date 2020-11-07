# File to download FEC primary election data and merge with DIME's dime_final.csv
import pandas as pd
import os


def download_primary_data():
    """
    Download the primary election data from the FEC website and put into dataframe.
    """
    election_files = [
        "https://www.fec.gov/documents/1700/federalelections2014.xls",
        "https://www.fec.gov/documents/1890/federalelections2016.xlsx",
        "https://www.fec.gov/documents/1897/federalelections2018.xlsx",
    ]

    df_primaries = None

    for election_file in election_files:
        year = election_file.split(".")[-2][-4:]
        df_year = pd.read_excel(
            election_file,
            sheet_name="{year} US House Results by State".format(year=year),
            index_col=0,
        )
        df_year.rename(
            columns={
                "FEC ID#": "Cand.ID",
                "PRIMARY VOTES": "votes_primary",
                "PRIMARY %": "pct_primary",
                "RUNOFF VOTES": "votes_runoff",
                "RUNOFF %": "pct_runoff",
                "GENERAL VOTES ": "votes_general",
                "GENERAL %": "pct_general",
                "GE WINNER INDICATOR": "won_general",
                "PARTY": "party",
                "DISTRICT": "district",
                "D": "district",
            },
            inplace=True,
            errors="ignore",
        )

        df_year = df_year.dropna(subset=["Cand.ID"])
        df_year["Cand.ID"] = df_year["Cand.ID"].str.strip()
        df_year = df_year[(df_year["party"] == "D") | (df_year["party"] == "R")]

        # A couple of manual fixes:
        if year == "2016":
            # Dave Koller has an extra random row
            df_year.drop(752, inplace=True)

        if year == "2018":
            # John Chrin is two rows for some reason
            df_year.loc[3480, "votes_primary"] = df_year.loc[3481]["votes_primary"]
            df_year.loc[3480, "pct_primary"] = df_year.loc[3481]["pct_primary"]
            df_year.drop(3481, inplace=True)

            # Jennifer Zordani has the wrong ID
            df_year.loc[1141, "Cand.ID"] = "H8IL06105"

            # Anya Tynia is listed multiple times for different parties, only gets >30 votes once
            df_year.drop([4293], inplace=True)

        df_year = df_year[
            [
                "Cand.ID",
                # "party",
                "votes_primary",
                "pct_primary",
                "votes_runoff",
                "pct_runoff",
                "votes_general",
                "pct_general",
                "won_general",
                # "district",
            ]
        ]
        df_year["cycle"] = int(year)

        # Remove special election if there's already a regular election
        df_year.drop_duplicates(subset=["Cand.ID", "cycle"], inplace=True)

        if df_primaries is None:
            df_primaries = df_year
        else:
            df_primaries = pd.concat([df_primaries, df_year])

    return df_primaries


def merge_primary_data():
    df_dime = pd.read_csv("../../data/dime_final.csv")
    df_primary = download_primary_data()
    df_merged = pd.merge(df_dime, df_primary, how="left", on=["Cand.ID", "cycle"])
    # 11/6/2020: 10510 pre-2020 DIME candidates, 4826 primary results of which 51 unmatched
    df_merged.drop(columns=["Unnamed: 0", "Unnamed: 0.1"], inplace=True)
    df_merged.to_csv("../../data/dime_with_primaries.csv", index=False)


if __name__ == "__main__":
    merge_primary_data()

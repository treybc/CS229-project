import pandas as pd
from pandas import DataFrame
import numpy as np
import requests
import os
import subprocess
from matplotlib import pyplot as plt
import sys
from typing import List


## Create map of possible dollar ranges in disclosure files to max/min values.
range_map = {}
range_map["Undetermined"] = (0, 0)
range_map["None"] = (0, 0)

# Asset range classifications
range_map["$1 - $1,000"] = (1, 1000)
range_map["$1,001 - $15,000"] = (1001, 15000)
range_map["$15,001 - $50,000"] = (15001, 50000)
range_map["$50,001 - $100,000"] = (50001, 100000)
range_map["$10,000 - $15,000"] = (10000, 15000)
range_map["$100,001 - $250,000"] = (100001, 250000)
range_map["$250,001 - $500,000"] = (250001, 500000)
range_map["Over $50,000,000"] = (50000000, 50000000)
range_map["Spouse/DC over $1,000,000"] = (1000000, 1000000)
# Because Raymond Pena reported exact numbers instead of ranges
range_map["$4,100.00"] = (4100, 4100)
range_map["$83,000.00"] = (83000, 83000)


# Liability-specific classifications
range_map["$500,001 - $1,000,000"] = (500001, 1000000)
range_map["$1,000,001 - $5,000,000"] = (1000001, 5000000)
range_map["$5,000,001 - $25,000,000"] = (5000001, 25000000)
range_map["$25,000,001 - $50,000,000"] = (25000001, 50000000)

# Unearned-income-specifc classifications
range_map["$1 - $200"] = (1, 200)
range_map["$201 - $1,000"] = (201, 1001)
range_map["$1,001 - $2,500"] = (1001, 2500)
range_map["$2,501 - $5,000"] = (2500, 5000)
range_map["$5,001 - $15,000"] = (5001, 15000)
range_map["$15,001 - $50,000"] = (15001, 50000)
range_map["$100,001 - $1,000,000"] = (100001, 1000000)
range_map["Over $5,000,000"] = (5000000, 5000000)


def parse_unearned_income(row: pd.Series) -> pd.Series:
    """
    Parse ranges for unearned income.
    """
    # First check previous year's income
    if row["income_prev_year"] in range_map:
        values = range_map[row["income_prev_year"]]
        row["min_unearned_income"] = values[0]
        row["max_unearned_income"] = values[1]
    # If that's not available, try the current year
    elif row["income"] in range_map:
        values = range_map[row["income"]]
        row["min_unearned_income"] = values[0]
        row["max_unearned_income"] = values[1]
    # No value in either; so it's just zero.
    elif pd.isna(row["income"]) and pd.isna(row["income_prev_year"]):
        row["min_unearned_income"] = 0
        row["max_unearned_income"] = 0
    # Sometimes the two rows will be combined and we need to split them.
    elif row["income"] == row["income_prev_year"]:
        tokens = row["income_prev_year"].split()
        if len(tokens) == 6 and tokens[1] == "-" and tokens[4] == "-":
            # (hopefully) looks something like '$2,501 - $5,000 $5,001 - $15,000'
            key = " ".join(row["income_prev_year"].split(" ")[3:])
            values = range_map[key]
            row["min_unearned_income"] = values[0]
            row["max_unearned_income"] = values[1]
        elif "None" in tokens:
            # (hopefully) looks something like '$2,501 - None $15,000' or 'None None'
            key = " ".join(list(filter(lambda x: x != "None", tokens)))
            if key in range_map:
                values = range_map[key]
            elif key == "":
                values = (0, 0)
            row["min_unearned_income"] = values[0]
            row["max_unearned_income"] = values[1]
        elif len(tokens) == 6 and tokens[1] == "-" and tokens[3] == "-":
            # If the row entries spill over onto two lines,
            # they'll still be read left-to-right, so
            # $50,001 -            $15,001 -
            # $100,000             $50,000
            # gets picked up as '$50,001 - $15,001 - $100,000 $50,000'.
            # We just take previous year, which is 2,5 (the right column)
            key = "{} - {}".format(tokens[2], tokens[5])
            values = range_map[key]
            row["min_unearned_income"] = values[0]
            row["max_unearned_income"] = values[1]
        else:
            print("Not processed!", row["income"], " ; ", row["income_prev_year"])
    # The previous check should've gotten them all
    else:
        print("Should not be!", row["income"], " ; ", row["income_prev_year"])
    return row


def parse_earned_income(row: pd.Series) -> pd.Series:
    """
    Parse the earned income table, choosing between previous and current year
    and fixing malformatted values.
    """
    # One or both dollar value might be empty:
    if row["income_prev_year"] in ["N/A", "N/a"] or row["income_prev_year"] is None:
        # Either prev year income is explicitly N/A, or one of the two fields is blank.
        # In either case, just use the other one.
        income = row["income_ytd"]
    else:
        income = row["income_prev_year"]
    try:
        if income in ["N/A", "N/a"]:
            row["income_earned"] = 0
        else:
            row["income_earned"] = float(income.replace("$", "").replace(",", ""))
    except (AttributeError, ValueError):
        row["income_earned"] = np.nan
        print(
            "failed to parse income:",
            row["file"],
            row["income_prev_year"],
            row["income_ytd"],
        )
    return row


def get_candidate_set() -> DataFrame:
    """
    Return a dataframe containing the candidates for which we will parse financial data.
    Must have Year and DocID fields.
    """
    # Read in Congressional Elections Dataset
    election_df = pd.read_csv("dime_cong_elections_current.csv")

    # Filter down candidates
    filtered_df = election_df[election_df.candidate_inactive == 0]
    print("Total active candidates: ", len(filtered_df))
    filtered_df = filtered_df[filtered_df.seat == "federal:house"]
    print("Total House candidates: ", len(filtered_df))
    filtered_df = filtered_df[filtered_df.cycle == 2018]
    print("2018 House candidates: ", len(filtered_df))

    incumbents = filtered_df[filtered_df.Incum_Chall == "I"]

    final_df = filtered_df[filtered_df.party == "D"]
    print("2018 Democratic House candidates:", len(final_df))
    final_df = final_df[final_df.num_prim_opps >= 6]
    print(
        "2018 Democratic House candidates in primaries with at least 6 candidates:",
        len(final_df),
    )
    final_df = final_df[~final_df.district.isin(incumbents.district)]
    print(
        "2018 Democratic House candidates in primaries with at least 6 candidates and \
            no incumbent:",
        len(final_df),
    )
    final_df = final_df[final_df.total_receipts >= 5000]
    print(
        "2018 Democratic House candidates in primaries with at least 6 candidates and \
            no incumbent who raised above reporting threshold:",
        len(final_df),
    )

    final_df.sort_values("dcp", inplace=True)

    # Read in financial disclosure form log
    df_disclosures_2018 = pd.read_csv("2018FD.txt", delimiter="\t")
    df_disclosures_2017 = pd.read_csv("2017FD.txt", delimiter="\t")
    df_disclosures = pd.concat([df_disclosures_2018, df_disclosures_2017])
    df_disclosures = df_disclosures.astype({"DocID": np.int32})
    # We only want initial filings (probably)
    df_disclosures = df_disclosures[df_disclosures["FilingType"] == "C"]

    # Get matching last name fields
    final_df["last"] = final_df.Name.str.split(",", 1).str[0]
    df_disclosures["last"] = df_disclosures.Last.str.upper()

    new_df = pd.merge(
        final_df,
        df_disclosures,
        how="left",
        left_on=["last", "district"],
        right_on=["last", "StateDst"],
    )

    # TODO: California primary elections have some filing in 2016??
    # In general, we want to be able to find everyone who raised over $5000.
    # But for now we'll just drop the bad ones.
    new_df = new_df[new_df.total_receipts >= 5000]
    new_df = new_df.dropna(subset=["DocID"])
    new_df = new_df.astype({"DocID": np.int32})

    # Some people have two filings, one in 2017 and one in 2018. Keep the 2017 one.
    # TODO: look at this more in detail
    new_df.sort_values(["dcp", "Year"], inplace=True)
    new_df.drop_duplicates(subset=["Name", "dcp"], inplace=True)

    # Texas's 29th distrinct has 2 Garcias, so their data gets mixed up.
    # Just drop it for now. TODO: don't do this; it's dumb.
    new_df = new_df[new_df.district != "TX29"]

    return new_df


def download_disclosure_files(df: DataFrame) -> None:
    """
    Download the disclosure files for the selected candidates from the House
    Clerk's website database, and store them in the raw_disclosures folder.
    """
    url_format = "http://clerk.house.gov/public_disc/financial-pdfs/{0}/{1}.pdf"
    if not os.path.exists("../data"):
        os.makedirs("../data")
    if not os.path.exists("../data/raw_disclosures"):
        os.makedirs("../data/raw_disclosures")

    for _, row in df.iterrows():
        url = url_format.format(row.Year, row.DocID)
        dst = "../data/raw_disclosures/{0}.pdf".format(row.DocID)
        if not os.path.exists(dst):
            r = requests.get(url, stream=True)
            with open(dst, "wb") as f:
                f.write(r.content)
            break


def parse_disclosure_files() -> DataFrame:
    """
    Converts the pdf disclosure files into a useable dataframe indexed by DocID.
    The house-pfd-parser gets most of the way there, then we do some additional
    manipulation to clean everything up.
    """
    # Run the subprocess.
    parse_disclosure_files_js()

    assets = pd.read_csv("../data/parsed_disclosures/assets-and-unearned-income.csv")
    assets.rename(columns={"income.1": "income_prev_year"}, inplace=True)
    liabilities = pd.read_csv("../data/parsed_disclosures/liabilities.csv")
    earned_income = pd.read_csv("../data/parsed_disclosures/earned-income.csv")

    # Clean up non-disclosed/empty data
    assets.drop(assets[assets.file.str.contains("None disclosed")].index, inplace=True)
    liabilities.drop(
        liabilities[liabilities.file.str.contains("None disclosed")].index, inplace=True
    )
    earned_income.drop(
        earned_income[earned_income.file.str.contains("None disclosed")].index,
        inplace=True,
    )

    # There's one row on 10018705 and one on 10019048 that cross over pages and
    # aren't merged properly because the asset name spans both lines.
    # Since we don't care about the full asset name, and the correct value is
    # in another row, we simply drop the rows we've verified.
    assets = assets[
        ~(
            assets.file.isin(["10018705", "10019048"])
            & (assets["value-of-asset"].isna())
        )
    ]

    # Parse ranges for assets and liabilities
    for (string_range, values) in range_map.items():
        liabilities.loc[
            liabilities["amount-of-liability"] == string_range, "min_liability"
        ] = values[0]
        liabilities.loc[
            liabilities["amount-of-liability"] == string_range, "max_liability"
        ] = values[1]
        assets.loc[assets["value-of-asset"] == string_range, "min_asset"] = values[0]
        assets.loc[assets["value-of-asset"] == string_range, "max_asset"] = values[1]

    # Parse ranges for unearned income
    assets = assets.apply(parse_unearned_income, axis=1)

    # Parse earned income data
    earned_income[["income_ytd", "income_prev_year"]] = earned_income.amount.str.split(
        " ", n=1, expand=True
    )
    earned_income = earned_income.apply(parse_earned_income, axis=1)

    # test for correctly mapping everything
    assert liabilities.min_liability.isna().sum() == 0
    assert liabilities.max_liability.isna().sum() == 0
    assert assets.min_asset.isna().sum() == 0
    assert assets.max_asset.isna().sum() == 0
    assert assets.min_unearned_income.isna().sum() == 0
    assert assets.max_unearned_income.isna().sum() == 0
    assert earned_income.income_earned.isna().sum() == 0

    return merge_disclosure_files(assets, liabilities, earned_income)


## Parse disclosure files:
def parse_disclosure_files_js():
    """
    Run the house-pfd-parse as a subprocess, which parses the pdfs in the 
    raw_disclosures folder into an almost-useable state.
    """
    subprocess.run(
        "cd ../house-pfd-parser && npm start",
        shell=True,
        stdout=sys.stdout,
        stderr=subprocess.STDOUT,
    )


def merge_disclosure_files(
    assets: DataFrame, liabilities: DataFrame, earned_income: DataFrame
) -> DataFrame:
    """
    Merge all disclosure categies together according to their file code.
    """
    assets_by_file = (
        assets[
            [
                "file",
                "min_asset",
                "max_asset",
                "min_unearned_income",
                "max_unearned_income",
            ]
        ]
        .groupby("file")
        .sum()
    )
    liabilities_by_file = (
        liabilities[["file", "min_liability", "max_liability"]].groupby("file").sum()
    )
    earned_income_by_file = (
        earned_income[["file", "income_earned"]].groupby("file").sum()
    )
    df_pfd = liabilities_by_file.join(assets_by_file, how="outer").join(
        earned_income_by_file, how="outer"
    )
    # Fill in na's generated by outer join
    df_pfd.fillna(0, inplace=True)

    df_pfd["min_wealth"] = df_pfd["min_asset"] - df_pfd["max_liability"]
    df_pfd["max_wealth"] = df_pfd["max_asset"] - df_pfd["min_liability"]
    df_pfd["wealth"] = (df_pfd["min_wealth"] + df_pfd["max_wealth"]) / 2
    df_pfd["income"] = (
        df_pfd["income_earned"]
        + (df_pfd["min_unearned_income"] + df_pfd["max_unearned_income"]) / 2
    )
    return df_pfd


def save_disclosure_data(df: DataFrame):
    df.to_csv("../data/pfd_final.csv")


def main():
    candidate_df = get_candidate_set()
    download_disclosure_files(candidate_df)
    df_pfd = parse_disclosure_files()
    save_disclosure_data(df_pfd)


if __name__ == "__main__":
    main()

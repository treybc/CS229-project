import pandas as pd
from pandas import DataFrame
import numpy as np
import requests
import os
import subprocess
from matplotlib import pyplot as plt
import sys
from typing import List
import zipfile

DEFAULT_YEARS = [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]


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
range_map["Over"] = (50000000, 50000000)
range_map["over $50,000,000"] = (50000000, 50000000)
range_map["Spouse/DC over $1,000,000"] = (1000000, 1000000)
range_map["Spouse/DC Over $1,000,000"] = (1000000, 1000000)
range_map["spouse/DC Over $1,000,000"] = (1000000, 1000000)
range_map["SP"] = (1000000, 1000000)
range_map["Spouse/DC Over"] = (1000000, 1000000)


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
range_map["$1 - $15,000"] = (1, 15000)
range_map["$5,001 - $15,000"] = (5001, 15000)
range_map["$15,001 - $50,000"] = (15001, 50000)
range_map["$100,001 - $1,000,000"] = (100001, 1000000)
range_map["Over $5,000,000"] = (5000000, 5000000)


def is_exact_number(amount: str) -> bool:
    """
    Returns whether a string is a valid dollar-value string that can be parsed
    into a single number.
    """
    return not np.isnan(parse_exact_number(amount))


def parse_exact_number(amount: str) -> int:
    """
    Parses a valid dollar-value string into a single number, or returns nan.
    """
    # possible valid cases:
    # $.12
    # $12,400.23
    # $1.00
    # $3,000
    if type(amount) != str or amount[0] != "$" or " " in amount:
        return np.nan
    if len(amount) == 4 and amount[:2] == "$.":
        return 0
    processed_amount = amount[1:].replace(",", "")
    if len(processed_amount) >= 2 and processed_amount[-2] == ".":
        # If there's a dot, there should only be one, and should be followed by 1 digits.
        dot = processed_amount.find(".")
        if dot != len(processed_amount) - 2:
            return np.nan
        processed_amount = processed_amount[:dot]
    elif len(processed_amount) >= 3 and processed_amount[-3] == ".":
        # If there's a dot, there should only be one, and should be followed by 2 digits.
        dot = processed_amount.find(".")
        if dot != len(processed_amount) - 3:
            return np.nan
        processed_amount = processed_amount[:dot]
    try:
        return int(processed_amount)
    except:
        return np.nan


def parse_asset(row: pd.Series) -> pd.Series:
    val = row["value-of-asset"]
    if val in range_map:
        return pd.Series(range_map[val])
    elif is_exact_number(val):
        num = parse_exact_number(val)
        return pd.Series((num, num))
    else:
        print("error parse asset:", row["file"], val)
        return pd.Series((np.nan, np.nan))


def parse_liability(row: pd.Series) -> pd.Series:
    val = row["amount-of-liability"]
    if val in range_map:
        return pd.Series(range_map[val])
    elif is_exact_number(val):
        num = parse_exact_number(val)
        return pd.Series((num, num))
    else:
        print("error parse liability:", val)
        return pd.Series((np.nan, np.nan))


def parse_unearned_income(row: pd.Series) -> pd.Series:
    """
    Parse ranges for unearned income.
    """
    min_unearned_income = None
    max_unearned_income = None
    if row["income"] == "over $5,000,000":
        return pd.Series((5000000, 5000000))
    # First check previous year's income
    if row["income_prev_year"] in range_map:
        values = range_map[row["income_prev_year"]]
        min_unearned_income = values[0]
        max_unearned_income = values[1]
    elif is_exact_number(row["income_prev_year"]):
        num = parse_exact_number(row["income_prev_year"])
        min_unearned_income = num
        max_unearned_income = num
    # If that's not available, try the current year
    elif row["income"] in range_map:
        values = range_map[row["income"]]
        min_unearned_income = values[0]
        max_unearned_income = values[1]
    elif is_exact_number(row["income"]):
        num = parse_exact_number(row["income"])
        min_unearned_income = num
        max_unearned_income = num
    # No value in either; so it's just zero.
    elif (
        (type(row["income"]) == str and "Not" in row["income"])
        or pd.isna(row["income"])
    ) and (
        (type(row["income_prev_year"]) == str and "Not" in row["income_prev_year"])
        or pd.isna(row["income_prev_year"])
    ):
        min_unearned_income = 0
        max_unearned_income = 0
    # Sometimes the two rows will be combined and we need to split them.
    elif row["income"] == row["income_prev_year"]:
        tokens = row["income_prev_year"].split()
        if len(tokens) == 6 and tokens[1] == "-" and tokens[4] == "-":
            # (hopefully) looks something like '$2,501 - $5,000 $5,001 - $15,000'
            key = " ".join(row["income_prev_year"].split(" ")[3:])
            values = range_map[key]
            min_unearned_income = values[0]
            max_unearned_income = values[1]
        elif "None" in tokens:
            # (hopefully) looks something like '$2,501 - None $15,000' or 'None None'
            key = " ".join(list(filter(lambda x: x != "None", tokens)))
            if key in range_map:
                values = range_map[key]
            elif key == "":
                values = (0, 0)
            min_unearned_income = values[0]
            max_unearned_income = values[1]
        elif len(tokens) == 6 and tokens[1] == "-" and tokens[3] == "-":
            # If the row entries spill over onto two lines,
            # they'll still be read left-to-right, so
            # $50,001 -            $15,001 -
            # $100,000             $50,000
            # gets picked up as '$50,001 - $15,001 - $100,000 $50,000'.
            # We just take previous year, which is 2,5 (the right column)
            key = "{} - {}".format(tokens[2], tokens[5])
            values = range_map[key]
            min_unearned_income = values[0]
            max_unearned_income = values[1]
        else:
            print(
                "Not processed!",
                row["file"],
                row["income"],
                ";",
                row["income_prev_year"],
            )
    # The previous check should've gotten them all
    else:
        print(
            "Should not be!", row["file"], row["income"], " ; ", row["income_prev_year"]
        )
    return pd.Series([min_unearned_income, max_unearned_income])


def parse_earned_income(row: pd.Series) -> pd.Series:
    """
    Parse the earned income table, choosing between previous and current year
    and fixing malformatted values.
    """
    income_earned = None
    # One or both dollar value might be empty:
    if (
        row["income_prev_year"] in ["N/A", "N/a", "$"]
        or row["income_prev_year"] is None
    ):
        # Either prev year income is explicitly N/A, or one of the two fields is blank.
        # In either case, just use the other one.
        income = row["income_ytd"]
    else:
        income = row["income_prev_year"]
    try:
        if income in ["N/A", "N/a", "$"] or pd.isna(income):
            income_earned = 0
        else:
            income_earned = float(income.replace("$", "").replace(",", ""))
    except (AttributeError, ValueError):
        income_earned = np.nan
        print(
            "failed to parse income:",
            row["file"],
            row["income_prev_year"],
            row["income_ytd"],
        )
    return income_earned


def get_candidate_set_old() -> DataFrame:
    """
    NB: this is the old get_candidate_set function from my summer work (now unused)
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


def get_candidate_set(years: List[int]) -> DataFrame:
    """
    Download the manifest files for the selected years from the House
    Clerk's website database, and combine them into a dataframe of candidates
    to parse. They come in zip files, so there're a few extra files around at the end.

    Returns a dataframe containing the candidates for which we will parse financial data.
    We only need the Year and DocID fields for this part.
    """
    url_format = (
        "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.ZIP"
    )
    if not os.path.exists("../data"):
        os.makedirs("../data")
    if not os.path.exists("../data/pfd"):
        os.makedirs("../data/pfd")
    if not os.path.exists("../data/pfd/manifests"):
        os.makedirs("../data/pfd/manifests")

    print("Downloading the manifest files for these years:", years)
    downloaded_years = []

    df_manifest = None
    for year in years:
        url = url_format.format(year=year)
        dst = "../data/pfd/manifests/{year}FD.zip".format(year=year)
        if not os.path.exists(dst):
            downloaded_years.append(year)
            r = requests.get(url, stream=True)
            with open(dst, "wb") as f:
                f.write(r.content)
            with zipfile.ZipFile(dst, "r") as zipped:
                zipped.extractall("../data/pfd/manifests")

        df_year = pd.read_csv(
            "../data/pfd/manifests/{year}FD.txt".format(year=year), delimiter="\t"
        )
        if df_manifest is None:
            df_manifest = df_year
        else:
            df_manifest = pd.concat([df_manifest, df_year])

    print(
        "Downloaded {0} manifests; the remainder already exist.".format(
            len(downloaded_years)
        )
    )

    # A little bit of cleanup
    df_manifest = df_manifest.astype({"DocID": np.int32})
    df_manifest.drop_duplicates(inplace=True)
    # We only want initial filings and incumbent annual filings (probably)
    df_manifest = df_manifest[df_manifest["FilingType"].isin(["C", "O"])]

    return df_manifest


def download_disclosure_files(df: DataFrame) -> None:
    """
    Download the disclosure files for the selected candidates from the House
    Clerk's website database, and store them in the raw_disclosures folder.
    """
    url_format = "http://clerk.house.gov/public_disc/financial-pdfs/{year}/{doc}.pdf"
    if not os.path.exists("../data"):
        os.makedirs("../data")
    if not os.path.exists("../data/pfd"):
        os.makedirs("../data/pfd")
    if not os.path.exists("../data/pfd/raw_disclosures"):
        os.makedirs("../data/pfd/raw_disclosures")

    print("Downloading {0} disclosure files. This will take a while...".format(len(df)))

    downloaded = 0
    skipped = 0
    for _, row in df.iterrows():
        if str(row.DocID)[0] in ["8", "9"]:
            # We can't parse the hand-written ones, so skip download to save space/time.
            skipped += 1
            continue
        url = url_format.format(year=row.Year, doc=row.DocID)
        # NB: it's okay to mash up all the filings together into one folder
        # because DocID is unique across all years 2012-2020.
        dst = "../data/pfd/raw_disclosures/{doc}.pdf".format(doc=row.DocID)
        if not os.path.exists(dst):
            downloaded += 1
            r = requests.get(url, stream=True)
            with open(dst, "wb") as f:
                f.write(r.content)
    print(
        "Download complete. Downloaded {0} files; skipped {1}; {2} were already downloaded".format(
            downloaded, skipped, len(df) - downloaded - skipped
        )
    )


def parse_disclosure_files() -> DataFrame:
    """
    Converts the pdf disclosure files into a useable dataframe indexed by DocID.
    The house-pfd-parser gets most of the way there, then we do some additional
    manipulation to clean everything up.
    """
    # Run the house-pfd-parser subprocess.
    parse_disclosure_files_js()

    assets = open_disclosure_file(
        "../data/pfd/parsed_disclosures/assets-and-unearned-income.csv"
    )
    assets.rename(columns={"income.1": "income_prev_year"}, inplace=True)
    liabilities = open_disclosure_file("../data/pfd/parsed_disclosures/liabilities.csv")
    earned_income = open_disclosure_file(
        "../data/pfd/parsed_disclosures/earned-income.csv"
    )

    # Clean up non-disclosed/empty data
    assets.drop(assets[assets.file.str.contains("None disclosed")].index, inplace=True)
    liabilities.drop(
        liabilities[liabilities.file.str.contains("None disclosed")].index, inplace=True
    )
    earned_income.drop(
        earned_income[earned_income.file.str.contains("None disclosed")].index,
        inplace=True,
    )

    # We should really manually verify the 120 or so NaNs, but whatever
    assets = assets[~(assets["value-of-asset"].isna())]
    # 10000624 literally has NaN-valued liability entries...
    liabilities = liabilities[
        ~(
            liabilities.file.isin(["10000624"])
            & (liabilities["amount-of-liability"].isna())
        )
    ]

    # Parse ranges for assets and liabilities
    assets[["min_asset", "max_asset"]] = assets.apply(parse_asset, axis=1)
    liabilities[["min_liability", "max_liability"]] = liabilities.apply(
        parse_liability, axis=1
    )
    # for (string_range, values) in range_map.items():
    #     liabilities.loc[
    #         liabilities["amount-of-liability"] == string_range, "min_liability"
    #     ] = values[0]
    #     liabilities.loc[
    #         liabilities["amount-of-liability"] == string_range, "max_liability"
    #     ] = values[1]
    #     assets.loc[assets["value-of-asset"] == string_range, "min_asset"] = values[0]
    #     assets.loc[assets["value-of-asset"] == string_range, "max_asset"] = values[1]

    # Parse ranges for unearned income
    assets[["min_unearned_income", "max_unearned_income"]] = assets.apply(
        parse_unearned_income, axis=1
    )

    # Parse earned income data
    earned_income[["income_ytd", "income_prev_year"]] = earned_income.amount.str.split(
        " ", n=1, expand=True
    )
    earned_income["income_earned"] = earned_income.apply(parse_earned_income, axis=1)

    # test for correctly mapping everything
    print(assets[assets.min_asset.isna()].loc[:, assets.columns != "asset"])
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
        "cd house-pfd-parser && npm start",
        shell=True,
        stdout=sys.stdout,
        stderr=subprocess.STDOUT,
    )


def open_disclosure_file(filename) -> DataFrame:
    start = 0
    with open(filename) as f:
        for line in f:
            if "file,page" in line:
                break
            start += 1
    return pd.read_csv(filename, skiprows=start)


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
    df.to_csv("../data/pfd/pfd_final.csv")


def main(years: List[int]):
    candidate_df = get_candidate_set(years)
    download_disclosure_files(candidate_df)
    df_pfd = parse_disclosure_files()
    save_disclosure_data(df_pfd)


if __name__ == "__main__":
    years = DEFAULT_YEARS
    if len(sys.argv) > 1:
        years = sys.argv[1:]
    main(years)

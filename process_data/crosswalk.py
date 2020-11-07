# Script to create a crosswalk file matching candidate-year entries to PFD documents.
import pandas as pd
import numpy as np
from unidecode import unidecode

YEARS = [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]


def get_pfd_manifest() -> pd.DataFrame:
    year_dfs = []
    for year in YEARS:
        df_year = pd.read_csv(
            "../data/pfd/manifests/{year}FD.txt".format(year=year), delimiter="\t"
        )
        # Rename fields to be nicer
        df_year.rename(
            columns={
                "Prefix": "prefix",
                "Last": "last",
                "First": "first",
                "Suffix": "suffix",
                "FilingType": "filing_type",
                "StateDst": "district",
                "Year": "year",
                "FilingDate": "filing_date",
                "DocID": "pfd_id",
            },
            inplace=True,
        )
        # Add cycle year field
        df_year["cycle"] = year + (year % 2)
        year_dfs.append(df_year)

    df_manifest = pd.concat(year_dfs)
    # Some cleanup and filtering:
    df_manifest = df_manifest.astype(
        {"pfd_id": np.int32, "last": str, "first": str, "prefix": str, "suffix": str}
    )
    df_manifest.drop_duplicates(inplace=True)
    df_manifest = df_manifest[df_manifest["filing_type"].isin(["C", "O"])]
    # At-large districts are 00 in PFD and 01 in DIME:
    def fix_district(row):
        if row["district"][-2:] == "00":
            return row["district"][:-2] + "01"
        return row["district"]

    df_manifest["district"] = df_manifest.apply(fix_district, axis=1)

    # Normalize data
    for name_field in ["prefix", "last", "first", "suffix"]:
        df_manifest[name_field] = (
            df_manifest[name_field]
            .str.upper()
            .apply(unidecode)
            .str.replace(r"[- ]", "")
        )

    # We might want a more comprehensive crosswalk at some point,
    # but for now I'm subsetting to just the ones we have digital PFDs for
    # and also 2018 to see what we're missing
    # df_manifest = df_manifest[df_manifest["pfd_id"] >= 10000000]
    # df_manifest = df_manifest[df_manifest["cycle"] <= 2018]

    return df_manifest


def get_dime_manifest():
    df_dime = pd.read_csv("../data/dime_with_primaries.csv")

    name_fields = [
        "name",
        "lname",
        "ffname",
        "fname",
        "mname",
        "nname",
        "title",
        "suffix",
    ]
    for name_field in name_fields:
        df_dime[name_field] = df_dime[name_field].str.upper().str.replace(r"[- ]", "")
    df_dime["llname"] = df_dime["name"].apply(lambda x: x.split(",")[0])

    df_dime = df_dime[["rid", "cycle", "district", "llname"] + name_fields]
    return df_dime


no_match = 0
too_many_match = 0
missing_districts = set()


def create_crosswalk(df_manifest, df_dime):
    def find_matching_rid(row):
        global too_many_match
        result = {"pfd_id": row["pfd_id"], "cycle": row["cycle"], "rid": None}

        df_candidate_set = df_dime[
            (df_dime["district"] == row["district"])
            & (df_dime["cycle"] == row["cycle"])
            & df_dime["lname"].str.contains(row["last"], regex=False)
        ]
        # df_candidate_set = df_dime[
        #     (df_dime["district"] == row["district"])
        #     & (df_dime["cycle"] == row["cycle"])
        # ]
        # reverse_match = df_candidate_set.apply(
        #     lambda r: r["lname"] in row["last"], axis=1
        # )
        # df_candidate_set = df_candidate_set[
        #     df_candidate_set["lname"].str.contains(row["last"], regex=False)
        #     | df_candidate_set["llname"].str.contains(row["last"], regex=False)
        #     | reverse_match
        # ]

        if len(df_candidate_set) == 1:
            result["rid"] = df_candidate_set.iloc[0]["rid"]
        elif len(df_candidate_set) == 0:
            matched = False
            # if row["district"][:2] == "PA":
            # Try matching over entire state (PA, TX observed)
            df_candidate_set = df_dime[
                (df_dime.district.str[:2] == row["district"][:2])
                & (df_dime["cycle"] == row["cycle"])
                & df_dime["lname"].str.contains(row["last"], regex=False)
                & df_dime["ffname"].str.contains(row["first"], regex=False)
            ]
            if len(df_candidate_set) == 1:
                matched = True
                result["rid"] = df_candidate_set.iloc[0]["rid"]

            if not matched:
                # Try matching on first name instead
                df_candidate_set = df_dime[
                    (df_dime["district"] == row["district"])
                    & (df_dime["cycle"] == row["cycle"])
                    & df_dime["ffname"].str.contains(row["first"], regex=False)
                ]
                if len(df_candidate_set) == 1:
                    matched = True
                    result["rid"] = df_candidate_set.iloc[0]["rid"]

            if not matched and row["cycle"] <= 2018:
                global no_match
                no_match += 1
                missing_districts.add(row["district"])
                result["rid"] = "missing"
        else:
            # print("Too many matches!", row)
            # print(df_candidate_set)
            df_candidate_set = df_dime[
                (df_dime["district"] == row["district"])
                & (df_dime["cycle"] == row["cycle"])
                & df_dime["lname"].str.contains(row["last"], regex=False)
                & df_dime["ffname"].str.contains(row["first"], regex=False)
            ]
            if len(df_candidate_set) > 1 and row["cycle"] <= 2018:
                if row.cucle != 2020:
                    too_many_match += 1
                # TODO: matching for now so we can just look at missing; disambiguate later
                result["rid"] = "dupe"  # df_candidate_set.iloc[0]["rid"]
            elif len(df_candidate_set) == 0:
                # This means we matched on too many by last name, but nothing by first...
                pass
                if row.cycle != 2020:
                    too_many_match += 1
                # TODO: matching for now so we can just look at missing; disambiguate later
                result["rid"] = "dupe"  # df_candidate_set.iloc[0]["rid"]
            else:
                result["rid"] = df_candidate_set.iloc[0]["rid"]
        if result["rid"] is None and row.cycle != 2020:
            print(row)
            print(df_candidate_set)
            raise ValueError
        return pd.Series(result)

    # One candidate-cycle may have multiple filings, so we should have a m:1 mapping
    # So we need to match from df_manifest
    df_crosswalk = df_manifest.apply(find_matching_rid, axis=1)
    df_crosswalk.to_csv("../data/pfd/crosswalk.csv")
    print(
        len(df_manifest) - no_match - too_many_match,
        "matched",
        no_match,
        "no matches,",
        too_many_match,
        "excess matches",
    )
    print(missing_districts)
    return df_crosswalk


def apply_crosswalk(df_crosswalk):
    pfd_final = pd.read_csv("../data/pfd/pfd_final.csv")
    df_dime = pd.read_csv("../data/dime_with_primaries.csv")
    df_final = pd.merge(
        df_crosswalk, pfd_final, how="right", left_on="pfd_id", right_on="file"
    )
    # merge somehow the duplicate disclosures.
    # TODO: currently keeping most recent filing; maybe do average or highest-information?
    df_final = df_final.sort_values(["pfd_id"]).drop_duplicates(
        subset=["rid", "cycle"], keep="last"
    )

    df_final = pd.merge(
        df_final,
        df_dime,
        how="left",
        left_on=["rid", "cycle"],
        right_on=["rid", "cycle"],
    )
    df_final.dropna(subset=["rid"], inplace=True)

    df_final.to_csv("../data/merged_data.csv")


def main():
    df_manifest = get_pfd_manifest()
    df_dime = get_dime_manifest()
    df_crosswalk = create_crosswalk(df_manifest, df_dime)
    apply_crosswalk(df_crosswalk)


if __name__ == "__main__":
    main()

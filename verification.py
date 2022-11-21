import glob
import json
import pandas as pd


def get_daily_reports():
    """Get all daily reports"""

    daily_reports = []
    for file_path in glob.glob("data/daily_reports/2*.json"):  # Should have us covered for a millennium
        with open(file_path, "r") as fp:
            daily_reports.append(json.load(fp))

    return pd.DataFrame(daily_reports).sort_values(by="date", ascending=False)


def check_document_for_outliers(df: pd.DataFrame, document: pd.DataFrame) -> list:
    """Outputs dict outliers keys based on dataframe data"""

    outlier = list()
    for column in df.columns:
        try:
            standard_deviation = df[column].std()
            mean = df[column].mean()
            latest_value = document[column].values[0]

            if mean - standard_deviation*3 > latest_value or latest_value > mean + standard_deviation*3:
                print(f"{column}: {mean - standard_deviation*3} < {latest_value} > {mean + standard_deviation*3}")

                outlier.append(column)

        except (KeyError, TypeError) as error:
            pass

        except Exception as error:
            print(error)
            outlier.append(column)

    return outlier


def verify_latest_report(invalid_outliers: set):
    """True if the latest report does not contain outliers we care about"""

    daily_report_df = get_daily_reports()

    past_reports_df = daily_report_df.iloc[1:, :]
    latest_report_df = daily_report_df.iloc[0:1, :]

    outliers = set(check_document_for_outliers(past_reports_df, latest_report_df))

    return not bool(outliers.intersection(invalid_outliers))


if __name__ == "__main__":

    from create_daily_ospool_report_json import OUTLIERS_WE_CARE_ABOUT, write_document_to_file

    if verify_latest_report(OUTLIERS_WE_CARE_ABOUT):

        with open(sorted(glob.glob("data/daily_reports/2*.json"))[-1], "r") as fp:

            latest_document = json.load(fp)
            write_document_to_file(latest_document, True, True)

import glob
import json
import pandas as pd
from datetime import datetime, timedelta


def get_daily_reports():
    """Get all daily reports"""

    daily_reports = []
    file_names = reversed(sorted(glob.glob("data/daily_reports/2*.json")))
    for file_path in file_names:  # Should have us covered for a millennium
        with open(file_path, "r") as fp:

            json_data = json.load(fp)

            # Convert YYYY-MM-DD to datetime object
            json_data['date'] = datetime.strptime(json_data['date'], "%Y-%m-%d")

            daily_reports.append(json_data)

    return pd.DataFrame(daily_reports).sort_values(by="date", ascending=False)


def check_document_for_outliers(df: pd.DataFrame, document: pd.DataFrame) -> list:
    """Outputs dict outliers keys based on dataframe data"""

    outlier = list()
    for column in df.columns:
        try:
            standard_deviation = df[column].std()

            # Calculate the mean using ewm
            mean = df[column].ewm(times=df['date'], halflife=timedelta(days=30)).mean().iloc[-1]

            latest_value = document[column].values[0]

            if mean - standard_deviation*3 > latest_value or latest_value > mean + standard_deviation*3:
                print(f"{column}: {mean - standard_deviation*3} < {latest_value} > {mean + standard_deviation*3}")

                # Graph out the outlier as a barchart
                # df[column].plot(kind='bar', title=f"{column} Outlier Check")
                # plt.show()

                outlier.append(column)

        except (KeyError, TypeError) as error:
            pass

        except Exception as error:
            print(error)
            outlier.append(column)

    return outlier


def verify_latest_report(invalid_outliers: set, expected_keys: list):
    """True if the latest report does not contain outliers we care about"""

    daily_report_df = get_daily_reports()

    past_reports_df = daily_report_df.iloc[1:365, :]
    latest_report_df = daily_report_df.iloc[0:1, :]

    if not all(key in latest_report_df.columns for key in expected_keys):
        return False

    outliers = set(check_document_for_outliers(past_reports_df, latest_report_df))

    return not bool(outliers.intersection(invalid_outliers))


if __name__ == "__main__":

    from create_daily_ospool_report_json import OUTLIERS_WE_CARE_ABOUT, EXPECTED_KEYS, write_document_to_file

    if verify_latest_report(OUTLIERS_WE_CARE_ABOUT, EXPECTED_KEYS):

        with open(sorted(glob.glob("data/daily_reports/2*.json"))[-1], "r") as fp:

            latest_document = json.load(fp)
            write_document_to_file(latest_document, True, True)

import os
import json


def write_document_to_file(
        document,
        directory: str,
        path: str,
        overwrite: bool = False
):
    """Write a daily file into a data directory"""

    output_path = f"{directory}/{path}"

    if overwrite or not os.path.isfile(output_path):
        with open(output_path, "w") as fp:
            json.dump(document, fp)


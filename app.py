import pandas as pd
import os, zipfile
import requests


extracted_files_path = "storage/extracted_files"
modified_files_path = "storage/modified_files"
downloaded_zip_path = "storage/downloaded_files.zip"


def generate_mod_file(filename):
    FILENAME = f"{extracted_files_path}/{filename}"
    MOD_FILENAME = f"{modified_files_path}/{filename}"

    df = pd.read_csv(FILENAME)
    df = df[
        [
            "SYMBOL",
            "TIMESTAMP",
            "OPEN",
            "HIGH",
            "LOW",
            "CLOSE",
            "TOTTRDQTY",
        ]
    ]

    df.to_csv(MOD_FILENAME, index=False)


def get_data_and_unzip(start_date: str, end_date: str, source: str = "NSE"):
    url = "https://www.samco.in/bse_nse_mcx/getBhavcopy"

    payload = f"start_date={start_date}&end_date={end_date}&show_or_down=2&bhavcopy_data%5B%5D={source}"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    if response.status_code == 200:
        with open(downloaded_zip_path, "wb") as f:
            f.write(response.content)
    else:
        return

    with zipfile.ZipFile(downloaded_zip_path, "r") as zip_ref:
        zip_ref.extractall(extracted_files_path)


def process_all_files():

    for filename in os.listdir(extracted_files_path):
        generate_mod_file(filename)


def clean_up():
    for filename in os.listdir(extracted_files_path):
        os.remove(f"{extracted_files_path}/{filename}")
        os.remove(f"{modified_files_path}/{filename}")


def zip_modified_files(start_date: str, end_date: str, source: str) -> str:
    modified_zip_files_path = (
        f"storage/trading_data_{start_date}_{end_date}_{source}.zip"
    )
    zipObj = zipfile.ZipFile(modified_zip_files_path, "w")
    for filename in os.listdir(modified_files_path):
        zipObj.write(f"{modified_files_path}/{filename}")
    zipObj.close()
    return modified_zip_files_path


def run_retrival(start_date: str, end_date: str, source: str = "NSE"):
    get_data_and_unzip(start_date, end_date, source)
    process_all_files()
    return zip_modified_files(start_date, end_date, source)


def reset():

    for filename in os.listdir(extracted_files_path):
        if os.path.isdir(f"storage/{filename}"):
            continue
        os.remove(f"{extracted_files_path}/{filename}")

    for filename in os.listdir(modified_files_path):
        if os.path.isdir(f"storage/{filename}"):
            continue
        os.remove(f"{modified_files_path}/{filename}")

    for filename in os.listdir("storage"):
        if os.path.isdir(f"storage/{filename}"):
            continue
        os.remove(f"storage/{filename}")


from flask import Flask, request, send_file, render_template, redirect, url_for

app = Flask(__name__, template_folder="ui")


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/download", methods=["POST"])
def get_files():
    try:
        start_date = request.form.get("start-date")
        end_date = request.form.get("end-date")
        source = request.form.get("platform")

        zip_file_path = run_retrival(start_date, end_date, source)
        res_to_return = send_file(
            zip_file_path, mimetype="application/zip", as_attachment=True
        )
        clean_up()
        os.remove(downloaded_zip_path)
        os.remove(zip_file_path)

        return res_to_return
    except Exception as e:
        reset()
        return redirect(url_for("home"))


@app.route("/reset", methods=["GET"])
def reset_server():
    reset()
    return redirect(url_for("home"))


if __name__ == "__main__":
    app.run(debug=True)
"""
CDC WONDER Data Downloader - Web UI

Run with: python app.py
Then open: http://localhost:5000
"""

from flask import Flask, render_template, request, jsonify, send_file
from cdc_wonder import CDCWonderClient
import os
import threading
import time
import uuid

app = Flask(__name__)

# Store running jobs
jobs = {}

# Presets configuration
PRESETS = {
    "all_causes_by_year": {
        "name": "All Causes by Year",
        "description": "Total deaths grouped by year",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["year"],
        "cause": None,
    },
    "all_causes_by_age_year": {
        "name": "All Causes by Age & Year",
        "description": "Deaths by age group and year",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["age", "year"],
        "cause": None,
    },
    "covid_by_year": {
        "name": "COVID-19 by Year",
        "description": "COVID-19 deaths by year",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["year"],
        "cause": "covid19",
    },
    "covid_by_age": {
        "name": "COVID-19 by Age & Year",
        "description": "COVID-19 deaths by age and year",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["age", "year"],
        "cause": "covid19",
    },
    "heart_disease_by_year": {
        "name": "Heart Disease by Year",
        "description": "Heart disease deaths by year",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["year"],
        "cause": "heart_disease",
    },
    "cancer_by_year": {
        "name": "Cancer by Year",
        "description": "Cancer deaths by year",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["year"],
        "cause": "cancer",
    },
    "drug_overdose_by_year": {
        "name": "Drug Overdose by Year",
        "description": "Drug overdose deaths by year",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["year"],
        "cause": "drug_overdose",
    },
    "all_by_age_weekly": {
        "name": "All Causes Weekly by Age",
        "description": "Weekly deaths by age group (like sample XLS)",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["age", "year", "week"],
        "cause": None,
    },
}

# Available options
YEARS = list(range(2018, 2026))
GROUP_BY_OPTIONS = [
    ("year", "Year"),
    ("month", "Month"),
    ("week", "Week"),
    ("age", "Age Group (10-year)"),
    ("gender", "Gender"),
    ("race", "Race"),
    ("state", "State"),
]
CAUSE_OPTIONS = [
    ("", "All Causes"),
    ("covid19", "COVID-19"),
    ("heart_disease", "Heart Disease"),
    ("cancer", "Cancer"),
    ("accidents", "Accidents"),
    ("stroke", "Stroke"),
    ("alzheimers", "Alzheimer's"),
    ("diabetes", "Diabetes"),
    ("influenza_pneumonia", "Influenza & Pneumonia"),
    ("kidney_disease", "Kidney Disease"),
    ("suicide", "Suicide"),
    ("drug_overdose", "Drug Overdose"),
]
AGE_GROUPS = [
    ("*All*", "All Ages"),
    ("1", "< 1 year"),
    ("1-4", "1-4 years"),
    ("5-14", "5-14 years"),
    ("15-24", "15-24 years"),
    ("25-34", "25-34 years"),
    ("35-44", "35-44 years"),
    ("45-54", "45-54 years"),
    ("55-64", "55-64 years"),
    ("65-74", "65-74 years"),
    ("75-84", "75-84 years"),
    ("85+", "85+ years"),
]


@app.route("/")
def index():
    return render_template(
        "index.html",
        presets=PRESETS,
        years=YEARS,
        group_by_options=GROUP_BY_OPTIONS,
        cause_options=CAUSE_OPTIONS,
        age_groups=AGE_GROUPS,
    )


@app.route("/api/query", methods=["POST"])
def run_query():
    """Run a CDC WONDER query."""
    data = request.json
    job_id = str(uuid.uuid4())[:8]

    # Start job in background
    jobs[job_id] = {"status": "running", "progress": 0, "result": None, "error": None}
    thread = threading.Thread(target=execute_query, args=(job_id, data))
    thread.start()

    return jsonify({"job_id": job_id})


def execute_query(job_id, data):
    """Execute query in background thread."""
    import xml.etree.ElementTree as ET

    try:
        client = CDCWonderClient()
        query_type = data.get("query_type", "preset")
        time_granularity = data.get("time_granularity", "year")

        # For weekly queries, use template-based approach for reliability
        template_file = "Provisional Mortality Statistics, 2018 through Last Week_1768692313298-req ) Master.xml"
        use_template = time_granularity == "week" and os.path.exists(template_file)

        if use_template:
            # Use template XML and modify parameters
            tree = ET.parse(template_file)
            root = tree.getroot()

            # Get cause filter if specified
            cause = None
            if query_type == "preset":
                preset_key = data.get("preset")
                if preset_key in PRESETS:
                    cause = PRESETS[preset_key].get("cause")
            else:
                cause = data.get("cause") or None

            # Apply cause of death filter
            if cause:
                cause_code = CDCWonderClient.CAUSES_OF_DEATH.get(cause, cause)
                for param in root.findall("parameter"):
                    name = param.find("name")
                    if name is not None and name.text == "F_D176.V2":
                        value = param.find("value")
                        if value is not None:
                            value.text = cause_code
                        break

            # Add accept_datause_restrictions
            found = False
            for param in root.findall("parameter"):
                name = param.find("name")
                if name is not None and name.text == "accept_datause_restrictions":
                    found = True
                    value = param.find("value")
                    if value is not None:
                        value.text = "true"
                    break
            if not found:
                param = ET.SubElement(root, "parameter")
                name = ET.SubElement(param, "name")
                name.text = "accept_datause_restrictions"
                value = ET.SubElement(param, "value")
                value.text = "true"

            xml_request = ET.tostring(root, encoding="unicode")
            result = client._send_request(xml_request)

        elif query_type == "preset":
            preset_key = data.get("preset")
            if preset_key not in PRESETS:
                jobs[job_id]["error"] = f"Unknown preset: {preset_key}"
                jobs[job_id]["status"] = "error"
                return

            preset = PRESETS[preset_key]

            # Apply time granularity override
            group_by = list(preset["group_by"])  # Copy original

            # Remove existing time groupings and add new ones based on granularity
            group_by = [g for g in group_by if g not in ["year", "month", "week"]]
            if time_granularity == "month":
                group_by = ["year", "month"] + group_by
            else:
                group_by = ["year"] + group_by

            result = client.query(
                years=preset["years"],
                group_by=group_by,
                cause_of_death=preset["cause"],
            )
        else:
            # Custom query
            years = [int(y) for y in data.get("years", [2024])]
            group_by = data.get("group_by", ["year"])
            cause = data.get("cause") or None
            age_group = data.get("age_group")

            age_groups = None
            if age_group and age_group != "*All*":
                age_groups = [age_group]

            result = client.query(
                years=years,
                group_by=group_by,
                cause_of_death=cause,
                age_groups=age_groups,
            )

        if result.get("error"):
            jobs[job_id]["error"] = result["error"]
            jobs[job_id]["status"] = "error"
            return

        # Save to CSV
        filename = f"cdc_wonder_{job_id}.csv"
        filepath = os.path.join(os.path.dirname(__file__), filename)
        client.save_to_csv(result, filepath)

        jobs[job_id]["result"] = {
            "rows": len(result["data"]),
            "headers": result["headers"],
            "preview": result["data"][:10],
            "filename": filename,
            "caveats": result.get("caveats", [])[:3],
        }
        jobs[job_id]["status"] = "complete"

    except Exception as e:
        jobs[job_id]["error"] = str(e)
        jobs[job_id]["status"] = "error"


@app.route("/api/job/<job_id>")
def get_job_status(job_id):
    """Get status of a running job."""
    if job_id not in jobs:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(jobs[job_id])


@app.route("/api/download/<filename>")
def download_file(filename):
    """Download a result file."""
    filepath = os.path.join(os.path.dirname(__file__), filename)
    if not os.path.exists(filepath):
        return jsonify({"error": "File not found"}), 404
    return send_file(filepath, as_attachment=True)


@app.route("/api/batch", methods=["POST"])
def run_batch():
    """Run batch queries cycling through selected dimensions."""
    data = request.json
    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {"status": "running", "progress": 0, "result": None, "error": None, "files": [], "current": ""}

    thread = threading.Thread(target=execute_batch_queries, args=(job_id, data))
    thread.start()

    return jsonify({"job_id": job_id})


# Label mappings for filenames
GENDER_LABELS = {"M": "male", "F": "female", "*All*": "all"}
RACE_LABELS = {
    "2106-3": "white",
    "2054-5": "black",
    "1002-5": "aian",
    "A": "asian",
    "NHOPI": "nhopi",
    "*All*": "all"
}
CAUSE_LABELS = {
    "": "all_causes",
    "covid19": "covid",
    "heart_disease": "heart",
    "cancer": "cancer",
    "accidents": "accidents",
    "stroke": "stroke",
    "alzheimers": "alzheimers",
    "diabetes": "diabetes",
    "influenza_pneumonia": "flu_pneumonia",
    "drug_overdose": "overdose"
}
# 10-year (decennial) age groups - V_D176.V5
AGE_DECENNIAL = {
    "1": "under1",
    "1-4": "1_4",
    "5-14": "5_14",
    "15-24": "15_24",
    "25-34": "25_34",
    "35-44": "35_44",
    "45-54": "45_54",
    "55-64": "55_64",
    "65-74": "65_74",
    "75-84": "75_84",
    "85+": "85plus"
}

# 5-year (quinquennial) age groups - V_D176.V51
AGE_QUINQUENNIAL = {
    "1": "under1",
    "1-4": "1_4",
    "5-9": "5_9",
    "10-14": "10_14",
    "15-19": "15_19",
    "20-24": "20_24",
    "25-29": "25_29",
    "30-34": "30_34",
    "35-39": "35_39",
    "40-44": "40_44",
    "45-49": "45_49",
    "50-54": "50_54",
    "55-59": "55_59",
    "60-64": "60_64",
    "65-69": "65_69",
    "70-74": "70_74",
    "75-79": "75_79",
    "80-84": "80_84",
    "85-89": "85_89",
    "90-94": "90_94",
    "95-99": "95_99",
    "100+": "100plus",
    "NS": "not_stated"
}

# Legacy alias
AGE_LABELS = AGE_DECENNIAL


def execute_batch_queries(job_id, data):
    """Execute batch queries for all combinations."""
    import xml.etree.ElementTree as ET
    import itertools
    import csv

    template_file = "Provisional Mortality Statistics, 2018 through Last Week_1768692313298-req ) Master.xml"

    if not os.path.exists(template_file):
        jobs[job_id]["error"] = "Template XML file not found"
        jobs[job_id]["status"] = "error"
        return

    genders = data.get("genders", ["*All*"])
    races = data.get("races", ["*All*"])
    causes = data.get("causes", [""])
    age_granularity = data.get("age_granularity", "decennial")
    age_cycle = data.get("age_cycle", False)
    combine_files = data.get("combine_files", False)

    # Determine age parameter based on granularity (this affects the group-by in the query)
    if age_granularity == "quinquennial":
        age_groups_dict = AGE_QUINQUENNIAL
        age_param = "V_D176.V51"
    else:
        age_groups_dict = AGE_DECENNIAL
        age_param = "V_D176.V5"

    # If cycling through ages, iterate through each age group; otherwise use None (all ages)
    age_groups = list(age_groups_dict.keys()) if age_cycle else [None]

    # Generate all combinations
    combinations = list(itertools.product(genders, races, causes, age_groups))
    total = len(combinations)
    files = []

    # For combined output
    combined_data = []
    combined_headers = None

    # Human-readable labels for combined file
    GENDER_READABLE = {"M": "Male", "F": "Female", "*All*": "All"}
    RACE_READABLE = {
        "2106-3": "White",
        "2054-5": "Black",
        "1002-5": "American Indian/Alaska Native",
        "A": "Asian",
        "NHOPI": "Native Hawaiian/Pacific Islander",
        "*All*": "All"
    }
    CAUSE_READABLE = {
        "": "All Causes",
        "covid19": "COVID-19",
        "heart_disease": "Heart Disease",
        "cancer": "Cancer",
        "accidents": "Accidents",
        "stroke": "Stroke",
        "alzheimers": "Alzheimer's",
        "diabetes": "Diabetes",
        "influenza_pneumonia": "Influenza/Pneumonia",
        "drug_overdose": "Drug Overdose"
    }

    client = CDCWonderClient()

    try:
        for i, (gender, race, cause, age) in enumerate(combinations):
            # Build filename
            parts = []
            if gender != "*All*":
                parts.append(GENDER_LABELS.get(gender, gender))
            if race != "*All*":
                parts.append(RACE_LABELS.get(race, race))
            if cause:
                parts.append(CAUSE_LABELS.get(cause, cause))
            if age:
                parts.append(age_groups_dict.get(age, age))

            filename = "deaths_" + ("_".join(parts) if parts else "all") + ".csv"
            jobs[job_id]["progress"] = int((i / total) * 100)
            jobs[job_id]["current"] = f"Query {i+1}/{total}: {filename}"

            # Load and modify template
            tree = ET.parse(template_file)
            root = tree.getroot()

            # Swap age group-by parameter based on granularity (V_D176.V5 <-> V_D176.V51)
            old_age_param = "V_D176.V51" if age_granularity == "decennial" else "V_D176.V5"
            for param in root.findall("parameter"):
                name = param.find("name")
                value = param.find("value")
                # Check group-by parameters (B_1, B_2, etc.) and the age value parameter
                if name is not None and value is not None:
                    if value.text == old_age_param:
                        value.text = age_param

            # Set gender filter
            if gender != "*All*":
                for param in root.findall("parameter"):
                    name = param.find("name")
                    if name is not None and name.text == "V_D176.V7":
                        value = param.find("value")
                        if value is not None:
                            value.text = gender
                        break

            # Set race filter
            if race != "*All*":
                for param in root.findall("parameter"):
                    name = param.find("name")
                    if name is not None and name.text == "V_D176.V42":
                        value = param.find("value")
                        if value is not None:
                            value.text = race
                        break

            # Set cause filter
            if cause:
                cause_code = CDCWonderClient.CAUSES_OF_DEATH.get(cause, cause)
                for param in root.findall("parameter"):
                    name = param.find("name")
                    if name is not None and name.text == "F_D176.V2":
                        value = param.find("value")
                        if value is not None:
                            value.text = cause_code
                        break

            # Set age filter
            if age and age_param:
                for param in root.findall("parameter"):
                    name = param.find("name")
                    if name is not None and name.text == age_param:
                        value = param.find("value")
                        if value is not None:
                            value.text = age
                        break

            # Add accept_datause_restrictions
            found = False
            for param in root.findall("parameter"):
                name = param.find("name")
                if name is not None and name.text == "accept_datause_restrictions":
                    found = True
                    value = param.find("value")
                    if value is not None:
                        value.text = "true"
                    break
            if not found:
                param = ET.SubElement(root, "parameter")
                name = ET.SubElement(param, "name")
                name.text = "accept_datause_restrictions"
                value = ET.SubElement(param, "value")
                value.text = "true"

            xml_request = ET.tostring(root, encoding="unicode")

            try:
                result = client._send_request(xml_request)
                if not result.get("error"):
                    if combine_files:
                        # Store headers from first result
                        if combined_headers is None:
                            combined_headers = ["Gender", "Race", "Cause_of_Death"] + result["headers"]
                            if age_cycle:
                                combined_headers.insert(3, "Age_Filter")

                        # Add rows with identifying columns
                        gender_label = GENDER_READABLE.get(gender, gender)
                        race_label = RACE_READABLE.get(race, race)
                        cause_label = CAUSE_READABLE.get(cause, cause)
                        age_label = age_groups_dict.get(age, "All") if age else "All"

                        for row in result["data"]:
                            if age_cycle:
                                combined_data.append([gender_label, race_label, cause_label, age_label] + list(row))
                            else:
                                combined_data.append([gender_label, race_label, cause_label] + list(row))
                    else:
                        client.save_to_csv(result, filename)
                        files.append({"name": filename.replace(".csv", "").replace("deaths_", ""), "file": filename, "rows": len(result["data"])})
                else:
                    if not combine_files:
                        files.append({"name": filename, "file": None, "error": result["error"]})
            except Exception as e:
                if not combine_files:
                    files.append({"name": filename, "file": None, "error": str(e)})

            time.sleep(2)  # Rate limiting

        # Save combined file if requested
        if combine_files and combined_data:
            combined_filename = "deaths_combined.csv"
            with open(combined_filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(combined_headers)
                writer.writerows(combined_data)

            jobs[job_id]["result"] = {
                "rows": len(combined_data),
                "headers": combined_headers,
                "preview": combined_data[:10],
                "filename": combined_filename
            }
        else:
            jobs[job_id]["files"] = files

        jobs[job_id]["status"] = "complete"
        jobs[job_id]["progress"] = 100

    except Exception as e:
        jobs[job_id]["error"] = str(e)
        jobs[job_id]["status"] = "error"


@app.route("/api/run_by_age", methods=["POST"])
def run_by_age():
    """Run queries for each age group separately (legacy endpoint)."""
    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {"status": "running", "progress": 0, "result": None, "error": None, "files": []}

    thread = threading.Thread(target=execute_age_queries, args=(job_id,))
    thread.start()

    return jsonify({"job_id": job_id})


def execute_age_queries(job_id):
    """Execute queries for each age group."""
    import xml.etree.ElementTree as ET

    template_file = "Provisional Mortality Statistics, 2018 through Last Week_1768692313298-req ) Master.xml"

    if not os.path.exists(template_file):
        jobs[job_id]["error"] = "Template XML file not found"
        jobs[job_id]["status"] = "error"
        return

    age_groups = [
        ("1", "under_1_year"),
        ("1-4", "1_4_years"),
        ("5-14", "5_14_years"),
        ("15-24", "15_24_years"),
        ("25-34", "25_34_years"),
        ("35-44", "35_44_years"),
        ("45-54", "45_54_years"),
        ("55-64", "55_64_years"),
        ("65-74", "65_74_years"),
        ("75-84", "75_84_years"),
        ("85+", "85_plus_years"),
    ]

    client = CDCWonderClient()
    files = []
    total = len(age_groups)

    try:
        for i, (age_code, age_label) in enumerate(age_groups):
            jobs[job_id]["progress"] = int((i / total) * 100)
            jobs[job_id]["current"] = age_label

            # Load and modify template
            tree = ET.parse(template_file)
            root = tree.getroot()

            for param in root.findall("parameter"):
                name = param.find("name")
                if name is not None and name.text == "V_D176.V5":
                    value = param.find("value")
                    if value is not None:
                        value.text = age_code
                    break

            # Add accept_datause_restrictions
            found = False
            for param in root.findall("parameter"):
                name = param.find("name")
                if name is not None and name.text == "accept_datause_restrictions":
                    found = True
                    value = param.find("value")
                    if value is not None:
                        value.text = "true"
                    break
            if not found:
                param = ET.SubElement(root, "parameter")
                name = ET.SubElement(param, "name")
                name.text = "accept_datause_restrictions"
                value = ET.SubElement(param, "value")
                value.text = "true"

            xml_request = ET.tostring(root, encoding="unicode")
            result = client._send_request(xml_request)

            if not result.get("error"):
                filename = f"deaths_{age_label}.csv"
                client.save_to_csv(result, filename)
                files.append({"name": age_label, "file": filename, "rows": len(result["data"])})

            time.sleep(2)  # Rate limiting

        jobs[job_id]["files"] = files
        jobs[job_id]["status"] = "complete"
        jobs[job_id]["progress"] = 100

    except Exception as e:
        jobs[job_id]["error"] = str(e)
        jobs[job_id]["status"] = "error"


if __name__ == "__main__":
    # Create templates folder if needed
    os.makedirs("templates", exist_ok=True)
    print("CDC WONDER Data Downloader")
    print("=" * 40)
    print("Open http://localhost:5000 in your browser")
    print("Press Ctrl+C to stop")
    print("=" * 40)
    app.run(debug=True, port=5000)
else:
    # Production mode (gunicorn)
    os.makedirs("templates", exist_ok=True)

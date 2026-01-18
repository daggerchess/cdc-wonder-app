"""Run queries for each age group separately using the sample XML as template."""

from cdc_wonder import CDCWonderClient
import xml.etree.ElementTree as ET
import time
import os

# Age group codes and labels
AGE_GROUPS = [
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

# Template XML file
TEMPLATE_FILE = "Provisional Mortality Statistics, 2018 through Last Week_1768692313298-req ) Master.xml"


def modify_age_filter(xml_path: str, age_code: str) -> str:
    """Load XML template and modify the age filter."""
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Find and modify V_D176.V5 (Ten-Year Age Groups filter)
    for param in root.findall("parameter"):
        name = param.find("name")
        if name is not None and name.text == "V_D176.V5":
            value = param.find("value")
            if value is not None:
                value.text = age_code
            break

    # Ensure accept_datause_restrictions is set
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

    return ET.tostring(root, encoding="unicode")


def main():
    if not os.path.exists(TEMPLATE_FILE):
        print(f"Error: Template file not found: {TEMPLATE_FILE}")
        return

    client = CDCWonderClient()

    print("Running queries for each age group using template XML...")
    print("=" * 60)

    for age_code, age_label in AGE_GROUPS:
        print(f"\nQuerying: {age_label} (code: {age_code})")

        try:
            # Modify template XML with this age group
            xml_request = modify_age_filter(TEMPLATE_FILE, age_code)

            # Send request
            result = client._send_request(xml_request)

            if result.get("error"):
                print(f"  Error: {result['error']}")
                continue

            output_file = f"deaths_{age_label}.csv"
            client.save_to_csv(result, output_file)

            # Show summary
            print(f"  Rows: {len(result['data'])}")
            if result["data"] and len(result["data"]) > 0:
                print(f"  Sample: {result['data'][0][:4]}...")

        except Exception as e:
            print(f"  Exception: {e}")

        # Rate limit: wait 2 seconds between queries per CDC guidelines
        time.sleep(2)

    print("\n" + "=" * 60)
    print("Done! CSV files saved for each age group.")


if __name__ == "__main__":
    main()

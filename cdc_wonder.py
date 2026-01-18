"""
CDC WONDER API Client

A Python module for programmatically downloading data from CDC WONDER.
Supports the Provisional Mortality Statistics database (D176) and other datasets.

Usage:
    from cdc_wonder import CDCWonderClient

    client = CDCWonderClient()
    data = client.query(years=[2023, 2024], group_by=['year', 'age'])
"""

import requests
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional
import time
import csv
import io


class CDCWonderClient:
    """Client for interacting with the CDC WONDER API."""

    BASE_URL = "https://wonder.cdc.gov/controller/datarequest"

    # Database codes
    DATABASES = {
        "provisional_mortality": "D176",  # Provisional Multiple Cause of Death
        "detailed_mortality": "D76",      # Detailed Mortality 1999-2020
        "natality": "D66",                # Natality 2016-2022
    }

    # Group by field mappings for D176
    GROUP_BY_FIELDS = {
        "age": "D176.V5",
        "age_single_year": "D176.V51",
        "age_infant": "D176.V52",
        "gender": "D176.V7",
        "race": "D176.V42",
        "ethnicity": "D176.V43",
        "year": "D176.V100-level1",
        "month": "D176.V100-level2",
        "week": "D176.V1",
        "weekday": "D176.V4",
        "state": "D176.V9",
        "region": "D176.V10",
        "ucd": "D176.V2",              # Underlying cause of death
        "ucd_chapter": "D176.V25",     # UCD ICD chapter
        "mcd": "D176.V13",             # Multiple cause of death
        "autopsy": "D176.V20",
        "place_of_death": "D176.V21",
    }

    # Common cause of death codes (ICD-10)
    CAUSES_OF_DEATH = {
        "all": "*All*",
        "covid19": "U07.1",
        "heart_disease": "I00-I09,I11,I13,I20-I51",
        "cancer": "C00-C97",
        "accidents": "V01-X59,Y85-Y86",
        "stroke": "I60-I69",
        "alzheimers": "G30",
        "diabetes": "E10-E14",
        "influenza_pneumonia": "J09-J18",
        "kidney_disease": "N00-N07,N17-N19,N25-N27",
        "suicide": "X60-X84,Y87.0",
        "drug_overdose": "X40-X44,X60-X64,X85,Y10-Y14",
    }

    # Year codes for F_D176.V100
    YEARS = {
        2018: "2018",
        2019: "2019",
        2020: "2020",
        2021: "2021",
        2022: "2022",
        2023: "2023",
        2024: "2024",
        2025: "2025",
    }

    def __init__(self, database: str = "provisional_mortality"):
        """
        Initialize the CDC WONDER client.

        Args:
            database: Database identifier (default: provisional_mortality)
        """
        self.database = self.DATABASES.get(database, database)
        self.endpoint = f"{self.BASE_URL}/{self.database}"

    def load_xml_template(self, xml_path: str) -> ET.Element:
        """Load an XML query template from file."""
        tree = ET.parse(xml_path)
        return tree.getroot()

    def _build_base_parameters(self) -> dict:
        """Build the base parameters common to most queries."""
        return {
            # Accept data use restrictions
            "accept_datause_restrictions": "true",

            # Dataset info
            "dataset_code": self.database,
            "stage": "request",
            "action-Send": "Send",

            # Output options
            "O_precision": "1",
            "O_timeout": "600",
            "O_show_totals": "true",
            "O_show_zeros": "true",
            "O_show_suppressed": "true",
            "O_rate_per": "100000",
            "O_javascript": "on",
            "O_dates": "MMWR",

            # Measures to return
            "M_1": "D176.M1",  # Deaths
            "M_2": "D176.M2",  # Population
            "M_3": "D176.M3",  # Crude Rate

            # Age adjustment
            "O_aar": "aar_none",
            "O_aar_pop": "0000",
            "O_age": "D176.V5",

            # Location settings
            "O_location": "D176.V9",
            "O_death_location": "D176.V79",
            "O_death_urban": "D176.V89",
            "O_urban": "D176.V19",
            "O_race": "D176.V42",
            "O_ucd": "D176.V2",
            "O_mcd": "D176.V15",

            # Filter modes
            "O_V1_fmode": "freg",
            "O_V2_fmode": "freg",
            "O_V9_fmode": "freg",
            "O_V10_fmode": "freg",
            "O_V13_fmode": "fadv",
            "O_V15_fmode": "fadv",
            "O_V16_fmode": "fadv",
            "O_V25_fmode": "freg",
            "O_V26_fmode": "fadv",
            "O_V27_fmode": "freg",
            "O_V77_fmode": "freg",
            "O_V79_fmode": "freg",
            "O_V80_fmode": "freg",
            "O_V100_fmode": "freg",
        }

    def _build_default_filters(self) -> dict:
        """Build default filter values (V_ parameters)."""
        return {
            "V_D176.V5": "*All*",   # Age
            "V_D176.V7": "*All*",   # Gender
            "V_D176.V42": "*All*",  # Race
            "V_D176.V43": "*All*",  # Hispanic origin
            "V_D176.V44": "*All*",  # Race/Ethnicity combined
            "V_D176.V11": "*All*",  # Month
            "V_D176.V12": "*All*",  # Weekday
            "V_D176.V17": "*All*",
            "V_D176.V19": "*All*",  # Urbanization
            "V_D176.V20": "*All*",  # Autopsy
            "V_D176.V21": "*All*",  # Place of death
            "V_D176.V22": "*All*",
            "V_D176.V23": "*All*",
            "V_D176.V4": "*All*",
            "V_D176.V51": "*All*",
            "V_D176.V52": "*All*",
            "V_D176.V6": "00",
            "V_D176.V81": "*All*",
            "V_D176.V89": "*All*",

            # Empty value filters
            "V_D176.V1": "",
            "V_D176.V2": "",
            "V_D176.V9": "",
            "V_D176.V10": "",
            "V_D176.V13": "",
            "V_D176.V13_AND": "",
            "V_D176.V15": "",
            "V_D176.V15_AND": "",
            "V_D176.V16": "",
            "V_D176.V16_AND": "",
            "V_D176.V25": "",
            "V_D176.V26": "",
            "V_D176.V26_AND": "",
            "V_D176.V27": "",
            "V_D176.V77": "",
            "V_D176.V79": "",
            "V_D176.V80": "",
            "V_D176.V100": "",
        }

    def _build_finder_parameters(self) -> dict:
        """Build finder stage parameters."""
        return {
            "finder-stage-D176.V1": "codeset",
            "finder-stage-D176.V2": "codeset",
            "finder-stage-D176.V9": "codeset",
            "finder-stage-D176.V10": "codeset",
            "finder-stage-D176.V13": "codeset",
            "finder-stage-D176.V15": "",
            "finder-stage-D176.V16": "",
            "finder-stage-D176.V25": "codeset",
            "finder-stage-D176.V26": "codeset",
            "finder-stage-D176.V27": "codeset",
            "finder-stage-D176.V77": "codeset",
            "finder-stage-D176.V79": "codeset",
            "finder-stage-D176.V80": "codeset",
            "finder-stage-D176.V100": "codeset",
        }

    def _build_default_f_parameters(self) -> dict:
        """Build default F_ (filter display) parameters."""
        return {
            "F_D176.V1": "*All*",
            "F_D176.V2": "*All*",
            "F_D176.V9": "*All*",
            "F_D176.V10": "*All*",
            "F_D176.V13": "*All*",
            "F_D176.V25": "*All*",
            "F_D176.V26": "*All*",
            "F_D176.V27": "*All*",
            "F_D176.V77": "*All*",
            "F_D176.V79": "*All*",
            "F_D176.V80": "*All*",
        }

    def build_query_xml(
        self,
        years: Optional[list] = None,
        group_by: Optional[list] = None,
        cause_of_death: Optional[str] = None,
        age_groups: Optional[list] = None,
        gender: Optional[str] = None,
        race: Optional[list] = None,
    ) -> str:
        """
        Build an XML query string for the CDC WONDER API.

        Args:
            years: List of years to include (e.g., [2020, 2021, 2022])
            group_by: List of fields to group by (e.g., ['year', 'age'])
            cause_of_death: Cause of death filter (use CAUSES_OF_DEATH keys or ICD codes)
            age_groups: List of age group codes to filter
            gender: Gender filter ('M', 'F', or None for all)
            race: List of race codes to filter

        Returns:
            XML string for the API request
        """
        params = {}
        params.update(self._build_base_parameters())
        params.update(self._build_default_filters())
        params.update(self._build_finder_parameters())
        params.update(self._build_default_f_parameters())

        # Set group by fields (B_1 through B_5)
        group_by = group_by or ["year"]
        for i in range(1, 6):
            if i <= len(group_by):
                field = group_by[i-1]
                params[f"B_{i}"] = self.GROUP_BY_FIELDS.get(field, field)
            else:
                params[f"B_{i}"] = "*None*"

        # Set year filter
        years = years or [2024, 2025]
        f_years = [str(y) for y in years]

        # Build XML
        root = ET.Element("request-parameters")

        # Add year parameters (multiple values)
        for name, value in params.items():
            if name == "F_D176.V100":
                continue  # Handle separately
            param = ET.SubElement(root, "parameter")
            name_elem = ET.SubElement(param, "name")
            name_elem.text = name
            value_elem = ET.SubElement(param, "value")
            value_elem.text = str(value)

        # Add year filter (F_D176.V100) with multiple values
        year_param = ET.SubElement(root, "parameter")
        year_name = ET.SubElement(year_param, "name")
        year_name.text = "F_D176.V100"
        for year in f_years:
            year_value = ET.SubElement(year_param, "value")
            year_value.text = year

        # Add cause of death filter if specified
        if cause_of_death:
            cod = self.CAUSES_OF_DEATH.get(cause_of_death, cause_of_death)
            # Set F_D176.V2 (Finder parameter) for cause of death filter
            for elem in root.findall(".//parameter"):
                name_elem = elem.find("name")
                if name_elem is not None and name_elem.text == "F_D176.V2":
                    value_elem = elem.find("value")
                    if value_elem is not None:
                        value_elem.text = cod
                    break

        # Add gender filter if specified
        if gender:
            gender_map = {"M": "M", "F": "F", "male": "M", "female": "F"}
            gender_code = gender_map.get(gender.upper() if isinstance(gender, str) else gender, gender)
            for elem in root.findall(".//parameter"):
                name_elem = elem.find("name")
                if name_elem is not None and name_elem.text == "V_D176.V7":
                    value_elem = elem.find("value")
                    if value_elem is not None:
                        value_elem.text = gender_code
                    break

        # Add age group filter if specified
        if age_groups:
            # age_groups should be a list of age codes like ["1", "1-4", "25-34"]
            for elem in root.findall(".//parameter"):
                name_elem = elem.find("name")
                if name_elem is not None and name_elem.text == "V_D176.V5":
                    value_elem = elem.find("value")
                    if value_elem is not None:
                        # Join multiple age groups or use single value
                        if isinstance(age_groups, list):
                            value_elem.text = age_groups[0] if len(age_groups) == 1 else age_groups[0]
                        else:
                            value_elem.text = str(age_groups)
                    break

        return ET.tostring(root, encoding="unicode")

    def query_from_xml_file(self, xml_path: str, output_format: str = "xml") -> dict:
        """
        Execute a query using an existing XML file as template.

        Args:
            xml_path: Path to the XML query file
            output_format: 'xml' or 'csv'

        Returns:
            Dictionary with response data
        """
        tree = ET.parse(xml_path)
        root = tree.getroot()

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

        xml_str = ET.tostring(root, encoding="unicode")
        return self._send_request(xml_str)

    def query(
        self,
        years: Optional[list] = None,
        group_by: Optional[list] = None,
        cause_of_death: Optional[str] = None,
        age_groups: Optional[list] = None,
        gender: Optional[str] = None,
        race: Optional[list] = None,
    ) -> dict:
        """
        Execute a query with the specified parameters.

        Args:
            years: List of years to query
            group_by: Fields to group results by
            cause_of_death: Cause of death filter
            age_groups: Age groups to include
            gender: Gender filter
            race: Race filter

        Returns:
            Dictionary with response data including 'data' and 'metadata'
        """
        xml = self.build_query_xml(
            years=years,
            group_by=group_by,
            cause_of_death=cause_of_death,
            age_groups=age_groups,
            gender=gender,
            race=race,
        )
        return self._send_request(xml)

    def _send_request(self, xml_request: str) -> dict:
        """Send a request to the CDC WONDER API."""
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {
            "request_xml": xml_request,
            "accept_datause_restrictions": "true",
        }

        response = requests.post(self.endpoint, headers=headers, data=data)
        response.raise_for_status()

        return self._parse_response(response.text)

    def _parse_response(self, response_text: str) -> dict:
        """Parse the XML response from CDC WONDER."""
        result = {
            "raw_xml": response_text,
            "data": [],
            "headers": [],
            "caveats": [],
            "error": None,
        }

        try:
            root = ET.fromstring(response_text)

            # Check for errors
            error = root.find(".//error")
            if error is not None:
                result["error"] = error.text
                return result

            # Build a mapping of variable/hier-level codes to labels
            var_labels = {}
            for var in root.findall(".//variable[@code][@label]"):
                code = var.get("code", "")
                label = var.get("label", "")
                if code and label:
                    var_labels[code] = label
                # Also get hier-level labels
                for hier in var.findall("hier-level"):
                    hier_code = hier.get("code", "")
                    hier_label = hier.get("label", "")
                    if hier_code and hier_label:
                        var_labels[hier_code] = hier_label

            # Extract measure labels for headers
            measure_labels = {}
            for measure in root.findall(".//measure[@code][@label]"):
                code = measure.get("code", "")
                label = measure.get("label", "")
                if code and label:
                    measure_labels[code] = label

            # Get groupby variable codes from request parameters (B_1, B_2, etc.)
            groupby_codes = []
            for param in root.findall(".//request-parameters/parameter"):
                name_elem = param.find("name")
                value_elem = param.find("value")
                if name_elem is not None and value_elem is not None:
                    name = name_elem.text or ""
                    value = value_elem.text or ""
                    if name.startswith("B_") and value and value != "*None*":
                        groupby_codes.append(value)

            # Build groupby column headers
            groupby_labels = []
            for code in groupby_codes:
                label = var_labels.get(code, code)
                groupby_labels.append(label)

            # Get selected measures to determine column headers
            # Look for measure-selections-all (near data-table) which has the selected measures
            measure_selections = None
            for ms in root.findall(".//measure-selections-all"):
                measure_selections = ms
                break
            if measure_selections is None:
                measure_selections = root.find(".//measure-selections")

            selected_measures = []
            if measure_selections is not None:
                for measure in measure_selections.findall("measure"):
                    code = measure.get("code", "")
                    if code in measure_labels:
                        selected_measures.append(measure_labels[code])

            # Build headers: groupby columns + measure columns
            result["headers"] = groupby_labels + selected_measures

            # Extract data table
            data_table = root.find(".//data-table")
            num_groupby_cols = len(groupby_codes)
            num_measure_cols = len(selected_measures)
            expected_cols = num_groupby_cols + num_measure_cols
            last_groupby_values = [""] * num_groupby_cols

            if data_table is not None:
                # Get rows (skip total rows marked with c="1" or dt attributes)
                for row in data_table.findall(".//r"):
                    row_data = []
                    is_total_row = False

                    cells = row.findall("c")

                    # Check if this is a total row (first cell has c="1" or any cell has dt)
                    first_cell = cells[0] if cells else None
                    if first_cell is not None and (first_cell.get("c") or first_cell.get("dt")):
                        continue

                    # CDC WONDER omits leading cells when groupby values repeat
                    # Calculate how many groupby cells are missing
                    num_cells = len(cells)
                    missing_cols = expected_cols - num_cells

                    # Prepend carried-forward values for missing groupby columns
                    for i in range(missing_cols):
                        if i < num_groupby_cols:
                            row_data.append(last_groupby_values[i])

                    # Process actual cells
                    for i, cell in enumerate(cells):
                        val = cell.get("v") or cell.get("l") or cell.text or ""
                        row_data.append(val)

                        # Update last groupby values for cells that are groupby columns
                        actual_col_idx = missing_cols + i
                        if actual_col_idx < num_groupby_cols and val:
                            last_groupby_values[actual_col_idx] = val

                    if row_data:
                        result["data"].append(row_data)

            # Extract caveats
            for caveat in root.findall(".//caveat"):
                caveat_text = caveat.text or ""
                if caveat_text.strip():
                    result["caveats"].append(caveat_text.strip())

        except ET.ParseError as e:
            result["error"] = f"Failed to parse XML response: {e}"

        return result

    def save_to_csv(self, result: dict, output_path: str):
        """Save query results to a CSV file."""
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if result["headers"]:
                writer.writerow(result["headers"])
            for row in result["data"]:
                writer.writerow(row)
        print(f"Saved {len(result['data'])} rows to {output_path}")

    def save_raw_xml(self, result: dict, output_path: str):
        """Save the raw XML response to a file."""
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(result["raw_xml"])
        print(f"Saved raw XML to {output_path}")


def main():
    """Example usage of the CDC WONDER client."""
    client = CDCWonderClient()

    print("CDC WONDER API Client")
    print("=" * 50)
    print("\nAvailable group by fields:")
    for name, code in client.GROUP_BY_FIELDS.items():
        print(f"  {name}: {code}")

    print("\nAvailable cause of death filters:")
    for name, code in client.CAUSES_OF_DEATH.items():
        print(f"  {name}: {code}")

    print("\nExample query:")
    print("  result = client.query(years=[2023, 2024], group_by=['year', 'age'])")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
CDC WONDER Data Download CLI

Command-line interface for downloading mortality data from CDC WONDER.

Usage:
    python cli.py --years 2023 2024 --group-by year age --output deaths.csv
    python cli.py --from-xml query.xml --output results.csv
    python cli.py --preset covid_by_age --output covid_deaths.csv
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

from cdc_wonder import CDCWonderClient


# Preset query configurations
PRESETS = {
    "all_causes_by_year": {
        "description": "All causes of death grouped by year",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["year"],
        "cause_of_death": None,
    },
    "all_causes_by_year_month": {
        "description": "All causes of death grouped by year and month",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["year", "month"],
        "cause_of_death": None,
    },
    "all_causes_by_age": {
        "description": "All causes of death grouped by age group",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["age", "year"],
        "cause_of_death": None,
    },
    "covid_by_year": {
        "description": "COVID-19 deaths grouped by year",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["year"],
        "cause_of_death": "covid19",
    },
    "covid_by_age": {
        "description": "COVID-19 deaths grouped by age and year",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["age", "year"],
        "cause_of_death": "covid19",
    },
    "covid_by_month": {
        "description": "COVID-19 deaths grouped by year and month",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["year", "month"],
        "cause_of_death": "covid19",
    },
    "heart_disease_by_year": {
        "description": "Heart disease deaths by year",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["year"],
        "cause_of_death": "heart_disease",
    },
    "cancer_by_year": {
        "description": "Cancer deaths by year",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["year"],
        "cause_of_death": "cancer",
    },
    "drug_overdose_by_year": {
        "description": "Drug overdose deaths by year",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["year"],
        "cause_of_death": "drug_overdose",
    },
    "drug_overdose_by_age": {
        "description": "Drug overdose deaths by age and year",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["age", "year"],
        "cause_of_death": "drug_overdose",
    },
    "all_by_gender_age": {
        "description": "All deaths grouped by gender and age",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["gender", "age", "year"],
        "cause_of_death": None,
    },
    "all_by_race": {
        "description": "All deaths grouped by race and year",
        "years": [2020, 2021, 2022, 2023, 2024, 2025],
        "group_by": ["race", "year"],
        "cause_of_death": None,
    },
}


def list_presets():
    """List all available preset configurations."""
    print("\nAvailable presets:")
    print("-" * 60)
    for name, config in PRESETS.items():
        print(f"  {name}")
        print(f"    {config['description']}")
        print(f"    Years: {config['years']}")
        print(f"    Group by: {config['group_by']}")
        if config['cause_of_death']:
            print(f"    Cause: {config['cause_of_death']}")
        print()


def list_options():
    """List available group-by fields and cause filters."""
    client = CDCWonderClient()

    print("\nAvailable group-by fields:")
    print("-" * 40)
    for name in sorted(client.GROUP_BY_FIELDS.keys()):
        print(f"  {name}")

    print("\nAvailable cause of death filters:")
    print("-" * 40)
    for name in sorted(client.CAUSES_OF_DEATH.keys()):
        print(f"  {name}")


def run_query(args):
    """Execute a query based on command line arguments."""
    client = CDCWonderClient()

    # Determine query parameters
    if args.from_xml:
        print(f"Loading query from XML file: {args.from_xml}")
        result = client.query_from_xml_file(args.from_xml)
    elif args.preset:
        if args.preset not in PRESETS:
            print(f"Error: Unknown preset '{args.preset}'")
            list_presets()
            return 1
        preset = PRESETS[args.preset]
        print(f"Running preset: {args.preset}")
        print(f"  {preset['description']}")
        result = client.query(
            years=preset["years"],
            group_by=preset["group_by"],
            cause_of_death=preset["cause_of_death"],
        )
    else:
        # Custom query from arguments
        years = args.years or [2024, 2025]
        group_by = args.group_by or ["year"]
        cause = args.cause

        print(f"Running custom query:")
        print(f"  Years: {years}")
        print(f"  Group by: {group_by}")
        if cause:
            print(f"  Cause: {cause}")

        result = client.query(
            years=years,
            group_by=group_by,
            cause_of_death=cause,
            gender=args.gender,
        )

    # Check for errors
    if result.get("error"):
        print(f"\nError: {result['error']}")
        return 1

    # Report results
    print(f"\nReceived {len(result['data'])} rows of data")

    if result.get("caveats"):
        print("\nCaveats:")
        for caveat in result["caveats"][:3]:  # Show first 3 caveats
            print(f"  - {caveat[:100]}...")

    # Save output
    if args.output:
        output_path = Path(args.output)
        if output_path.suffix.lower() == ".xml":
            client.save_raw_xml(result, args.output)
        else:
            client.save_to_csv(result, args.output)
    elif args.save_xml:
        client.save_raw_xml(result, args.save_xml)

    # Print preview if not quiet
    if not args.quiet and result["data"]:
        print("\nData preview (first 5 rows):")
        if result["headers"]:
            print("  " + " | ".join(result["headers"][:5]))
            print("  " + "-" * 60)
        for row in result["data"][:5]:
            print("  " + " | ".join(str(v)[:15] for v in row[:5]))

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Download mortality data from CDC WONDER",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cli.py --preset covid_by_year --output covid.csv
  python cli.py --years 2023 2024 --group-by year age --output data.csv
  python cli.py --from-xml "query.xml" --output results.csv
  python cli.py --list-presets
  python cli.py --list-options
        """,
    )

    # Query source options
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--from-xml",
        metavar="FILE",
        help="Use an existing XML query file as template",
    )
    source.add_argument(
        "--preset",
        metavar="NAME",
        help="Use a preset query configuration",
    )

    # Query parameters
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        help="Years to include (e.g., --years 2020 2021 2022)",
    )
    parser.add_argument(
        "--group-by",
        nargs="+",
        metavar="FIELD",
        help="Fields to group by (e.g., --group-by year age)",
    )
    parser.add_argument(
        "--cause",
        metavar="NAME",
        help="Filter by cause of death (e.g., covid19, heart_disease)",
    )
    parser.add_argument(
        "--gender",
        choices=["M", "F"],
        help="Filter by gender",
    )

    # Output options
    parser.add_argument(
        "-o", "--output",
        metavar="FILE",
        help="Output file path (.csv or .xml)",
    )
    parser.add_argument(
        "--save-xml",
        metavar="FILE",
        help="Save the raw XML response",
    )
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Suppress output preview",
    )

    # Info options
    parser.add_argument(
        "--list-presets",
        action="store_true",
        help="List available preset configurations",
    )
    parser.add_argument(
        "--list-options",
        action="store_true",
        help="List available group-by fields and cause filters",
    )

    args = parser.parse_args()

    # Handle info options
    if args.list_presets:
        list_presets()
        return 0

    if args.list_options:
        list_options()
        return 0

    # Must have either a query source or output specified
    if not any([args.from_xml, args.preset, args.years, args.group_by, args.cause]):
        parser.print_help()
        print("\nNo query parameters specified. Use --help for usage information.")
        return 1

    return run_query(args)


if __name__ == "__main__":
    sys.exit(main())

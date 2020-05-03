import argparse
from ckanapi import RemoteCKAN
from csv import DictReader
from slugify import slugify
from pprint import pprint
from datetime import datetime


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apikey",
        required=True,
        help="CKAN API Key (You can see this on your profile page)",
    )
    parser.add_argument(
        "--ckan-url", required=True, help="e.g. https://data.openup.org.za"
    )
    parser.add_argument('indexfile', nargs=1)
    return parser.parse_args()


def read_index(filename):
    with open(filename) as indexfile:
        reader = DictReader(indexfile)
        for row in reader:
            yield row


def make_tags(tags_string):
    return [{"name": slugify(t.strip())} for t in tags_string.strip().split(",")]


def parse_date(date_string):
    return datetime.strptime(date_string, "%m/%d/%Y %H:%M:%S %p +0000")


def dataset_fields(item):
    return {
        "name": slugify(item["Name"]),
        "title": item["Name"],
        "resources": [],
        "tags": make_tags(item["Keywords"]),
        "license_name": item["License"],
        "private": item["Public"] == "FALSE",
        "notes": item["Description"],
        "group_name": item["Category"],
        "maintainer": item["Owner"],
        "maintainer_email": item["Contact Email"],
        "organization_name": item["data_provided_by"],
    }


def resource_fields(item):
    fields = {
        "name": item["Name"],
        "created": parse_date(item["Creation Date"]).isoformat(),
        "last_modified": parse_date(item["Last Update Date (data)"]).isoformat(),
    }
    if item["Parent UID"]:
        fields["description"] = item["Description"]
    return fields


def socrata_to_pre_ckan(socrata):
    datasets = dict()
    organizations = dict()
    resources = dict()
    resource_datasets = []

    for item in socrata:
        socrata_id = item["U ID"]
        if item["Parent UID"]:
            dataset_socrata_id = item["Parent UID"]
        else:
            dataset_socrata_id = socrata_id
            datasets[socrata_id] = dataset_fields(item)
        resources[socrata_id] = resource_fields(item)
        resource_datasets.append((socrata_id, dataset_socrata_id))

    for resource_id, dataset_id in resource_datasets:
        datasets[dataset_id]["resources"].append(resources[resource_id])

    return organizations, datasets


def main():
    args = parse_args()
    ckan = RemoteCKAN(args.ckan_url, apikey=args.apikey)

    ckan_organizations = ckan.action.organization_list(all_fields=True)
    ckan_licenses = ckan.action.license_list()

    org_name_to_id = {o["title"]: o["name"] for o in ckan_organizations}

    socrata_index = read_index(args.indexfile[0])
    organizations, datasets = socrata_to_pre_ckan(socrata_index)

    for item in datasets.items():
        pprint(item)



if __name__ == "__main__":
    main()

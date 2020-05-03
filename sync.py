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
    return [{"name": slugify(t.strip())} for t in tags_string.strip().split(",") if t.strip()]


def parse_date(date_string):
    return datetime.strptime(date_string, "%m/%d/%Y %H:%M:%S %p +0000")


def dataset_fields(item):
    fields = {
        "name": slugify(item["Name"]),
        "title": item["Name"],
        "resources": [],
        "tags": make_tags(item["Keywords"]),
        "license_name": item["License"],
        "private": item["Public"] == "false",
        "notes": item["Description"],
        "maintainer": item["Owner"],
        "maintainer_email": item["Contact Email"],
        "organization_name": None,
    }

    if item["Category"]:
        fields["groups"] = [{"name": item["Category"]}]
    if item["data_provided_by"]:
        fields["organization_name"] = item["data_provided_by"]
    return fields


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
    resources = dict()
    resource_datasets = []

    for item in socrata:
        if item["Derived View"] == "true":
            continue
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

    return datasets.values()


def get_missing_orgs(ckan_organizations, pre_ckan_datasets):
    existing_org_names = set()
    for org in ckan_organizations:
        existing_org_names.add(org["title"])
    dataset_org_names = set()
    for dataset in pre_ckan_datasets:
        if dataset["organization_name"]:
            dataset_org_names.add(dataset["organization_name"])
    return dataset_org_names - existing_org_names


def main():
    args = parse_args()
    ckan = RemoteCKAN(args.ckan_url, apikey=args.apikey)

    ckan_organizations = ckan.action.organization_list(all_fields=True)
    ckan_licenses = ckan.action.license_list()
    ckan_groups = ckan.action.group_list()

    socrata_index = read_index(args.indexfile[0])
    pre_ckan_datasets = socrata_to_pre_ckan(socrata_index)

    new_org_names = get_missing_orgs(ckan_organizations, pre_ckan_datasets)

    print(new_org_names)




if __name__ == "__main__":
    main()

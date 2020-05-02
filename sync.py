import argparse
from ckanapi import RemoteCKAN
from csv import DictReader
from slugify import slugify
from pprint import pprint


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


def dataset_fields(socrata_item):
    return {
        "name": slugify(socrata_item["Name"]),
        "title": socrata_item["Name"],
        "resources": [],
    }


def resource_fields(socrata_item):
    return {
        "name": socrata_item["Name"],
    }


def socrata_to_ckan(socrata):
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
    org_name_to_id = {o["title"]: o["name"] for o in ckan_organizations}

    socrata_index = read_index(args.indexfile[0])
    organizations, datasets = socrata_to_ckan(socrata_index)

    for item in datasets.items():
        if len(item[1]["resources"]) > 1:
            pprint(item)


if __name__ == "__main__":
    main()

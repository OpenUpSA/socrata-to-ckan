import argparse
from ckanapi import RemoteCKAN
from csv import DictReader
from slugify import slugify
from pprint import pprint
from datetime import datetime
import logging
import glob
import os

logger = logging.getLogger(__name__)

LICENCE_IDS = {
    "Creative Commons Attribution | Share Alike 3.0 Unported": 'cc-by-sa',
    "Creative Commons Attribution | Share Alike 4.0 International": 'cc-by-sa',
    "Open Data Commons Attribution License": 'odc-by',
    "Open Data Commons Public Domain Dedication and License": 'odc-pddl',
    "Open Database License": 'odc-odbl',
    "Public Domain": 'other-pd',
}


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
    parser.add_argument('filesdir', nargs=1)
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
        "name": slugify(item['Name'] + "-" + item['U ID']),
        "title": item["Name"],
        "resources": [],
        "tags": make_tags(item["Keywords"]),
        "private": item["Public"] == "false",
        "notes": item["Description"],
        "maintainer": item["Owner"],
        "maintainer_email": item["Contact Email"],
        "organization_title": None,
        "group_title": None,
        "url": item["source_link"],
    }

    if item["Category"]:
        fields["group_title"] = item["Category"]
    if item["data_provided_by"]:
        fields["organization_title"] = item["data_provided_by"]
    if item["License"] in LICENCE_IDS:
        fields["license_id"] = LICENCE_IDS[item["License"]]
    return fields


def resource_fields(item):
    fields = {
        "name": item["Name"],
        "created": parse_date(item["Creation Date"]).isoformat(),
        "last_modified": parse_date(item["Last Update Date (data)"]).isoformat(),
        "socrata_id": item["U ID"],
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
    existing_org_titles = set()
    for org in ckan_organizations:
        existing_org_titles.add(org["title"])
    dataset_org_titles = set()
    for dataset in pre_ckan_datasets:
        if dataset["organization_title"]:
            dataset_org_titles.add(dataset["organization_title"])
    return dataset_org_titles - existing_org_titles


def get_missing_groups(ckan_groups, pre_ckan_datasets):
    existing_group_titles = set()
    for group in ckan_groups:
        existing_group_titles.add(group["title"])
    dataset_group_titles = set()
    for dataset in pre_ckan_datasets:
        if dataset["group_title"]:
            dataset_group_titles.add(dataset["group_title"])
    return dataset_group_titles - existing_group_titles


def add_resource_paths(datasets, file_paths):
    for dataset in datasets:
        path_resources = []
        for resource in dataset["resources"]:
            for path in file_paths:
                if resource["socrata_id"] in path:
                    path_resource = resource.copy()
                    del path_resource["socrata_id"]
                    path_resource["path"] = path
                    filename = os.path.basename(path)
                    extension = os.path.splitext(filename)[1]
                    path_resource["format"] = extension[1:].upper()
                    path_resources.append(path_resource)
        dataset["resources"] = path_resources


def add_group(datasets, ckan_groups):
    group_by_title = {g["title"]: g for g in ckan_groups}
    for dataset in datasets:
        group_title = dataset.pop("group_title")
        if group_title:
            dataset["groups"] = [group_by_title[group_title]]


def add_organization(datasets, ckan_organizations):
    organization_by_title = {o["title"]: o for o in ckan_organizations}
    for dataset in datasets:
        org_title = dataset.pop("organization_title")
        if org_title:
            dataset["owner_org"] = organization_by_title[org_title]["id"]


def sync_dataset(ckan, dataset):
    resources = dataset.pop("resources")
    print("Creating dataset:")
    pprint(dataset)
    dataset = ckan.action.package_create(**dataset)
    for resource in resources:
        path = resource.pop("path")
        with open(path, 'rb') as fd:
            resource["upload"] = fd
            resource["package_id"] = dataset["id"]
            print("Creating resource:")
            pprint(resource)
            ckan.action.resource_create(**resource)


def main():
    args = parse_args()
    ckan = RemoteCKAN(args.ckan_url, apikey=args.apikey)

    ckan_organizations = ckan.action.organization_list(all_fields=True)
    ckan_groups = ckan.action.group_list(all_fields=True)

    socrata_index = read_index(args.indexfile[0])
    datasets = socrata_to_pre_ckan(socrata_index)

    new_org_titles = get_missing_orgs(ckan_organizations, datasets)
    for org_title in new_org_titles:
        new_org = ckan.action.organization_create(name=slugify(org_title), title=org_title)
        ckan_organizations.append(new_org)

    new_group_titles = get_missing_groups(ckan_groups, datasets)
    for group_title in new_group_titles:
        new_group = ckan.action.group_create(name=slugify(group_title), title=group_title)
        ckan_groups.append(new_group)

    add_group(datasets, ckan_groups)
    add_organization(datasets, ckan_organizations)

    file_paths = list(glob.iglob(args.filesdir[0] + "/**/*"))
    add_resource_paths(datasets, file_paths)

    for dataset in datasets:
        sync_dataset(ckan, dataset)


if __name__ == "__main__":
    main()

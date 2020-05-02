import argparse
from ckanapi import RemoteCKAN
from csv import DictReader

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


def main():
    args = parse_args()
    ckan = RemoteCKAN(args.ckan_url, apikey=args.apikey)

    ckan_organizations = ckan.action.organization_list(all_fields=True)
    org_name_to_id = {o["title"]: o["name"] for o in ckan_organizations}

    socrata_index = read_index(args.indexfile[0])
    for item in socrata_index:
        print(item["U ID"])


if __name__ == "__main__":
    main()

import argparse

import requests


class SimpleHttpClient:
    def __init__(self, base_url):
        self.base_url = base_url

    def make_request(self, endpoint, method="GET", data=None, headers=None):
        url = f"{self.base_url}/{endpoint}"

        try:
            if method == "GET":
                response = requests.get(url, headers=headers)
            elif method == "POST":
                response = requests.post(url, data=data, headers=headers)
            elif method == "PUT":
                response = requests.put(url, data=data, headers=headers)
            elif method == "DELETE":
                response = requests.delete(url, headers=headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            return response.text

        except requests.RequestException as e:
            print(f"Request failed: {e}")
            return None


def parse_arguments():
    parser = argparse.ArgumentParser(description="Simple HTTP Client")
    parser.add_argument("base_url", help="Base URL for the API")
    parser.add_argument("endpoint", help="API endpoint")
    parser.add_argument(
        "--method", default="GET", help="HTTP method (default: GET)"
    )
    parser.add_argument("--data", help="Data for POST or PUT requests")
    parser.add_argument(
        "--header", help="Custom header in the format 'Key: Value'"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    http_client = SimpleHttpClient(args.base_url)

    data = args.data.encode("utf-8") if args.data else None
    headers = (
        {
            header.split(":")[0].strip(): header.split(":")[1].strip()
            for header in [args.header]
        }
        if args.header
        else None
    )

    response_content = http_client.make_request(
        args.endpoint, method=args.method, data=data, headers=headers
    )

    if response_content is not None:
        print(f"Response Content:\n{response_content}")

# poetry run python populate_db_client.py http://localhost:8000 api/alive/

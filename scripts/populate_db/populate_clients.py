import requests


BASE_URL = "http://localhost:8000"
CREDENTIALS = dict(username="admin@fractal.xy", password="1234")  # nosec


class SimpleHttpClient:
    base_url: str

    def __init__(self, base_url):
        self.base_url = base_url

    # login needs a custom func because the content type of this endpoint
    # is not json but x-www-form-urlencoded,
    # so it requires `data` attribute in requests.post
    def login(self, credentials: str):
        response = requests.post(
            f"{self.base_url}/auth/token/login/",
            data=credentials,
        )
        return response

    def make_request(self, endpoint, method="GET", data=None, headers=None):
        url = f"{self.base_url}/{endpoint}"

        try:
            if method == "GET":
                response = requests.get(url, headers=headers)
            elif method == "POST":
                response = requests.post(url, json=data, headers=headers)
            elif method == "PATCH":
                response = requests.patch(url, json=data, headers=headers)
            elif method == "DELETE":
                response = requests.delete(url, headers=headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            return response

        except requests.RequestException as e:
            print(f"Request failed: {e}")
            return None


class BearerHttpClient(SimpleHttpClient):
    bearer_token: str

    def __init__(self, base_url: str, bearer_token: str):
        super().__init__(base_url=base_url)
        self.bearer_token = bearer_token
        self.headers = {"Authorization": f"Bearer {bearer_token}"}

    def make_request(self, endpoint: str, method: str = "GET", data=None):
        res = super().make_request(
            endpoint=endpoint, method=method, data=data, headers=self.headers
        )
        return res


def auth_client(base_url: str, credentials: str) -> BearerHttpClient:
    anon_client = SimpleHttpClient(base_url)

    res = anon_client.login(credentials=credentials)

    bearer_token = res.json().get("access_token")

    bearer_client = BearerHttpClient(
        base_url=base_url, bearer_token=bearer_token
    )

    return bearer_client

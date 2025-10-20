import json
import urllib.parse
import urllib.request

BASE = "http://localhost:8000"


def fetch(path: str) -> dict:
    with urllib.request.urlopen(BASE + path) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> None:
    # Minimal query to keep payload small
    params = urllib.parse.urlencode({
        "countries": "UA",
        "indicators": "GDP",
        "start_year": 2019,
        "end_year": 2021,
    })

    print("/api/worldbank/indicators ...")
    data = fetch(f"/api/worldbank/indicators?{params}")
    print("keys:", list(data.keys()))
    print("total_records:", data.get("total_records"))

    print("/api/worldbank/normalized ...")
    norm = fetch(f"/api/worldbank/normalized?{params}&currency=USD")
    print("keys:", list(norm.keys()))
    print("total_records:", norm.get("total_records"))


if __name__ == "__main__":
    main()

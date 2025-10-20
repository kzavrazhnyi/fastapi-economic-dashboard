import sys
import os

# Ensure project root on sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from app.worldbank_client import WorldBankDataProvider


def main() -> None:
    provider = WorldBankDataProvider()

    # Minimal query to maximize chance of success when API is flaky
    countries = ["UA"]
    indicators = ["GDP"]  # friendly name supported
    start_year = 2019
    end_year = 2021

    df = provider.get_economic_indicators(
        country_codes=countries,
        indicators=indicators,
        start_year=start_year,
        end_year=end_year,
    )

    print(f"Rows: {len(df)} Columns: {list(df.columns)}")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()

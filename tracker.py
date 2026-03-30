from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
DATA_PATH = BASE_DIR / "data" / "prices.csv"
SUMMARY_PATH = BASE_DIR / "output" / "weekly_summary.txt"
PRICE_URL = "https://www.investing.com/commodities/malaysian-crude-palm-oil-futures-historical-data"
FX_URL = "https://api.frankfurter.app"
SOURCE_LABEL = "Malaysia CPO futures (Investing.com CPOc1)"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/136.0.0.0 Safari/537.36"
)
FIELDNAMES = [
    "trade_date",
    "price_usd_per_mt",
    "usd_inr",
    "fx_rate_date",
    "freight_usd_per_mt",
    "insurance_usd_per_mt",
    "cif_usd_per_mt",
    "cif_inr_per_mt",
    "effective_import_duty_pct",
    "import_duty_inr_per_mt",
    "port_charges_inr_per_mt",
    "other_cost_inr_per_mt",
    "total_landed_inr_per_mt",
    "total_landed_inr_per_kg",
    "source",
]
DEFAULT_CONFIG = {
    "history_days": 20,
    "price_source": {
        "name": SOURCE_LABEL,
        "url": PRICE_URL,
    },
    "fx_source": {
        "name": "Frankfurter USD/INR",
        "url": FX_URL,
    },
    "costs": {
        "freight_usd_per_mt": 35.0,
        "insurance_pct_of_cfr": 0.0025,
        "effective_import_duty_pct": 0.165,
        "port_charges_inr_per_mt": 1500.0,
        "other_cost_inr_per_mt": 0.0,
    },
}


class TrackerError(RuntimeError):
    pass


def deep_merge(base: dict, override: dict) -> dict:
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def load_config() -> dict:
    config = json.loads(json.dumps(DEFAULT_CONFIG))
    if CONFIG_PATH.exists():
        user_config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        deep_merge(config, user_config)
    return config


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return session


def fetch_price_history(session: requests.Session, history_days: int) -> list[dict]:
    response = session.get(PRICE_URL, timeout=30)
    response.raise_for_status()

    text = BeautifulSoup(response.text, "lxml").get_text(" ", strip=True)
    section_match = re.search(
        r"Date\s+Price\s+Open\s+High\s+Low\s+Vol\.\s+Change %(.*?)Highest:",
        text,
        re.S,
    )
    if not section_match:
        raise TrackerError("Could not locate the Malaysia CPO historical table.")

    section = section_match.group(1)
    row_pattern = re.compile(
        r"([A-Z][a-z]{2} \d{2}, \d{4})\s+"
        r"([\d,]+\.\d+)\s+"
        r"[\d,]+\.\d+\s+"
        r"[\d,]+\.\d+\s+"
        r"[\d,]+\.\d+"
        r"(?:\s+[\d.]+[KMB])?\s*"
        r"(?:[+-]?\d+\.\d+%|0\.00%)"
    )

    parsed = {}
    for match in row_pattern.finditer(section):
        trade_date = datetime.strptime(match.group(1), "%b %d, %Y").date().isoformat()
        parsed[trade_date] = {
            "trade_date": trade_date,
            "price_usd_per_mt": float(match.group(2).replace(",", "")),
        }

    if not parsed:
        raise TrackerError("No Malaysia CPO rows were parsed from the source page.")

    rows = [parsed[key] for key in sorted(parsed)]
    return rows[-history_days:]


def fetch_usd_inr(session: requests.Session, trade_date: str, cache: dict[str, dict]) -> dict:
    if trade_date in cache:
        return cache[trade_date]

    response = session.get(f"{FX_URL}/{trade_date}?from=USD&to=INR", timeout=20)
    response.raise_for_status()
    payload = response.json()
    rates = payload.get("rates", {})
    if "INR" not in rates:
        raise TrackerError(f"USD/INR rate missing for {trade_date}.")

    cache[trade_date] = {
        "fx_rate_date": payload["date"],
        "usd_inr": float(rates["INR"]),
    }
    return cache[trade_date]


def calculate_rows(price_rows: list[dict], config: dict, session: requests.Session) -> list[dict]:
    costs = config["costs"]
    fx_cache: dict[str, dict] = {}
    output_rows = []

    for price_row in price_rows:
        fx_row = fetch_usd_inr(session, price_row["trade_date"], fx_cache)
        freight = float(costs["freight_usd_per_mt"])
        insurance_pct = float(costs["insurance_pct_of_cfr"])
        duty_pct = float(costs["effective_import_duty_pct"])
        port_charges = float(costs["port_charges_inr_per_mt"])
        other_cost = float(costs["other_cost_inr_per_mt"])

        cfr_usd = price_row["price_usd_per_mt"] + freight
        insurance_usd = cfr_usd * insurance_pct
        cif_usd = cfr_usd + insurance_usd
        cif_inr = cif_usd * fx_row["usd_inr"]
        import_duty_inr = cif_inr * duty_pct
        total_landed_inr = cif_inr + import_duty_inr + port_charges + other_cost

        output_rows.append(
            {
                "trade_date": price_row["trade_date"],
                "price_usd_per_mt": price_row["price_usd_per_mt"],
                "usd_inr": fx_row["usd_inr"],
                "fx_rate_date": fx_row["fx_rate_date"],
                "freight_usd_per_mt": freight,
                "insurance_usd_per_mt": insurance_usd,
                "cif_usd_per_mt": cif_usd,
                "cif_inr_per_mt": cif_inr,
                "effective_import_duty_pct": duty_pct,
                "import_duty_inr_per_mt": import_duty_inr,
                "port_charges_inr_per_mt": port_charges,
                "other_cost_inr_per_mt": other_cost,
                "total_landed_inr_per_mt": total_landed_inr,
                "total_landed_inr_per_kg": total_landed_inr / 1000.0,
                "source": SOURCE_LABEL,
            }
        )

    return output_rows


def read_existing_rows() -> dict[str, dict]:
    if not DATA_PATH.exists() or DATA_PATH.stat().st_size == 0:
        return {}

    with DATA_PATH.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return {row["trade_date"]: row for row in reader if row.get("trade_date")}


def format_row(row: dict) -> dict:
    return {
        "trade_date": row["trade_date"],
        "price_usd_per_mt": f"{row['price_usd_per_mt']:.2f}",
        "usd_inr": f"{row['usd_inr']:.4f}",
        "fx_rate_date": row["fx_rate_date"],
        "freight_usd_per_mt": f"{row['freight_usd_per_mt']:.2f}",
        "insurance_usd_per_mt": f"{row['insurance_usd_per_mt']:.2f}",
        "cif_usd_per_mt": f"{row['cif_usd_per_mt']:.2f}",
        "cif_inr_per_mt": f"{row['cif_inr_per_mt']:.2f}",
        "effective_import_duty_pct": f"{row['effective_import_duty_pct']:.4f}",
        "import_duty_inr_per_mt": f"{row['import_duty_inr_per_mt']:.2f}",
        "port_charges_inr_per_mt": f"{row['port_charges_inr_per_mt']:.2f}",
        "other_cost_inr_per_mt": f"{row['other_cost_inr_per_mt']:.2f}",
        "total_landed_inr_per_mt": f"{row['total_landed_inr_per_mt']:.2f}",
        "total_landed_inr_per_kg": f"{row['total_landed_inr_per_kg']:.4f}",
        "source": row["source"],
    }


def write_csv(rows: list[dict]) -> None:
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    existing = read_existing_rows()
    for row in rows:
        existing[row["trade_date"]] = format_row(row)

    sorted_rows = [existing[key] for key in sorted(existing)]
    with DATA_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(sorted_rows)


def write_summary(rows: list[dict], config: dict) -> None:
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    latest = rows[-1]
    recent = rows[-min(5, len(rows)) :]
    base_row = rows[-6] if len(rows) >= 6 else rows[0]

    price_change = latest["price_usd_per_mt"] - base_row["price_usd_per_mt"]
    landed_change = latest["total_landed_inr_per_mt"] - base_row["total_landed_inr_per_mt"]
    average_landed = sum(row["total_landed_inr_per_mt"] for row in recent) / len(recent)

    summary = "\n".join(
        [
            "Palm Oil Landed Price Tracker",
            f"Generated at (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}",
            f"Latest trade date: {latest['trade_date']}",
            f"Price source: {config['price_source']['name']}",
            f"FX source: {config['fx_source']['name']} ({latest['fx_rate_date']})",
            "",
            f"Malaysia CPO close: USD {latest['price_usd_per_mt']:.2f} / MT",
            f"USD/INR: {latest['usd_inr']:.4f}",
            f"Estimated landed cost: INR {latest['total_landed_inr_per_mt']:.2f} / MT",
            f"Estimated landed cost: INR {latest['total_landed_inr_per_kg']:.4f} / kg",
            "",
            "Current assumptions",
            f"- Freight: USD {latest['freight_usd_per_mt']:.2f} / MT",
            f"- Insurance: {latest['effective_import_duty_pct'] * 0 + config['costs']['insurance_pct_of_cfr']:.4f} of CFR",
            f"- Effective import duty: {latest['effective_import_duty_pct']:.4f}",
            f"- Port charges: INR {latest['port_charges_inr_per_mt']:.2f} / MT",
            f"- Other cost: INR {latest['other_cost_inr_per_mt']:.2f} / MT",
            "",
            "Five-session move",
            f"- CPO price move: USD {price_change:.2f} / MT",
            f"- Landed move: INR {landed_change:.2f} / MT",
            f"- Five-session average landed: INR {average_landed:.2f} / MT",
        ]
    )
    SUMMARY_PATH.write_text(summary + "\n", encoding="utf-8")


def main() -> None:
    config = load_config()
    session = build_session()
    price_rows = fetch_price_history(session, int(config["history_days"]))
    landed_rows = calculate_rows(price_rows, config, session)
    write_csv(landed_rows)
    write_summary(landed_rows, config)
    latest = landed_rows[-1]
    print(
        f"Updated through {latest['trade_date']} | "
        f"Malaysia CPO USD {latest['price_usd_per_mt']:.2f}/MT | "
        f"Landed INR {latest['total_landed_inr_per_mt']:.2f}/MT"
    )


if __name__ == "__main__":
    main()

# palm-price-tracker

Daily landed crude palm oil tracker for India in INR/MT.

The tracker uses publicly available data from:
- Malaysia crude palm oil futures historical data on Investing.com: https://www.investing.com/commodities/malaysian-crude-palm-oil-futures-historical-data
- USD/INR exchange rates from Frankfurter: https://api.frankfurter.app

## What it does

- pulls the latest daily Malaysia CPO futures closes
- converts each day into INR using the matching USD/INR rate
- applies a configurable landed-cost model for India
- writes a history file to `data/prices.csv`
- writes a plain-text summary to `output/weekly_summary.txt`
- refreshes automatically through GitHub Actions on weekdays

## Landed-cost model

The model is configurable in `config.json`.

```text
CFR USD/MT = Malaysia CPO price + freight
Insurance USD/MT = CFR * insurance_pct
CIF INR/MT = (CFR + insurance) * USDINR
Estimated landed INR/MT = CIF INR
                         + (CIF INR * effective_import_duty_pct)
                         + port_charges_inr_per_mt
                         + other_cost_inr_per_mt
```

Default assumptions are only planning assumptions. Update them before relying on the numbers for procurement, customs, or accounting. IGST is intentionally excluded from the default landed model because many users treat it separately.

## Files

- `tracker.py`: fetches prices, FX, and writes outputs
- `config.json`: landed-cost assumptions
- `data/prices.csv`: historical tracker output
- `output/weekly_summary.txt`: latest summary snapshot
- `.github/workflows/daily-update.yml`: weekday automation

## Local run

```bash
pip install -r requirements.txt
python tracker.py
```

## GitHub Actions schedule

The workflow is scheduled for weekdays and can also be run manually from the Actions tab.

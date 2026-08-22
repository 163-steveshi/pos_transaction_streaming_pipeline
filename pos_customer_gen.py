"""
generate_dim_customers.py

Generates ONE CSV file per execution mimicking a retail `dim_customers` staging
extract, for testing Bronze -> Silver SCD2 merge logic.

Rules implemented:
- Row count: random between 100 and 6000 per run.
- customer_id: ALWAYS well-formed as "CUST-#####" (5-digit, zero-padded). Never corrupted.
- All other fields: each field independently has a 10% chance of being "corrupted"
  (nulls, bad types, malformed dates/emails/phones, whitespace/case issues, etc).
- Duplicates are intentionally introduced (some exact dup rows, some same
  customer_id with different attribute values / different updated_at,
  simulating multiple source extracts of the same natural key) so the SCD2
  logic has to prove it dedupes / picks latest / detects changes correctly.

Run:
    python generate_dim_customers.py
    python generate_dim_customers.py --rows 2500 --seed 42 --out my_file.csv
"""

import argparse
import csv
import random
import string
import sys
from datetime import datetime, timedelta

from faker import Faker

fake = Faker()

CORRUPTION_RATE = 0.10  # 10% per-field chance
DUP_RATE = 0.08  # ~8% of rows get an extra duplicate/near-duplicate injected

LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
GENDERS = ["M", "F", "Other", "Unknown"]
COUNTRIES = ["USA", "Canada", "UK", "Germany", "Australia", "India"]


def maybe_corrupt(value, kind="text"):
    """Return either the clean value, or a corrupted version, per CORRUPTION_RATE.
    `kind` controls the *style* of corruption so it stays plausible for that field type.
    """
    if random.random() >= CORRUPTION_RATE:
        return value

    corruption_type = random.choice(
        ["null", "empty", "whitespace", "case", "truncate", "garbage", "wrong_type"]
    )

    if corruption_type == "null":
        return None
    if corruption_type == "empty":
        return ""
    if corruption_type == "whitespace":
        return f"   {value}   " if value is not None else value
    if corruption_type == "case":
        return str(value).upper() if random.random() < 0.5 else str(value).lower()
    if corruption_type == "truncate":
        s = str(value)
        cut = max(1, len(s) // 2)
        return s[:cut]
    if corruption_type == "garbage":
        if kind == "date":
            return random.choice(["0000-00-00", "31/02/2024", "not-a-date", "N/A"])
        if kind == "email":
            return random.choice(
                ["not-an-email", "missing_at_sign.com", "user@@bad..com"]
            )
        if kind == "phone":
            return "".join(random.choices(string.ascii_letters, k=6))
        if kind == "number":
            return "N/A"
        return "".join(random.choices(string.punctuation + string.ascii_letters, k=8))
    if corruption_type == "wrong_type":
        if kind == "number":
            return fake.word()
        if kind == "date":
            return str(fake.random_number(digits=8))
        return random.randint(0, 99999)

    return value


def random_updated_at(base_date=None):
    base_date = base_date or fake.date_time_between(start_date="-2y", end_date="now")
    return base_date


def make_customer_row(customer_id=None, base_updated_at=None):
    """Build one clean-shape row, then apply per-field corruption (except customer_id)."""
    if customer_id is None:
        customer_id = f"CUST-{fake.random_number(digits=5, fix_len=True)}"

    first_name = fake.first_name()
    last_name = fake.last_name()
    email = fake.email()
    phone = fake.phone_number()
    address = fake.street_address()
    city = fake.city()
    state = fake.state_abbr()
    zip_code = fake.zipcode()
    country = random.choice(COUNTRIES)
    birth_date = fake.date_of_birth(minimum_age=18, maximum_age=85).isoformat()
    gender = random.choice(GENDERS)
    registration_date = fake.date_time_between(start_date="-5y", end_date="-2y")
    loyalty_tier = random.choice(LOYALTY_TIERS)
    is_active = random.choice([True, False])
    updated_at = base_updated_at or random_updated_at()

    row = {
        "customer_id": customer_id,  # NEVER corrupted
        "first_name": maybe_corrupt(first_name, "text"),
        "last_name": maybe_corrupt(last_name, "text"),
        "email": maybe_corrupt(email, "email"),
        "phone": maybe_corrupt(phone, "phone"),
        "address": maybe_corrupt(address, "text"),
        "city": maybe_corrupt(city, "text"),
        "state": maybe_corrupt(state, "text"),
        "zip_code": maybe_corrupt(zip_code, "number"),
        "country": maybe_corrupt(country, "text"),
        "birth_date": maybe_corrupt(birth_date, "date"),
        "gender": maybe_corrupt(gender, "text"),
        "registration_date": maybe_corrupt(registration_date.isoformat(), "date"),
        "loyalty_tier": maybe_corrupt(loyalty_tier, "text"),
        "is_active": maybe_corrupt(is_active, "wrong_type"),
        "updated_at": maybe_corrupt(updated_at.isoformat(), "date"),
    }
    return row


def generate_dataset(num_rows, seed=None):
    if seed is not None:
        random.seed(seed)
        Faker.seed(seed)

    rows = []
    for _ in range(num_rows):
        rows.append(make_customer_row())

    # --- Inject duplicates / near-duplicates for SCD2 testing ---
    # Some are byte-for-byte duplicate rows (simulates re-sending the same extract).
    # Some share customer_id but have different attribute values and a later
    # updated_at (simulates a genuine change that SCD2 must capture as a new version).
    num_dupe_events = int(len(rows) * DUP_RATE)
    for _ in range(num_dupe_events):
        source_row = random.choice(rows)
        if random.random() < 0.5:
            # exact duplicate
            rows.append(dict(source_row))
        else:
            # changed version of the same customer_id, later timestamp
            try:
                prior_ts = datetime.fromisoformat(str(source_row["updated_at"]))
            except (ValueError, TypeError):
                prior_ts = datetime.now()
            new_ts = prior_ts + timedelta(days=random.randint(1, 400))
            changed_row = make_customer_row(
                customer_id=source_row["customer_id"], base_updated_at=new_ts
            )
            rows.append(changed_row)

    random.shuffle(rows)
    return rows


def write_csv(rows, out_path):
    fieldnames = [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "address",
        "city",
        "state",
        "zip_code",
        "country",
        "birth_date",
        "gender",
        "registration_date",
        "loyalty_tier",
        "is_active",
        "updated_at",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(description="Generate a dim_customers test CSV.")
    parser.add_argument(
        "--rows",
        type=int,
        default=None,
        help="Exact row count to generate. If omitted, a random count between 100 and 6000 is used.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional random seed for reproducibility.",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=None,
        help="Output CSV path. Defaults to dim_customers_<timestamp>_<rowcount>.csv",
    )
    args = parser.parse_args()

    num_rows = args.rows if args.rows is not None else random.randint(100, 6000)

    rows = generate_dataset(num_rows, seed=args.seed)

    if args.out:
        out_path = args.out
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = f"dim_customers_{ts}_{len(rows)}rows.csv"

    write_csv(rows, out_path)

    print(
        f"Generated {len(rows)} rows (base rows requested: {num_rows}, "
        f"extra dup/near-dup rows injected: {len(rows) - num_rows}) -> {out_path}"
    )


if __name__ == "__main__":
    main()

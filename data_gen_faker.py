"""
POS Bronze-Layer Record Generator (Faker edition)
--------------------------------------------------
Generates synthetic point-of-sale transaction line-items using the `faker`
library for realistic IDs, barcodes, timestamps, and locale-aware data.
25% of records (configurable) are corrupted to simulate real POS terminal /
network faults, for testing a streaming pipeline's validation logic.

Install: pip install faker
"""

import random
import string
from faker import Faker

fake = Faker()

# ----------------------------- reference data -----------------------------

# Faker's country_code() returns any of ~250 countries; we constrain to a
# realistic set of markets a retail chain would actually operate in.
COUNTRIES = ["US", "CA", "GB", "DE", "FR", "AU"]
CURRENCIES = {
    "US": "USD",
    "CA": "CAD",
    "GB": "GBP",
    "DE": "EUR",
    "FR": "EUR",
    "AU": "AUD",
}
CHANNELS = ["IN_STORE", "ONLINE", "KIOSK", "MOBILE_APP"]
PAYMENT_TYPES = ["CREDIT_CARD", "DEBIT_CARD", "CASH", "MOBILE_WALLET", "GIFT_CARD"]
STORE_IDS = [f"STORE-{i:03d}" for i in range(1, 21)]

# Pre-generate a fixed SKU catalog with Faker's EAN-13 barcode provider, so
# the same "products" recur across transactions (more realistic than a new
# random barcode every time).
SKU_POOL = [fake.ean13() for _ in range(200)]


def _iso(dt) -> str:
    return dt.isoformat(timespec="milliseconds") + "Z"


# ----------------------------- clean record -----------------------------


def generate_clean_record() -> dict:
    """Generate one valid, well-formed bronze-layer POS line-item event."""
    country = random.choice(COUNTRIES)
    unit_price = float(
        fake.pydecimal(
            left_digits=3, right_digits=2, positive=True, min_value=1, max_value=199
        )
    )
    quantity = fake.random_int(min=1, max=5)
    discount = round(unit_price * quantity * random.choice([0, 0, 0, 0.1, 0.2]), 2)
    tax_rate = random.choice([0.05, 0.07, 0.0825, 0.2])
    taxable = unit_price * quantity - discount
    tax_amount = round(max(taxable, 0) * tax_rate, 2)

    ts = fake.date_time_between(start_date="-7d", end_date="now")

    return {
        # bothify lets us template a realistic-looking transaction ID with
        # random digits, similar to what a real POS system generates
        "transaction_id": fake.bothify(text=f"TXN-{ts.strftime('%Y%m%d')}-######"),
        "item_sequence": fake.random_int(min=1, max=8),
        "customer_id": (
            f"CUST-{fake.random_number(digits=5, fix_len=True)}"
            if random.random() > 0.3
            else None  # guest checkout, legitimately null
        ),
        "event_time": _iso(ts),
        "sku": random.choice(SKU_POOL),  # Faker EAN-13 barcode
        "item_unit_price": unit_price,
        "item_quantity": quantity,
        "discount_amount": discount,
        "tax_amount": tax_amount,
        "payment_type": random.choice(PAYMENT_TYPES),
        "currency": CURRENCIES[country],
        "country": country,
        "channel": random.choice(CHANNELS),
        "store_id": random.choice(STORE_IDS),
        "terminal_id": fake.bothify(text="TERM-##"),
    }


# ----------------------------- noise injection -----------------------------
# Each function mutates a clean record to mimic a real POS failure mode.
# Faker is used here too, e.g. its `password`/random-character generators
# double nicely as "corrupted data" simulators.


def _noise_missing_field(r: dict) -> dict:
    """Random field silently dropped (e.g. payload truncated in transit)."""
    droppable = [k for k in r if k not in ("transaction_id", "event_time")]
    r.pop(random.choice(droppable), None)
    return r


def _noise_null_field(r: dict) -> dict:
    """A required field comes back null (e.g. terminal firmware bug)."""
    field = random.choice(
        ["item_unit_price", "item_quantity", "sku", "store_id", "payment_type"]
    )
    r[field] = None
    return r


def _noise_negative_value(r: dict) -> dict:
    """Negative price/quantity from a refund mis-flagged as a sale, or scale glitch."""
    field = random.choice(["item_unit_price", "item_quantity", "tax_amount"])
    r[field] = -abs(r[field]) if isinstance(r[field], (int, float)) and r[field] else -1
    return r


def _noise_extreme_outlier(r: dict) -> dict:
    """Barcode misread as a huge quantity/price (fat-finger or scanner double-read)."""
    field = random.choice(["item_quantity", "item_unit_price"])
    r[field] = r[field] * random.choice([1000, 9999]) if r[field] else 99999
    return r


def _noise_wrong_type(r: dict) -> dict:
    """Numeric field arrives as a string, e.g. '4.99' instead of 4.99 (serialization bug)."""
    field = random.choice(
        ["item_unit_price", "item_quantity", "tax_amount", "discount_amount"]
    )
    r[field] = str(r[field])
    return r


def _noise_malformed_event_time(r: dict) -> dict:
    """Clock desync / locale bug produces a garbled or wrong-format timestamp."""
    r["event_time"] = random.choice(
        [
            "2026-13-45T99:99:99Z",  # invalid date
            fake.date(pattern="%m/%d/%Y") + " 2:32 PM",  # wrong format (locale leak)
            "",  # blank
            "1970-01-01T00:00:00.000Z",  # epoch reset (terminal reboot)
        ]
    )
    return r


def _noise_unknown_enum(r: dict) -> dict:
    """Terminal firmware sends an undocumented/legacy code for an enum field."""
    field = random.choice(["payment_type", "channel", "country"])
    r[field] = random.choice(["UNKNOWN", "N/A", "LEGACY_CODE_7", "", "??"])
    return r


def _noise_garbled_string(r: dict) -> dict:
    """Encoding mismatch corrupts a text field (mojibake from a legacy register)."""
    field = random.choice(["customer_id", "sku", "store_id", "terminal_id"])
    # Faker's password generator with symbols doubles well as "garbage bytes"
    r[field] = (
        fake.password(
            length=8, special_chars=True, digits=True, upper_case=True, lower_case=False
        )
        + "\ufffd"
    )
    return r


def _noise_duplicate_transaction(r: dict, seen_ids: list) -> dict:
    """Retry logic on a flaky connection re-sends the same transaction_id."""
    if seen_ids:
        r["transaction_id"] = random.choice(seen_ids)
    return r


NOISE_FUNCTIONS = [
    _noise_missing_field,
    _noise_null_field,
    _noise_negative_value,
    _noise_extreme_outlier,
    _noise_wrong_type,
    _noise_malformed_event_time,
    _noise_unknown_enum,
    _noise_garbled_string,
]


# ----------------------------- public API -----------------------------


def generate_pos_record(
    seen_ids: list | None = None, noise_rate: float = 0.25
) -> tuple[dict, bool]:
    """
    Generate one POS record. With probability `noise_rate`, the record is
    corrupted using a randomly chosen realistic fault. Otherwise clean.

    `seen_ids`: running list of transaction_ids, needed so duplicate-
    transaction noise references a real prior ID. Pass the same list
    across calls to accumulate history.
    """
    record = generate_clean_record()
    is_noise = random.random() < noise_rate

    if is_noise:
        if seen_ids and random.random() < 0.15:
            record = _noise_duplicate_transaction(record, seen_ids)
        else:
            record = random.choice(NOISE_FUNCTIONS)(record)

    if seen_ids is not None:
        seen_ids.append(record.get("transaction_id"))
        if len(seen_ids) > 500:
            seen_ids.pop(0)

    # return is_noise for the debug
    return record, is_noise

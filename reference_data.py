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

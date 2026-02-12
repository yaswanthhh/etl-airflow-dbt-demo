import os, time, uuid, random
from datetime import datetime, timezone, timedelta

import pandas as pd
from faker import Faker

fake = Faker()

OUT_DIR = os.path.join("data", "incoming", "listings")
os.makedirs(OUT_DIR, exist_ok=True)

LOCATIONS = ["Lund", "Malmo", "Goteborg"]

MAKES_MODELS = {
    "Volvo": ["V60", "V90", "XC40", "XC60"],
    "Volkswagen": ["Golf", "Passat", "Tiguan"],
    "BMW": ["320i", "330e", "X3"],
    "Audi": ["A3", "A4", "Q3"],
    "Toyota": ["Corolla", "RAV4", "Yaris"],
}

def realistic_year():
    return random.randint(2010, 2024)

def realistic_price_sek(make, year):
    base = {
        "Volvo": 220_000,
        "Volkswagen": 160_000,
        "BMW": 210_000,
        "Audi": 200_000,
        "Toyota": 150_000,
    }.get(make, 170_000)
    age = max(0, 2026 - year)
    price = base - age * random.randint(8_000, 15_000) + random.randint(-15_000, 20_000)
    return max(25_000, int(round(price / 1000) * 1000))

def realistic_mileage(year):
    age = max(0, 2026 - year)
    km = age * random.randint(10_000, 22_000) + random.randint(0, 15_000)
    return max(0, int(round(km / 1000) * 1000))

def make_drop(n_rows: int):
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    rows = []
    for _ in range(n_rows):
        make = random.choice(list(MAKES_MODELS.keys()))
        model = random.choice(MAKES_MODELS[make])
        year = realistic_year()
        rows.append({
            "listing_id": f"{random.choice(['LUND','MALMO','GOT'])}-{uuid.uuid4().hex[:8].upper()}",
            "listing_ts": (now - timedelta(hours=random.randint(0, 2))).isoformat().replace("+00:00", "Z"),
            "location": random.choice(LOCATIONS),
            "make": make,
            "model": model,
            "vehicle_year": year,
            "price_sek": realistic_price_sek(make, year),
            "mileage_km": realistic_mileage(year),
        })
    return pd.DataFrame(rows)

if __name__ == "__main__":
    # Tweak these
    every_seconds = 300          # create a drop every 60s
    min_rows, max_rows = 1, 8   # each file has 1–8 rows

    print(f"Writing CSV drops into: {OUT_DIR}")
    while True:
        df = make_drop(random.randint(min_rows, max_rows))
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        fname = f"listings_drop_{ts}.csv"
        path = os.path.join(OUT_DIR, fname)
        df.to_csv(path, index=False)
        print("Wrote", path, "rows=", len(df))
        time.sleep(every_seconds)

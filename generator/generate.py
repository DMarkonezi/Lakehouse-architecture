#!/usr/bin/env python3
"""
Generator sintetickih podataka za online prodavnicu tehnike (samo standardna biblioteka).

    python generate.py init --seed 42 --scale small     # istorija (batch 0)
    python generate.py next                             # sledeca nedelja (normal_week)
    python generate.py next --payments-in-cents         # provajder posalje jedan dan u centima (x100)
    python generate.py fix-payments                     # provajder posalje ispravljen fajl (_v2)
    python generate.py next --app-release-3             # aplikacija 3.0 salje experiment{id, variant}
    python generate.py next --promotions                # izvor dobija kolonu discount_pct
    python generate.py next --black-friday              # petak u nedelji ima ~5x vise saobracaja

Sve je deterministicko: isti seed daje iste podatke, pa se svaki dan moze ponovo izracunati
(npr. kada se u sledecem batch-u menjaju statusi starih porudzbina).

Izlaz (koren: /data/sources u explorer kontejneru, odnosno sources/files na hostu):
    postgres/batch_XX/*.csv + load.sql   -> ucitava se u PostgreSQL (izvor 1)
    payments/payments_YYYY-MM-DD.csv     -> dnevni fajl platnog provajdera (izvor 2)
    fx_rates/fx_rates_YYYY-MM-DD.csv     -> kursna lista (izvor 3)
    clickstream/date=YYYY-MM-DD/events_HH.jsonl -> clickstream po satu (izvor 4)
"""
from __future__ import annotations

import argparse
import bisect
import csv
import itertools
import json
import math
import random
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from pathlib import Path

# ----------------------------------------------------------------------------- konfiguracija
SCALES = {
    #        kupci  proizvodi  dana istorije  porudzbina/dan  novih kupaca/nedelja  dana clickstream-a u istoriji
    "small": dict(customers=1_000, products=200, history_days=7, orders_per_day=215,
                  new_customers=50, click_history_days=7),
    "demo": dict(customers=20_000, products=2_000, history_days=91, orders_per_day=660,
                 new_customers=500, click_history_days=14),
}
HISTORY_END = date(2026, 9, 20)       # batch 0 = istorija do ovog dana; batch k = k-ta sledeca nedelja
CONVERSION_RATE = 0.03                # ~3% sesija zavrsi kupovinom
STATUS_LOOKBACK_DAYS = 21             # koliko unazad porudzbine jos mogu da promene status

COUNTRIES = [  # zemlja, valuta, udeo kupaca, gradovi
    ("RS", "RSD", 0.55, ["Beograd", "Novi Sad", "Nis", "Kragujevac", "Subotica", "Cacak"]),
    ("HR", "EUR", 0.15, ["Zagreb", "Split", "Rijeka", "Osijek"]),
    ("SI", "EUR", 0.10, ["Ljubljana", "Maribor", "Celje"]),
    ("ME", "EUR", 0.08, ["Podgorica", "Niksic", "Budva"]),
    ("AT", "EUR", 0.07, ["Wien", "Graz", "Linz"]),
    ("DE", "EUR", 0.05, ["Berlin", "Muenchen", "Hamburg"]),
]
CATEGORIES = {  # kategorija: (min EUR, max EUR, brendovi, udeo u katalogu, pojmovi za pretragu)
    "Laptops": (450, 2500, ["Lenovo", "Dell", "HP", "Asus", "Apple", "Acer"], 0.12, ["laptop", "ultrabook", "macbook"]),
    "Smartphones": (150, 1300, ["Samsung", "Apple", "Xiaomi", "Google", "Motorola"], 0.14, ["telefon", "iphone", "galaxy"]),
    "Audio": (20, 400, ["Sony", "JBL", "Bose", "Sennheiser", "Marshall"], 0.15, ["slusalice", "zvucnik", "anc"]),
    "Gaming": (30, 700, ["Sony", "Microsoft", "Nintendo", "Logitech", "Razer"], 0.13, ["ps5", "konzola", "gaming mis"]),
    "Home Appliances": (60, 1200, ["Bosch", "Gorenje", "Philips", "Tefal", "Beko"], 0.12, ["usisivac", "aparat za kafu", "frizider"]),
    "Smart Home": (15, 300, ["Philips Hue", "TP-Link", "Xiaomi", "Ring", "Nest"], 0.12, ["pametna sijalica", "kamera", "termostat"]),
    "Accessories": (5, 120, ["Anker", "Belkin", "Spigen", "Logitech", "Baseus"], 0.22, ["punjac", "kabl", "maska"]),
}
FIRST = ["Marko", "Jelena", "Nikola", "Ana", "Stefan", "Milica", "Luka", "Ivana", "Petar", "Teodora", "Aleksa",
         "Sara", "Vuk", "Mina", "Filip", "Katarina", "Lazar", "Tamara", "Ivan", "Maja", "Luka", "Eva", "David", "Lena"]
LAST = ["Petrovic", "Jovanovic", "Nikolic", "Markovic", "Ilic", "Stojanovic", "Pavlovic", "Djordjevic", "Kovac",
        "Popovic", "Horvat", "Novak", "Kranjc", "Lazic", "Todorovic", "Babic", "Mueller", "Gruber", "Vukovic"]
HOUR_WEIGHTS = [1, .6, .4, .3, .3, .4, .8, 1.5, 2.2, 2.6, 2.8, 3, 3.2, 3, 2.9, 3, 3.2, 3.6, 4.4, 5.2, 5.6, 5, 3.6, 2]
WEEKDAY_FACTOR = [0.9, 0.9, 0.95, 1.0, 1.1, 1.3, 1.25]          # pon..ned, jaci vikendi
PAYMENT_METHODS = (["card", "paypal", "cash_on_delivery", "bank_transfer"], [58, 17, 15, 10])
UTM = [None, None, None,
       {"source": "google", "medium": "cpc", "campaign": "brand"},
       {"source": "google", "medium": "organic", "campaign": None},
       {"source": "facebook", "medium": "paid_social", "campaign": "autumn_sale"},
       {"source": "newsletter", "medium": "email", "campaign": "weekly_deals"},
       {"source": "instagram", "medium": "social", "campaign": "creators"}]


def rng(*key) -> random.Random:
    """Nezavisan, deterministican generator za svaki entitet/dan (zavisi samo od seed-a i kljuca)."""
    return random.Random(":".join(str(k) for k in (STATE.seed, *key)))


def money(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    cents = abs(cents)
    return f"{sign}{cents // 100}.{cents % 100:02d}"


def ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# ----------------------------------------------------------------------------- stanje (state.json)
@dataclass
class Batch:
    no: int
    start: str                 # prvi dan (ISO)
    end: str                   # poslednji dan (ISO)
    cents_day: str | None = None
    app_release_3: bool = False
    promotions: bool = False
    boost_day: str | None = None
    boost_factor: float = 1.0

    @property
    def start_d(self) -> date: return date.fromisoformat(self.start)
    @property
    def end_d(self) -> date: return date.fromisoformat(self.end)
    @property
    def load_ts(self) -> datetime:          # trenutak kada se batch "ucitava" u OLTP (kraj poslednjeg dana)
        return datetime.combine(self.end_d, time(23, 59, 59))


@dataclass
class State:
    seed: int = 42
    scale: str = "small"
    batches: list[Batch] = field(default_factory=list)

    @property
    def cfg(self) -> dict: return SCALES[self.scale]

    def batch_for(self, d: date) -> Batch | None:
        for b in self.batches:
            if b.start_d <= d <= b.end_d:
                return b
        return None


STATE = State()


def state_path(root: Path) -> Path:
    return root / "state.json"


def load_state(root: Path) -> None:
    global STATE
    raw = json.loads(state_path(root).read_text())
    STATE = State(seed=raw["seed"], scale=raw["scale"], batches=[Batch(**b) for b in raw["batches"]])


def save_state(root: Path) -> None:
    state_path(root).write_text(json.dumps(
        {"seed": STATE.seed, "scale": STATE.scale, "batches": [b.__dict__ for b in STATE.batches]}, indent=2))


# ----------------------------------------------------------------------------- referentni podaci
@lru_cache(maxsize=None)
def fx_rsd_per_eur(d: date) -> float:
    base = 117.15 + 0.08 * math.sin(d.toordinal() / 9)
    return round(base + rng("fx", d).gauss(0, 0.03), 4)


@lru_cache(maxsize=None)
def product(pid: int) -> dict:
    r = rng("p", pid)
    cats = list(CATEGORIES)
    cat = r.choices(cats, [CATEGORIES[c][3] for c in cats])[0]
    lo, hi, brands, _, _ = CATEGORIES[cat]
    brand = r.choice(brands)
    price = int(math.exp(r.uniform(math.log(lo), math.log(hi))))
    return {
        "product_id": pid, "category": cat, "brand": brand,
        "sku": f"{cat[:3].upper()}-{pid:05d}",
        "name": f"{brand} {r.choice(['Pro', 'Air', 'Max', 'Lite', 'Plus', 'X', 'S'])} {r.randint(10, 990)}",
        "base_price_cents": price * 100 + 99,
        "popularity": r.paretovariate(1.2),
    }


@lru_cache(maxsize=None)
def price_changes(batch_no: int) -> dict[int, float]:
    """Promene cena koje se primenjuju na kraju batch-a (1% proizvoda, -15% .. +12%)."""
    if batch_no == 0:
        return {}
    r = rng("price-change", batch_no)
    n = STATE.cfg["products"]
    return {pid: round(r.uniform(0.85, 1.12), 3) for pid in r.sample(range(1, n + 1), max(1, n // 100))}


def price_cents_at(pid: int, at: datetime) -> int:
    cents = product(pid)["base_price_cents"]
    for b in STATE.batches:
        if b.no >= 1 and b.load_ts <= at and pid in price_changes(b.no):
            cents = int(cents * price_changes(b.no)[pid]) // 100 * 100 + 99
    return cents


@lru_cache(maxsize=None)
def product_sampler() -> tuple[list[int], list[float]]:
    ids = list(range(1, STATE.cfg["products"] + 1))
    return ids, list(itertools.accumulate(product(p)["popularity"] for p in ids))


def new_customer_range(batch_no: int) -> range:
    cfg = STATE.cfg
    if batch_no == 0:
        return range(1, cfg["customers"] + 1)
    first = cfg["customers"] + (batch_no - 1) * cfg["new_customers"] + 1
    return range(first, first + cfg["new_customers"])


def customer_created_at(cid: int) -> datetime:
    r = rng("c-created", cid)
    if cid <= STATE.cfg["customers"]:
        start = datetime(2024, 1, 1)
        hist_start = HISTORY_END - timedelta(days=STATE.cfg["history_days"])
        return start + timedelta(seconds=r.randint(0, int((datetime.combine(hist_start, time()) - start).total_seconds())))
    batch_no = (cid - STATE.cfg["customers"] - 1) // STATE.cfg["new_customers"] + 1
    b = next(b for b in STATE.batches if b.no == batch_no)
    return datetime.combine(b.start_d, time()) + timedelta(seconds=r.randint(0, 20 * 3600))


@lru_cache(maxsize=None)
def customer(cid: int) -> dict:
    r = rng("c", cid)
    country, currency, _, cities = r.choices(COUNTRIES, [c[2] for c in COUNTRIES])[0]
    first, last = r.choice(FIRST), r.choice(LAST)
    return {
        "customer_id": cid, "first_name": first, "last_name": last,
        "email": f"{first}.{last}{cid}@example.com".lower(),
        "country": country, "currency": currency, "city": r.choice(cities),
        "customer_type": "b2c",
        "loyalty_tier": r.choices(["bronze", "silver", "gold"], [80, 15, 5])[0],
        "activity": min(r.paretovariate(1.3), 10.0),   # dugi rep, ali bez kupca sa stotinama porudzbina
    }


@lru_cache(maxsize=None)
def customer_sampler(n_available: int) -> tuple[list[int], list[float]]:
    ids = list(range(1, n_available + 1))
    return ids, list(itertools.accumulate(customer(c)["activity"] for c in ids))


def customers_available(d: date) -> int:
    """Kupci koji postoje pre pocetka dana d (novi kupci kupuju od drugog dana svoje nedelje)."""
    n = STATE.cfg["customers"]
    for b in STATE.batches:
        if b.no >= 1 and b.start_d < d:
            n += STATE.cfg["new_customers"]
    return n


def pick(r: random.Random, sampler: tuple[list[int], list[float]]) -> int:
    ids, cum = sampler
    return ids[bisect.bisect(cum, r.random() * cum[-1])]


# ----------------------------------------------------------------------------- porudzbine
@dataclass
class Order:
    order_id: int
    customer_id: int
    order_ts: datetime
    channel: str
    country: str
    currency: str
    method: str
    items: list[dict]
    timeline: list[tuple[str, datetime]]        # (status, od kada)
    chargeback_ts: datetime | None

    @property
    def total_cents(self) -> int:
        return sum(i["net_cents"] for i in self.items)

    def status_at(self, at: datetime) -> tuple[str, datetime] | None:
        current = None
        for status, since in self.timeline:
            if since <= at:
                current = (status, since)
        return current


def orders_per_day(d: date) -> int:
    b = STATE.batch_for(d)
    f = WEEKDAY_FACTOR[d.weekday()] * (1.25 if 10 <= d.day <= 12 else 1.0)    # dani oko plata
    if b and b.boost_day == d.isoformat():
        f *= b.boost_factor
    return round(STATE.cfg["orders_per_day"] * f * rng("n-orders", d).uniform(0.9, 1.1))


@lru_cache(maxsize=None)
def orders_for_day(d: date) -> list[Order]:
    r = rng("o", d)
    b = STATE.batch_for(d)
    promotions = bool(b and b.promotions)
    rsd = fx_rsd_per_eur(d)
    csampler = customer_sampler(customers_available(d))
    psampler = product_sampler()
    out = []
    for i in range(1, orders_per_day(d) + 1):
        hour = r.choices(range(24), HOUR_WEIGHTS)[0]
        order_ts = datetime.combine(d, time(hour, r.randint(0, 59), r.randint(0, 59)))
        c = customer(pick(r, csampler))
        order_id = int(f"{d:%y%m%d}{i:05d}")
        items = []
        for line in range(1, r.choices([1, 2, 3, 4, 5], [45, 28, 15, 8, 4])[0] + 1):
            pid = pick(r, psampler)
            qty = r.choices([1, 2, 3], [85, 12, 3] if product(pid)["category"] != "Accessories" else [60, 30, 10])[0]
            eur_cents = price_cents_at(pid, order_ts)
            unit = (math.ceil(eur_cents / 100 * rsd / 10) * 10 - 1) * 100 if c["currency"] == "RSD" else eur_cents
            pct = r.choice([5, 10, 15, 20]) if r.random() < 0.15 else 0
            discount = qty * unit * pct // 100
            items.append({"order_item_id": order_id * 10 + line, "product_id": pid, "quantity": qty,
                          "unit_cents": unit, "discount_cents": discount, "net_cents": qty * unit - discount,
                          "discount_pct": pct if promotions else None})
        method = r.choices(*PAYMENT_METHODS)[0]
        timeline = [("pending", order_ts)]
        chargeback = None
        if r.random() < 0.03:
            timeline.append(("cancelled", order_ts + timedelta(minutes=r.randint(30, 360))))
        else:
            paid = order_ts + timedelta(minutes=r.randint(2, 20))
            shipped = paid + timedelta(hours=r.randint(18, 40))
            delivered = shipped + timedelta(hours=r.randint(24, 72))
            timeline += [("paid", paid), ("shipped", shipped), ("delivered", delivered)]
            if r.random() < 0.04:
                timeline.append(("returned", delivered + timedelta(days=r.randint(2, 6))))
            elif r.random() < 0.003:
                chargeback = delivered + timedelta(days=r.randint(5, 10))
        out.append(Order(order_id, c["customer_id"], order_ts, "app" if r.random() < 0.45 else "web",
                         c["country"], c["currency"], method, items, timeline, chargeback))
    return out


def daterange(a: date, b: date):
    for n in range((b - a).days + 1):
        yield a + timedelta(days=n)


def payment_events(o: Order) -> list[dict]:
    """Dogadjaji platnog provajdera za porudzbinu: naplata, povracaj, chargeback."""
    tl = dict(o.timeline)
    if "paid" not in tl:
        return []
    captured_at = tl["delivered"] if o.method == "cash_on_delivery" else tl["paid"] + timedelta(seconds=30)
    ev = [("1", "captured", captured_at)]
    if "returned" in tl:
        ev.append(("2", "refunded", tl["returned"] + timedelta(hours=6)))
    if o.chargeback_ts:
        ev.append(("3", "chargeback", o.chargeback_ts))
    return [{"payment_id": f"PAY{o.order_id}{seq}", "order_id": o.order_id, "amount_cents": o.total_cents,
             "currency": o.currency, "method": o.method, "status": st, "processed_at": at} for seq, st, at in ev]


# ----------------------------------------------------------------------------- clickstream
def device_for(r: random.Random, channel: str, app3: bool) -> dict:
    if channel == "app":
        os_ = r.choice(["Android", "iOS"])
        return {"type": "mobile", "os": os_, "app_version": "3.0.1" if app3 else "2.9.4"}
    if r.random() < 0.4:
        return {"type": "mobile", "os": r.choice(["Android", "iOS"]), "app_version": None}
    return {"type": "desktop", "os": r.choice(["Windows", "macOS", "Linux"]), "app_version": None}


def session_events(r, d, sess_no, start, customer_id, channel, app3, products, order=None) -> list[dict]:
    device = device_for(r, channel, app3)
    utm = r.choice(UTM)
    session_id = f"S{d:%y%m%d}{sess_no:06d}"
    events, t, prev_url = [], start, (f"https://www.{utm['source']}.com/" if utm else None)
    variant = r.choice(["A", "B"]) if app3 and channel == "app" else None   # varijanta se dodeljuje po sesiji

    def add(event_type, url, product_id=None, search_query=None, order_id=None):
        nonlocal t, prev_url
        e = {"event_id": f"{d:%Y%m%d}{sess_no:06d}{len(events):02d}", "session_id": session_id,
             "customer_id": customer_id, "event_ts": t.strftime("%Y-%m-%dT%H:%M:%SZ"), "event_type": event_type}
        for k, v in (("product_id", product_id), ("search_query", search_query), ("order_id", order_id)):
            if v is not None:                    # klijent ne salje prazna polja
                e[k] = v
        e.update({"page": {"url": url, "referrer": prev_url}, "device": device, "utm": utm})
        if variant:
            e["experiment"] = {"id": "checkout_v2", "variant": variant}
        events.append(e)
        prev_url = url
        t += timedelta(seconds=r.randint(5, 120))

    add("page_view", "/")
    if r.random() < 0.35:
        cat = product(products[0])["category"] if products else r.choice(list(CATEGORIES))
        q = r.choice(CATEGORIES[cat][4])
        add("search", f"/search?q={q.replace(' ', '+')}", search_query=q)
    if order:
        for p in products:
            add("product_view", f"/p/{product(p)['sku']}", product_id=p)
            add("add_to_cart", "/cart", product_id=p)
        add("begin_checkout", "/checkout")
        t = max(t, order.order_ts)
        add("purchase", "/order/confirmation", order_id=order.order_id)
    elif products:
        for p in products:
            add("product_view", f"/p/{product(p)['sku']}", product_id=p)
        if r.random() < 0.15:
            add("add_to_cart", "/cart", product_id=products[0])
            if r.random() < 0.35:
                add("begin_checkout", "/checkout")
    return events


def clickstream_for_day(d: date) -> list[dict]:
    r = rng("cs", d)
    b = STATE.batch_for(d)
    app3 = bool(b and b.app_release_3)
    orders = orders_for_day(d)
    psampler = product_sampler()
    csampler = customer_sampler(customers_available(d))
    events, sess = [], 0
    for o in orders:                                   # sesije koje su se zavrsile kupovinom
        sess += 1
        start = o.order_ts - timedelta(minutes=r.randint(3, 25))
        events += session_events(r, d, sess, start, o.customer_id, o.channel, app3,
                                 [i["product_id"] for i in o.items], order=o)
    for _ in range(round(len(orders) * (1 / CONVERSION_RATE - 1))):   # sesije bez kupovine
        sess += 1
        start = datetime.combine(d, time(r.choices(range(24), HOUR_WEIGHTS)[0], r.randint(0, 59), r.randint(0, 59)))
        cust = pick(r, csampler) if r.random() < 0.35 else None
        prods = [pick(r, psampler) for _ in range(r.choices([0, 1, 2, 3], [30, 40, 20, 10])[0])]
        events += session_events(r, d, sess, start, cust, "app" if r.random() < 0.4 else "web", app3, prods)

    # realni problemi u podacima: neispravno vreme, dogadjaj bez tipa, ponovo poslat dogadjaj
    bad = rng("cs-bad", d)
    for e in events:
        x = bad.random()
        if x < 0.0015:
            e["event_ts"] = "N/A"
        elif x < 0.0025:
            del e["event_type"]
    events += [dict(e) for e in bad.sample(events, len(events) // 500)]
    return events


# ----------------------------------------------------------------------------- pisanje izlaza
def write_csv(path: Path, header: list[str], rows) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in rows:
            w.writerow(row)
            n += 1
    return n


def write_batch(root: Path, b: Batch) -> None:
    cfg = STATE.cfg
    pg = root / "postgres" / f"batch_{b.no:02d}"
    window = list(daterange(b.start_d, b.end_d))
    T = b.load_ts
    prev_T = next((x.load_ts for x in STATE.batches if x.no == b.no - 1), None)
    stats = {}

    # --- izvor 1: OLTP (nove porudzbine sa statusom u trenutku ucitavanja + promene starih)
    cust_cols = ["customer_id", "first_name", "last_name", "email", "country", "city", "customer_type",
                 "loyalty_tier", "created_at", "updated_at"]
    stats["customers"] = write_csv(pg / "customers.csv", cust_cols, (
        [customer(c)[k] for k in cust_cols[:8]] + [ts(customer_created_at(c))] * 2
        for c in new_customer_range(b.no)))
    if b.no == 0:
        stats["products"] = write_csv(
            pg / "products.csv",
            ["product_id", "sku", "name", "category", "brand", "list_price_eur", "created_at", "updated_at"],
            ([p, product(p)["sku"], product(p)["name"], product(p)["category"], product(p)["brand"],
              money(product(p)["base_price_cents"]), "2024-01-01 00:00:00", "2024-01-01 00:00:00"]
             for p in range(1, cfg["products"] + 1)))
    else:
        stats["product_updates"] = write_csv(
            pg / "product_updates.csv", ["product_id", "list_price_eur", "updated_at"],
            ([p, money(price_cents_at(p, T)), ts(T)] for p in price_changes(b.no)))

    orders = [o for d in window for o in orders_for_day(d)]
    order_rows, item_rows = [], []
    for o in orders:
        status, since = o.status_at(T)
        order_rows.append([o.order_id, o.customer_id, ts(o.order_ts), o.channel, o.country, o.currency, status, ts(since)])
        for i in o.items:
            row = [i["order_item_id"], o.order_id, i["product_id"], i["quantity"], money(i["unit_cents"]),
                   money(i["discount_cents"]), status, ts(since)]
            if b.promotions:
                row.append("" if i["discount_pct"] is None else f"{i['discount_pct']:.2f}")
            item_rows.append(row)
    stats["orders"] = write_csv(pg / "orders.csv", ["order_id", "customer_id", "order_ts", "sales_channel",
                                                    "country", "currency", "status", "updated_at"], order_rows)
    item_cols = ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount", "item_status",
                 "updated_at"] + (["discount_pct"] if b.promotions else [])
    stats["order_items"] = write_csv(pg / "order_items.csv", item_cols, item_rows)

    updates = []
    if prev_T:
        for d in daterange(b.start_d - timedelta(days=STATUS_LOOKBACK_DAYS), b.start_d - timedelta(days=1)):
            if STATE.batch_for(d) is None:
                continue
            for o in orders_for_day(d):
                old, new = o.status_at(prev_T), o.status_at(T)
                if old[0] != new[0]:
                    updates.append([o.order_id, new[0], ts(new[1])])
    stats["order_updates"] = write_csv(pg / "order_updates.csv", ["order_id", "status", "updated_at"], updates)

    sql = ["\\set ON_ERROR_STOP on", "BEGIN;"]
    copy = lambda table, cols, f: (f"COPY {table} ({', '.join(cols)}) FROM '/sources/postgres/batch_{b.no:02d}/{f}' "
                                   f"WITH (FORMAT csv, HEADER true, NULL '');")
    sql.append(copy("customers", cust_cols, "customers.csv"))
    if b.no == 0:
        sql.append(copy("products", ["product_id", "sku", "name", "category", "brand", "list_price_eur",
                                     "created_at", "updated_at"], "products.csv"))
    else:
        sql += ["CREATE TEMP TABLE product_updates (product_id INT, list_price_eur NUMERIC(10,2), updated_at TIMESTAMP);",
                copy("product_updates", ["product_id", "list_price_eur", "updated_at"], "product_updates.csv"),
                "UPDATE products p SET list_price_eur = u.list_price_eur, updated_at = u.updated_at "
                "FROM product_updates u WHERE p.product_id = u.product_id;"]
    if b.promotions:
        sql.append("ALTER TABLE order_items ADD COLUMN IF NOT EXISTS discount_pct NUMERIC(5,2);")
    sql += [copy("orders", ["order_id", "customer_id", "order_ts", "sales_channel", "country", "currency", "status",
                            "updated_at"], "orders.csv"),
            copy("order_items", item_cols, "order_items.csv"),
            "CREATE TEMP TABLE order_updates (order_id BIGINT, status TEXT, updated_at TIMESTAMP);",
            copy("order_updates", ["order_id", "status", "updated_at"], "order_updates.csv"),
            "UPDATE orders o SET status = u.status, updated_at = u.updated_at FROM order_updates u WHERE o.order_id = u.order_id;",
            "UPDATE order_items i SET item_status = u.status, updated_at = u.updated_at FROM order_updates u WHERE i.order_id = u.order_id;",
            # program lojalnosti: kupci sa vise porudzbina prelaze u visi nivo (SCD2 u dim_customer)
            "WITH c AS (SELECT customer_id, count(*) AS n FROM orders WHERE status <> 'cancelled' GROUP BY customer_id) "
            "UPDATE customers SET loyalty_tier = CASE WHEN c.n >= 6 THEN 'gold' ELSE 'silver' END, "
            f"updated_at = TIMESTAMP '{ts(T)}' FROM c WHERE customers.customer_id = c.customer_id AND "
            "((c.n >= 6 AND customers.loyalty_tier <> 'gold') OR (c.n BETWEEN 3 AND 5 AND customers.loyalty_tier = 'bronze'));",
            "COMMIT;"]
    (pg / "load.sql").write_text("\n".join(sql) + "\n", encoding="utf-8")

    # --- izvor 2: platni provajder, jedan CSV po danu obrade
    by_day: dict[date, list[dict]] = {d: [] for d in window}
    for d in daterange(b.start_d - timedelta(days=STATUS_LOOKBACK_DAYS), b.end_d):
        if STATE.batch_for(d) is None:
            continue
        for o in orders_for_day(d):
            for p in payment_events(o):
                if p["processed_at"].date() in by_day:
                    by_day[p["processed_at"].date()].append(p)
    stats["payments"] = sum(write_payments(root, d, rows, cents=(b.cents_day == d.isoformat()))
                            for d, rows in by_day.items())

    # --- izvor 3: kursna lista
    for d in window:
        write_csv(root / "fx_rates" / f"fx_rates_{d}.csv", ["rate_date", "currency", "rate_to_eur"],
                  [[d.isoformat(), "EUR", "1.00000000"], [d.isoformat(), "RSD", f"{1 / fx_rsd_per_eur(d):.8f}"]])

    # --- izvor 4: clickstream, JSON Lines po satu (namerno mnogo malih fajlova)
    click_days = window if b.no > 0 else window[-cfg["click_history_days"]:]
    n_events = 0
    for d in click_days:
        events = clickstream_for_day(d)
        n_events += len(events)
        by_hour: dict[int, list[str]] = {}
        for e in events:
            hour = int(e["event_ts"][11:13]) if e["event_ts"] != "N/A" else 0
            by_hour.setdefault(hour, []).append(json.dumps(e, ensure_ascii=False))
        out = root / "clickstream" / f"date={d}"
        out.mkdir(parents=True, exist_ok=True)
        for hour, lines in sorted(by_hour.items()):
            (out / f"events_{hour:02d}.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    stats["clickstream_events"] = n_events

    flags = [k for k in ("cents_day", "app_release_3", "promotions", "boost_day") if getattr(b, k)]
    print(f"batch {b.no:02d}  {b.start} .. {b.end}  {' '.join(f'{k}={v}' for k, v in stats.items())}"
          + (f"  [{', '.join(flags)}]" if flags else ""))


def write_payments(root: Path, d: date, rows: list[dict], cents: bool = False, suffix: str = "") -> int:
    r = rng("pay-dup", d)
    rows = sorted(rows, key=lambda p: p["processed_at"])
    rows += r.sample(rows, len(rows) // 200)          # provajder ponekad posalje isti red dva puta
    factor = 100 if cents else 1                       # greska: iznosi u centima/parama umesto u jedinicama
    return write_csv(root / "payments" / f"payments_{d}{suffix}.csv",
                     ["payment_id", "order_id", "amount", "currency", "method", "status", "processed_at"],
                     ([p["payment_id"], p["order_id"], money(p["amount_cents"] * factor), p["currency"], p["method"],
                       p["status"], ts(p["processed_at"])] for p in rows))


# ----------------------------------------------------------------------------- komande
def default_root() -> Path:
    for p in (Path("/data/sources"), Path("/home/iceberg/sources")):
        if p.exists():
            return p
    return Path(__file__).resolve().parent.parent / "sources" / "files"


def cmd_init(args, root: Path) -> None:
    global STATE
    if state_path(root).exists() and not args.force:
        raise SystemExit(f"{state_path(root)} vec postoji (reset.ps1 brise sve, ili --force)")
    STATE = State(seed=args.seed, scale=args.scale)
    days = SCALES[args.scale]["history_days"]
    b = Batch(no=0, start=(HISTORY_END - timedelta(days=days - 1)).isoformat(), end=HISTORY_END.isoformat())
    STATE.batches.append(b)
    write_batch(root, b)
    save_state(root)


def cmd_next(args, root: Path) -> None:
    load_state(root)
    last = STATE.batches[-1]
    start = last.end_d + timedelta(days=1)
    b = Batch(no=last.no + 1, start=start.isoformat(), end=(start + timedelta(days=6)).isoformat(),
              app_release_3=args.app_release_3 or last.app_release_3,       # jednom pusteno, ostaje
              promotions=args.promotions or last.promotions)
    if args.payments_in_cents:
        b.cents_day = (start + timedelta(days=2)).isoformat()
    if args.black_friday:
        b.boost_day = next(d for d in daterange(start, start + timedelta(days=6)) if d.weekday() == 4).isoformat()
        b.boost_factor = 5.0
    STATE.batches.append(b)
    save_state(root)          # pre pisanja, jer porudzbine zavise od zastavica batch-a
    write_batch(root, b)


def cmd_fix_payments(args, root: Path) -> None:
    load_state(root)
    b = next((x for x in reversed(STATE.batches) if x.cents_day), None)
    if not b:
        raise SystemExit("nema batch-a sa pogresnim iznosima")
    d = date.fromisoformat(b.cents_day)
    rows = [p for dd in daterange(d - timedelta(days=STATUS_LOOKBACK_DAYS), d) if STATE.batch_for(dd)
            for o in orders_for_day(dd) for p in payment_events(o) if p["processed_at"].date() == d]
    n = write_payments(root, d, rows, cents=False, suffix="_v2")
    print(f"ispravljen fajl payments_{d}_v2.csv ({n} redova)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=None)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("init")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--scale", choices=SCALES, default="small")
    p.add_argument("--force", action="store_true")
    p = sub.add_parser("next")
    p.add_argument("--payments-in-cents", action="store_true")
    p.add_argument("--app-release-3", action="store_true")
    p.add_argument("--promotions", action="store_true")
    p.add_argument("--black-friday", action="store_true")
    sub.add_parser("fix-payments")
    args = ap.parse_args()
    root = args.out or default_root()
    root.mkdir(parents=True, exist_ok=True)
    {"init": cmd_init, "next": cmd_next, "fix-payments": cmd_fix_payments}[args.cmd](args, root)


if __name__ == "__main__":
    main()

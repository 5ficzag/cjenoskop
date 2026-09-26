"""
Cjenoskop: dnevna obrada podataka s cijene.dev.

Preuzima dnevne ZIP arhive (danas + tjedne točke za zadnjih mjesec dana
+ kvartalne točke za zadnjih godinu dana), računa sažetke i zapisuje
male JSON datoteke koje aplikacija učitava:

  meta.json            datum, lanci, gradovi, zadana košarica
  products.json        usporedivi proizvodi (EAN) za pretragu
  prices/<grad>.json   najniža cijena po lancu u tom gradu
  prices/hr.json       tipična (medijan) cijena po lancu za cijelu Hrvatsku
  history.json         medijan cijene po EAN-u kroz vrijeme (osobna inflacija)
  deals.json           trenutne akcije s ocjenom je li popust stvaran

Pokretanje:  python pipeline/build.py --out site/data
Lokalni test: python pipeline/build.py --out out --local-dir ./zips
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
import re
import shutil
import sys
import tempfile
import time
import unicodedata
import zipfile
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

LIST_URL = "https://api.cijene.dev/v0/list"
UA = {"User-Agent": "cjenoskop-pipeline/1.0 (+https://github.com)"}

# Weekly points for the discount detector, quarterly points for inflation.
OFFSETS = [0, 7, 14, 21, 28, 91, 182, 273, 365]
REF_OFFSETS = [14, 21, 28]  # "usual price" window, before any current promo

CHAIN_NAMES = {
    "konzum": "Konzum", "lidl": "Lidl", "spar": "Spar", "kaufland": "Kaufland",
    "plodine": "Plodine", "tommy": "Tommy", "studenac": "Studenac",
    "eurospin": "Eurospin", "dm": "dm", "ktc": "KTC", "metro": "Metro",
    "trgocentar": "Trgocentar", "zabac": "Žabac", "vrutak": "Vrutak",
    "ribola": "Ribola", "ntl": "NTL", "roto": "Roto", "boso": "Boso",
    "brodokomerc": "Brodokomerc", "lorenco": "Lorenco", "gavranovic": "Gavranović",
    "branka": "Branka", "bure": "Bure", "dukat": "Dukat", "stanic": "Stanić",
    "stridon": "Stridon", "djelo_vodice": "Djelo Vodice",
    "jadranka_trgovina": "Jadranka", "trgovina_krk": "Trgovina Krk",
}

# Default basket: keyword rules -> most widely sold matching product.
DEFAULT_BASKET = [
    ("mlijeko", r"\bmlijeko\b", r"\b(1 ?l|1l|1,0 ?l)\b|^1(\.0+)? ?l$", 3),
    ("kruh", r"\bkruh\b", None, 2),
    ("jaja", r"\bjaja\b", r"10", 1),
    ("maslac", r"\bmaslac\b", None, 1),
    ("jogurt", r"\bjogurt\b", None, 2),
    ("kava", r"\bkava\b", None, 1),
    ("šećer", r"\bšećer\b", None, 1),
    ("brašno", r"\bbrašno\b", None, 1),
    ("ulje", r"\bulje\b.*\bsuncokret|\bsuncokretovo ulje\b", None, 1),
    ("tjestenina", r"\bšpageti\b|\bspaghetti\b", None, 1),
    ("toaletni papir", r"\btoaletni papir\b", None, 1),
    ("čokolada", r"\bčokolada\b", None, 1),
]

MIN_STORES_PRODUCT = 15      # keep EANs sold in at least this many stores...
MIN_CHAINS_PRODUCT = 2       # ...or in at least this many chains
MIN_STORES_CITY = 6          # cities with at least this many stores get a file
MAX_DEALS = 450
PRICE_MIN, PRICE_MAX = 0.05, 2000.0

EAN_RE = re.compile(r"^\d{8,14}$")


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def slugify(s: str) -> str:
    s = s.replace("đ", "d").replace("Đ", "D")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def fnum(x) -> float | None:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    return round(float(x), 2)


# ---------------------------------------------------------------- archives

def list_archives(local_dir: Path | None) -> dict[date, str]:
    if local_dir:
        out = {}
        for p in local_dir.glob("*.zip"):
            try:
                out[date.fromisoformat(p.stem)] = str(p)
            except ValueError:
                pass
        return out
    r = requests.get(LIST_URL, headers=UA, timeout=60)
    r.raise_for_status()
    return {date.fromisoformat(a["date"]): a["url"] for a in r.json()["archives"]}


def pick_dates(available: dict[date, str]) -> dict[int, date]:
    latest = max(available)
    picked = {}
    for off in OFFSETS:
        target = latest - timedelta(days=off)
        cands = [d for d in available if abs((d - target).days) <= 4]
        if cands:
            picked[off] = min(cands, key=lambda d: (abs((d - target).days), -d.toordinal()))
    return picked


def fetch(src: str, tmp: Path) -> Path:
    if not src.startswith("http"):
        return Path(src)
    dest = tmp / src.rsplit("/", 1)[-1]
    for attempt in range(4):
        try:
            with requests.get(src, headers=UA, stream=True, timeout=300) as r:
                r.raise_for_status()
                with open(dest, "wb") as f:
                    shutil.copyfileobj(r.raw, f, length=1 << 20)
            return dest
        except Exception as e:  # noqa: BLE001
            log(f"  download failed ({e}), retry {attempt + 1}")
            time.sleep(10 * (attempt + 1))
    raise RuntimeError(f"could not download {src}")


def read_chain_tables(zf: zipfile.ZipFile):
    """Yield (chain, stores_df, products_df, prices_df) for every chain in the archive."""
    by_chain = defaultdict(dict)
    for name in zf.namelist():
        parts = name.strip("/").split("/")
        if len(parts) >= 2 and parts[-1] in ("stores.csv", "products.csv", "prices.csv"):
            by_chain[parts[-2].lower()][parts[-1]] = name
    for chain, files in sorted(by_chain.items()):
        if not {"stores.csv", "products.csv", "prices.csv"} <= files.keys():
            continue
        try:
            with zf.open(files["stores.csv"]) as f:
                stores = pd.read_csv(f, dtype=str, keep_default_na=False)
            with zf.open(files["products.csv"]) as f:
                products = pd.read_csv(f, dtype=str, keep_default_na=False,
                                       usecols=lambda c: c in {"product_id", "barcode", "name", "brand", "unit", "quantity"})
            with zf.open(files["prices.csv"]) as f:
                prices = pd.read_csv(
                    f, dtype={"store_id": str, "product_id": str},
                    usecols=lambda c: c in {"store_id", "product_id", "price", "best_price_30", "anchor_price", "special_price"},
                )
        except Exception as e:  # noqa: BLE001
            log(f"  skip {chain}: {e}")
            continue
        yield chain, stores, products, prices


def prepare(chain, stores, products, prices):
    """Join prices with EANs and cities; compute the effective (paid) price."""
    for col in ("price", "best_price_30", "anchor_price", "special_price"):
        if col not in prices:
            prices[col] = float("nan")
        prices[col] = pd.to_numeric(prices[col], errors="coerce")
    products = products.drop_duplicates("product_id")
    products = products[products["barcode"].str.match(EAN_RE)]
    df = prices.merge(products[["product_id", "barcode"]], on="product_id", how="inner")
    sp = df["special_price"]
    promo = sp.notna() & (sp > 0) & (sp < df["price"])
    df["eff"] = df["price"].where(~promo, sp)
    df["promo"] = promo
    df = df[(df["eff"] >= PRICE_MIN) & (df["eff"] <= PRICE_MAX)]
    city = stores.drop_duplicates("store_id").set_index("store_id")["city"] if "city" in stores else pd.Series(dtype=str)
    df["city"] = df["store_id"].map(city).fillna("")
    df["chain"] = chain
    return df, products


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--local-dir")
    args = ap.parse_args()
    out = Path(args.out)
    (out / "prices").mkdir(parents=True, exist_ok=True)

    available = list_archives(Path(args.local_dir) if args.local_dir else None)
    picked = pick_dates(available)
    today = picked[0]
    log("dates:", {k: str(v) for k, v in picked.items()})
    tmp = Path(tempfile.mkdtemp())

    # snapshot results
    hist_nat = {}          # off -> {ean: national median eff}
    chain_eff = {}         # off -> {(chain, ean): median eff}
    chain_reg = {}         # off -> {(chain, ean): median regular price}
    # today-only
    names = {}             # ean -> Counter of (name, brand, qty) weighted by stores
    ean_stores = Counter()
    ean_chains = defaultdict(set)
    city_parts = []        # DataFrames: city, barcode, chain, eff (min per city)
    nat_chain_med = {}     # (chain, ean) -> median eff
    deals_raw = []
    store_cities = Counter()
    chains_seen = set()

    # Process newest first so a failure on an old archive still leaves today's data.
    for off in sorted(picked):
        d = picked[off]
        log(f"archive {d} (offset {off})")
        path = fetch(available[d], tmp)
        per_ean = defaultdict(list)
        ce, cr = {}, {}
        try:
            zf = zipfile.ZipFile(path)
        except zipfile.BadZipFile:
            log("  bad zip, skipped")
            continue
        with zf:
            for chain, stores, products, prices in read_chain_tables(zf):
                df, prods = prepare(chain, stores, products, prices)
                if df.empty:
                    continue
                g = df.groupby("barcode")
                med = g["eff"].median()
                ce.update({(chain, e): v for e, v in med.items()})
                cr.update({(chain, e): v for e, v in g["price"].median().items()})
                # national: one value per chain (its median) so big chains don't dominate
                for e, v in med.items():
                    per_ean[e].append(v)

                if off == 0:
                    chains_seen.add(chain)
                    n_st = g["store_id"].nunique()
                    for e, n in n_st.items():
                        ean_stores[e] += int(n)
                        ean_chains[e].add(chain)
                    nat_chain_med.update({(chain, e): v for e, v in med.items()})
                    info = prods.set_index("barcode")[["name", "brand", "quantity", "unit"]]
                    info = info[~info.index.duplicated()]
                    for e, row in info.iterrows():
                        if e in n_st.index:
                            names.setdefault(e, Counter())[(row["name"].strip(), row["brand"].strip(), row["quantity"].strip(), row["unit"].strip())] += int(n_st[e])
                    for c, n in stores.drop_duplicates("store_id")["city"].value_counts().items():
                        if c:
                            store_cities[c] += int(n)
                    cm = df[df["city"] != ""].groupby(["city", "barcode"], as_index=False)["eff"].min()
                    cm["chain"] = chain
                    city_parts.append(cm)
                    # current promotions in this chain
                    pr = df[df["promo"]]
                    if not pr.empty:
                        pg = pr.groupby("barcode")
                        agg = pd.DataFrame({
                            "special": pg["special_price"].median(),
                            "price": pg["price"].median(),
                            "best30": pg["best_price_30"].median(),
                            "anchor": pg["anchor_price"].median(),
                            "stores": pg["store_id"].nunique(),
                        })
                        agg["chain_stores"] = df["store_id"].nunique()
                        for e, row in agg.iterrows():
                            deals_raw.append((chain, e, row))
        chain_eff[off], chain_reg[off] = ce, cr
        hist_nat[off] = {e: float(pd.Series(v).median()) for e, v in per_ean.items()}
        if path.parent == tmp:
            path.unlink(missing_ok=True)
        log(f"  eans={len(hist_nat[off])}")

    # ---------------- products
    keep = {e for e in names if ean_stores[e] >= MIN_STORES_PRODUCT or len(ean_chains[e]) >= MIN_CHAINS_PRODUCT}
    products = []
    for e in keep:
        (n, b, q, u), _ = names[e].most_common(1)[0]
        qty = f"{q} {u}".strip() if q else u
        qty = re.sub(r"\.0+\b", "", qty)
        products.append({"e": e, "n": n, "b": b, "q": qty, "c": len(ean_chains[e]), "s": ean_stores[e]})
    products.sort(key=lambda p: -p["s"])
    chains = sorted(chains_seen, key=lambda c: -sum(1 for e in keep if c in ean_chains[e]))
    cidx = {c: i for i, c in enumerate(chains)}
    log(f"products kept: {len(products)}, chains: {chains}")

    def price_file(getter):
        p = {}
        for e in keep:
            row = [getter(c, e) for c in chains]
            if sum(v is not None for v in row):
                p[e] = row
        return p

    # national typical price per chain
    hr = price_file(lambda c, e: fnum(nat_chain_med.get((c, e))))
    write(out / "prices" / "hr.json", {"chains": chains, "p": hr})

    cities = [c for c, n in store_cities.most_common() if n >= MIN_STORES_CITY]
    allc = pd.concat(city_parts, ignore_index=True) if city_parts else pd.DataFrame(columns=["city", "barcode", "eff", "chain"])
    allc = allc[allc["city"].isin(cities) & allc["barcode"].isin(keep)]
    city_meta = []
    for c, sub in allc.groupby("city"):
        m = {(ch, e): v for ch, e, v in zip(sub["chain"], sub["barcode"], sub["eff"])}
        p = price_file(lambda ch, e, m=m: fnum(m.get((ch, e))))
        if len(p) < 200:
            continue
        slug = slugify(c)
        write(out / "prices" / f"{slug}.json", {"chains": chains, "p": p})
        present = [ch for ch in chains if any(v[cidx[ch]] is not None for v in p.values())]
        city_meta.append({"id": slug, "name": c, "stores": store_cities[c], "chains": present})
    city_meta.sort(key=lambda x: x["name"])
    log(f"cities: {len(city_meta)}")

    # ---------------- history (national, per snapshot)
    offs = sorted(picked, reverse=True)  # oldest first
    hist = {}
    for e in keep:
        row = [fnum(hist_nat.get(o, {}).get(e)) for o in offs]
        if sum(v is not None for v in row) >= 2:
            hist[e] = row
    write(out / "history.json", {"dates": [str(picked[o]) for o in offs], "p": hist})

    # ---------------- deals
    week_offs = [o for o in (28, 21, 14, 7, 0) if o in chain_eff]
    deals = []
    for chain, e, row in deals_raw:
        if e not in keep:
            continue
        special, price = float(row["special"]), float(row["price"])
        if not (special > 0 and price > 0) or special >= price * 0.97:
            continue
        min_stores = max(1, int(row["chain_stores"] * 0.1))
        if row["stores"] < min(min_stores, 5):
            continue
        refs = [chain_eff[o].get((chain, e)) for o in REF_OFFSETS if o in chain_eff]
        refs = [r for r in refs if r is not None and not math.isnan(r)]
        ref_src = "weeks"
        if len(refs) >= 2:
            ref = float(pd.Series(refs).median())
        elif not math.isnan(row["best30"]) and row["best30"] > 0:
            ref, ref_src = float(row["best30"]), "best30"
        else:
            continue
        claimed = 1 - special / price
        real = 1 - special / ref
        if real >= 0.10:
            verdict = "good"
        elif claimed - real >= 0.10 and price > ref * 1.05:
            verdict = "bad"
        else:
            verdict = "warn"
        (n, b, q, u), _ = names[e].most_common(1)[0]
        qty = re.sub(r"\.0+\b", "", f"{q} {u}".strip())
        deals.append({
            "e": e, "n": n, "b": b, "q": qty, "ch": chain,
            "sp": fnum(special), "pr": fnum(price), "ref": fnum(ref), "src": ref_src,
            "b30": fnum(row["best30"]), "anc": fnum(row["anchor"]),
            "cl": round(claimed, 3), "re": round(real, 3), "v": verdict,
            "st": int(row["stores"]),
            "w": [fnum(chain_eff[o].get((chain, e))) for o in week_offs],
            "wr": [fnum(chain_reg[o].get((chain, e))) for o in week_offs],
        })
    # most interesting first: popular products, big claimed discounts
    deals.sort(key=lambda d: -(ean_stores[d["e"]] ** 0.5) * (0.2 + d["cl"]))
    deals = deals[:MAX_DEALS]
    write(out / "deals.json", {"weeks": [str(picked[o]) for o in week_offs], "deals": deals})
    log(f"deals: {len(deals)} ({Counter(d['v'] for d in deals)})")

    # ---------------- default basket
    by_name = sorted(products, key=lambda p: -p["s"])
    basket = []
    for label, pat, qpat, qty in DEFAULT_BASKET:
        rx = re.compile(pat, re.I)
        qrx = re.compile(qpat, re.I) if qpat else None
        for p in by_name:
            if p["c"] >= 3 and rx.search(p["n"]) and (not qrx or qrx.search(p["q"]) or qrx.search(p["n"])):
                if p["e"] not in {b[0] for b in basket}:
                    basket.append([p["e"], qty])
                    break
    write(out / "meta.json", {
        "date": str(today),
        "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "chains": [{"id": c, "name": CHAIN_NAMES.get(c, c.title())} for c in chains],
        "cities": city_meta,
        "basket": basket,
        "counts": {"products": len(products), "deals": len(deals), "stores": sum(store_cities.values())},
        "source": "https://cijene.dev",
    })
    write(out / "products.json", products)
    shutil.rmtree(tmp, ignore_errors=True)
    log("done")


def write(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    log(f"  wrote {path} ({path.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    main()

"""Make small fake archives in the cijene.dev format to test the pipeline offline."""
import csv, io, random, zipfile, sys
from datetime import date, timedelta
out = sys.argv[1]
random.seed(1)
chains = {"konzum": 1.05, "lidl": .94, "spar": 1.03, "studenac": 1.07}
cities = ["Zagreb", "Čakovec", "Split", "Varaždin"]
names = [("Svježe mlijeko 2,8% m.m.", "Dukat", "1", "L"), ("Kruh polubijeli", "Klara", "600", "g"), ("Jaja M", "Farma", "10", "kom"),
         ("Maslac", "Vindija", "250", "g"), ("Jogurt tekući", "Dukat", "1", "kg"), ("Mljevena kava", "Franck", "250", "g"),
         ("Šećer kristal", "Viro", "1", "kg"), ("Brašno pšenično T-550", "Podravka", "1", "kg"), ("Suncokretovo ulje", "Zvijezda", "1", "L"),
         ("Špageti", "Barilla", "500", "g"), ("Toaletni papir 3-slojni", "Violeta", "10", "kom"), ("Mliječna čokolada", "Kraš", "100", "g")]
names += [(f"Proizvod {i}", "Marka", "1", "kom") for i in range(300)]
base = {f"38500{i:08d}": 1 + random.random() * 8 for i in range(len(names))}
eans = list(base)
latest = date(2026, 9, 26)
for off in [0, 7, 14, 21, 28, 91, 182, 273, 365, 3]:
    d = latest - timedelta(days=off)
    buf = io.BytesIO(); z = zipfile.ZipFile(buf, "w")
    for ch, m in chains.items():
        st = io.StringIO(); w = csv.writer(st); w.writerow(["store_id", "type", "address", "city", "zipcode"])
        stores = [(f"{ch[:2]}{i}", cities[i % 4]) for i in range(12)]
        for s, c in stores: w.writerow([s, "supermarket", "Ulica 1", c, "10000"])
        z.writestr(f"{ch}/stores.csv", st.getvalue())
        pr = io.StringIO(); w = csv.writer(pr); w.writerow(["product_id", "barcode", "name", "brand", "category", "unit", "quantity"])
        for i, e in enumerate(eans): n, b, q, u = names[i]; w.writerow([f"p{i}", e, n, b, "x", u, q])
        z.writestr(f"{ch}/products.csv", pr.getvalue())
        pc = io.StringIO(); w = csv.writer(pc); w.writerow(["store_id", "product_id", "price", "unit_price", "best_price_30", "anchor_price", "special_price"])
        for i, e in enumerate(eans):
            p = round(base[e] * m * (1 - off / 365 * 0.06), 2)
            promo = ""
            if off == 0 and i % 9 == 0: promo = round(p * 0.75, 2)
            if i % 9 == 3 and off in (0, 7, 3): p = round(p * 1.3, 2); promo = round(p / 1.3, 2) if off == 0 else ""
            for s, c in stores: w.writerow([s, f"p{i}", p, "", round(p*0.97,2), round(p*0.95,2), promo])
        z.writestr(f"{ch}/prices.csv", pc.getvalue())
    z.close(); open(f"{out}/{d}.zip", "wb").write(buf.getvalue())

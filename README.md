# Cjenoskop

Gdje je tvoja košarica najjeftinija, koliko je poskupjela i je li popust stvaran.
Stvarne cijene hrvatskih trgovačkih lanaca, osvježene svaki dan.

## Kako radi

1. GitHub Actions (`.github/workflows/update.yml`) svaki dan u 11:20 (po hrvatskom ljetnom vremenu) pokreće `pipeline/build.py`.
2. Skripta preuzima dnevne arhive cjenika s [cijene.dev](https://cijene.dev): današnju, tjedne točke za zadnjih mjesec dana i tromjesečne točke za zadnju godinu.
3. Iz njih računa male JSON datoteke (`data/`): proizvode, cijene po gradu i lancu, povijest cijena i ocijenjene akcije.
4. Stranica (`index.html`) i podaci objavljuju se na GitHub Pages. Najnoviji podaci su i u grani `data`.

Ručno pokretanje: Actions → *Dnevno osvježavanje cijena* → *Run workflow*.

## Lokalno

```bash
pip install -r pipeline/requirements.txt
python pipeline/build.py --out data     # preuzima ~700 MB arhiva
python -m http.server                   # pa otvori http://localhost:8000
```

Za test bez interneta: `python test/make_fake.py ./zips && python pipeline/build.py --out data --local-dir ./zips`.

## Ocjena popusta

- **Pravi popust**: akcijska cijena je barem 10 % ispod medijana cijene koju je isti lanac tražio prije 2, 3 i 4 tjedna.
- **Napuhan popust**: oglašeni popust je barem 10 postotnih bodova veći od stvarnog, a redovna cijena je podignuta više od 5 % iznad uobičajene.
- **Kozmetički popust**: sve ostalo, uglavnom stvarna ušteda ispod 10 %.

Cijene usluga (frizeri, servisi…) zasad su primjer. Obveza objave tih cjenika počinje 1. listopada 2026. (NN 101/2026).

Podaci: javni cjenici prema Odluci NN 75/2025 i NN 101/2026, obrada [cijene.dev](https://cijene.dev).

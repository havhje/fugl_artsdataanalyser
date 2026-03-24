# API Usage, Spatial Data, and Domain Concepts

## NorTaxa API

- Base URL: `https://nortaxa.artsdatabanken.no/api/v1/TaxonName`
- Apply rate limiting (`time.sleep()`) between API calls
- Use `@lru_cache` for caching repeated API lookups
- Always set `timeout=10` on `requests.get()` calls

## Error Handling

- Use try/except around HTTP requests with specific exception types
- Print errors with context: `print(f"Error fetching ID {id}: {e}")`
- Use `mo.stop(condition, message)` to halt notebook execution with a UI message
- Return `None` from functions that may fail (API calls)

## SQL (DuckDB via mo.sql)

- Use f-strings to inject table names into SQL queries
- DuckDB spatial extension: `INSTALL spatial; LOAD spatial;`
- Coordinate system is UTM Zone 33N (EPSG:25833) throughout
- Use `ST_GeomFromText()`, `ST_Intersects()`, `ST_Read()` for spatial ops

## Domain Concepts

- **Artsdatabanken** -- Norwegian Biodiversity Information Centre
- **NorTaxa** -- Norwegian taxonomy database/API
- **IUCN categories** -- CR, EN, VU, NT, LC (conservation status)
- **M1941** -- Norwegian valuation method for species importance
- **Arter av nasjonal forvaltningsinteresse** -- Species of national management interest
- **Hovedokosystem** -- Main ecosystem map service (Miljodirektoratet)
- Coordinate system: **UTM Zone 33N (EPSG:25833)** for all spatial data

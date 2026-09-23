# NorTaxa, navneidentitet og kontrakten til Fuglefilter

**Undersøkt 22. september 2026; offentlige hoveduttak UTC 12:49–12:52, avsluttende kildehashkontroll 12:59.** Dette er faktagrunnlag for beslutningsrunden, ikke en implementeringsbestilling. Ingen produksjonskode, observasjonsdata, kartprosjekter eller installasjoner er endret. Bare dette notatet er opprettet. Ingen forekomster er hentet eller lastet opp; nettverkskallene gjelder offentlige dokumenter og takson-/navnemetadata.

## Kort konklusjon

- Live NorTaxa **og** Artskarts takson-API gir forskjellige navne-ID-er til `Limosa limosa` (**3767**), `L. limosa subsp. islandica` (**3768**) og `L. limosa subsp. limosa` (**3769**). Takson-ID-ene er henholdsvis **3708, 3709 og 3710**. Underartene har foreldrearten 3767/3708; de har ikke samme egen navne-ID som arten.
- `databehandling.py` bevarer kildens `validScientificNameId` som **`Artens ID`**. Det separate hjelpefeltet **`ArtNavnId`** blir **3767 for alle tre**, brukes til fuglegruppering og eksporteres **ikke**. Her finnes faktisk en felles arts-ID, men den må ikke forveksles med eksport-ID-en.
- Vitenskapelig navn og rang hentes **ikke på nytt** fra NorTaxa til sluttabellen: `validScientificName → Art`, `scientificNameRank → Taksonomisk nivå`. Norske navn kommer fra `preferredPopularName → Navn`, eventuelt manuell utfylling av manglende **norsk** navn. Verken denne utfyllingen eller NorTaxa-berikingen retter kildens kombinasjon av navn, ID og rang.
- `(Artens ID, Art)` bevarer kildeparene og er allerede analysefunksjonens grupperingsnøkkel. Den kan følge preprocessing uten nye API-oppslag, synonymløsing eller sammenfolding til foreldreart. Den er derimot **ikke bevis for antall biologisk forskjellige taksa** dersom kildepar inneholder synonymer, eldre navn eller motstridende ID/navn. Kravet om å bevare separate rapportgrupper ved samme ID og ulike navn står ved lag.
- Preprocessing garanterer **ikke utfylt navne-ID (`Artens ID`) eller vitenskapelig navn på hver rad**. Å innføre obligatoriske ikke-null taksonnøkler i pluginen ville være et nytt valg, ikke bare å følge eksisterende databehandling.

## 1. Undersøkt arbeidskopi og sporbarhet

Lest: repoenes `AGENTS.md` og `README.md`, `CONTEXT.md`, `docs/qgis-pipeline-plan.md`, kanonisk `~/qgis/qgis-naturmangfold/qt6/environment/references/bird-data-workflow.md`, relevante produksjonsfunksjoner og innebygde tester i `databehandling/databehandling.py`, samt analysegrupperingen og GIS-klargjøringen. Forsknings- og grilling-instruksene er fulgt som avgrenset faktainnhenting for morsesjonen; ingen ny delegering eller brukerbeslutning er gjort.

| Kilde | Revisjon / SHA-256 ved kontroll |
|---|---|
| Fuglerepo HEAD | `d53ac6d9a49404db16da240a7ecb2964f91941a0` |
| GIS-repo HEAD | `fb6019757187bc36040336a0768c5b6493f0c06b` |
| `databehandling/databehandling.py` | `983d043b777cf75bf605bd7333a3d4e370bcc254234246f378d2de8f6685cb32` |
| `dataanalyse/data_analyse.py` | `fef8cabcc48953b283ded101ae82631e685823243e61e7c9a503e8b89c7f0870` |
| `databehandling/fugl_atributt_data` | `690ba9d490e509a62a39290135539ee90dae9a630172a8208c71045fdecc7afa` |
| GIS `scripts/prepare_bird_data.py` | `1998e4dd63c4b8d30770482df8dbbf2f83e2394ccf449fe9f56f49bf7031939d` |
| GIS `references/bird-data-workflow.md` | `c2c50867c8cfc0cf40283c9dda03b754787f2f34894964319b3452c185edcdbc` |

Begge arbeidskopier hadde allerede endringer. HEAD alene identifiserer derfor ikke alt undersøkt innhold. Linjehenvisningene nedenfor gjelder disse arbeidskopiene. Attributtdatabasen ble bare åpnet med `read_only=True`; ingen lokal observasjonsfil ble lest.

## 2. API-fakta: fire forskjellige ID-begreper

### Førstepartsskjema og felter

**NorTaxa:** [Swagger UI](https://nortaxa.artsdatabanken.no/swagger/index.html) og [maskinlesbart Swagger 2.0-skjema](https://nortaxa.artsdatabanken.no/swagger/v1/swagger.json). Skjemaets `info.title` er `Nortaxa`, versjon `1.0`, kontakt Artsdatabanken. Definisjonene ligger under `definitions`, ikke `components.schemas`.

- `GET /api/v1/TaxonName/ByScientificNameId/{id}`: beskrivelsen er **«Get taxon data by scientificNameId»**, svaret er `NamesInfoDto`.
- `GET /api/v1/TaxonName/ByTaxonId/{id}`: **«Get taxon data by taxonId»**, samme svartype, men et annet ID-domene.
- `NamesInfoDto` har `scientificNameId`, `taxonId`, `existsInNorway`, `scientificNames`, `vernacularNames`, `higherClassification`. **Det har ikke feltet `validScientificNameId`.**
- `ScientificNameDto` har navnepostens `id`, `taxonId`, `taxonomicStatus`, `scientificName`, `scientificNamePresentation`, `scientificNameAuthorship`, `taxonRank` med mer. `scientificName` er i disse arts-/underartssvarene bare epitetet (`limosa`/`islandica`); det fulle navnet står i `scientificNamePresentation`.
- `HigherClassification` har både `scientificNameId` og `taxonId`, i tillegg til navn og `taxonRank`. I de kontrollerte svarene omfatter listen også det forespurte nivået selv, ikke bare overordnede nivåer.
- `Children/ByScientificNameId/{id}` returnerer `ChildTaxonNamesDto`, med **både** `parentTaxonNameId`, `taxonNameId`, `taxonId` og `scientificNameId`. De er ikke utbyttbare felt. For Limosa er foreldrenavnenoden `3331`, foreldreartens vitenskapelige navne-ID `3767`, og foreldreartens takson-ID `3708`.
- ID-feltene ovenfor er deklarert som `integer/int32`. Skjemaet gir i liten grad forklarende felttekst og har ikke en `required`-liste i de aktuelle DTO-ene. Dette gir ikke grunnlag for å innføre nye null-/positivitetsregler for lokal Parquet.
- Navne-/taksonoppslag har valgfri `date`: **«Default date is now. Date format yyyy-mm-dd»**. Den lokale funksjonen sender ikke dato; beriking bruker altså oppslagstidspunktets svar, mens kilde-ID/navn/rang bevares.

**Artskart:** [offisiell API-beskrivelse](https://artsdatabanken.no/Pages/195884) lenker til [Public API Swagger](https://artskart.artsdatabanken.no/publicapi/swagger). Dagens [maskinlesbare skjema](https://artskart.artsdatabanken.no/publicapi/swagger/docs/v1), `definitions.TaxonInfo`, beskriver:

```text
TaxonId:                  "Integer taxonid"
ParentTaxonId:            "Parent Taxon id - Artsnavnebase"
ValidScientificNameId:    "ScientificNameId of then valid name for the taxon"
ValidScientificName:      "The accepted valid scientific name"
ScientificNames:          "A list of alternative scientific names"
ScientificNameIdHiarchy:  "A hierarchy of pointers to parent scientific names ids"
TaxonIdHiarchy:           "A hierarchy of pointers to parent taxon ids"
```

`TaxonInfo` beskrives som Artskarts lokale, nattlig synkroniserte taksonomikopi. Public API bruker PascalCase her; lokal CSV/pipeline bruker camelCase. Dette er taksonmetadata, ikke et nytt kontrollgrunnlag for alle mulige observasjons-CSV-er. Rangen heter `CategoryValue` i dette Artskart-skjemaet og `taxonRank` i NorTaxa; lokal `scientificNameRank` er et kildefelt som koden videresender.

Artsdatabankens [eldre faglige API-forklaring](https://artsdatabanken.no/Pages/180034/) sier uttrykkelig: **«names and taxa are distinct types of objects»**. Den siden brukes bare til begrepsforklaring; eldre ruter/eksempler brukes ikke som dokumentasjon for dagens NorTaxa-endepunkter.

### Live Limosa-data

Alle følgende navn har `taxonomicStatus: "Accepted"` og `existsInNorway: true` i NorTaxa-svarene:

| Fullt navn fra API | Rang | Vitenskapelig navne-ID | Takson-ID | `ParentTaxonId` i Artskart | Norsk navn i live metadata |
|---|---|---:|---:|---:|---|
| `Limosa limosa` | Species | 3767 | 3708 | 3704 (slekten Limosa) | svarthalespove |
| `Limosa limosa subsp. islandica` | Subspecies | 3768 | 3709 | 3708 | Ingen; NorTaxa `vernacularNames: []`, Artskart `PrefferedPopularname: null` |
| `Limosa limosa subsp. limosa` | Subspecies | 3769 | 3710 | 3708 | Ingen; samme tom/null-situasjon |

Direkte navneposter: [3767](https://nortaxa.artsdatabanken.no/api/v1/TaxonName/ByScientificNameId/3767), [3768](https://nortaxa.artsdatabanken.no/api/v1/TaxonName/ByScientificNameId/3768), [3769](https://nortaxa.artsdatabanken.no/api/v1/TaxonName/ByScientificNameId/3769). Tilhørende Artskart-taksonposter: [3708](https://artskart.artsdatabanken.no/publicapi/api/taxon/3708), [3709](https://artskart.artsdatabanken.no/publicapi/api/taxon/3709), [3710](https://artskart.artsdatabanken.no/publicapi/api/taxon/3710).

Det offisielle [artsoppslaget for svarthalespove](https://artsdatabanken.no/arter/takson/3708) bekrefter også «Vitenskapelig navn ID: 3767» og «Takson ID: 3708» og lenker til [NorTaxa navneside 3767](https://nortaxa.artsdatabanken.no/name-info/3767).

Eksakt offentlig utdrag fra `3768` (felter valgt fra det større svaret):

```json
{
  "scientificNameId": 3768,
  "taxonId": 3709,
  "scientificNames": [{
    "id": 3768,
    "taxonId": 3709,
    "taxonomicStatus": "Accepted",
    "scientificName": "islandica",
    "scientificNamePresentation": "Limosa limosa subsp. islandica",
    "taxonRank": "Subspecies"
  }],
  "vernacularNames": []
}
```

Samme svars `higherClassification` har `Species/scientificNameId=3767/taxonId=3708`, deretter `Subspecies/scientificNameId=3768/taxonId=3709`. Begge underarter har dessuten `Family=3716` (Scolopacidae) og `Order=161320` (Charadriiformes) målt som **navne-ID-er**.

[Barn av 3767](https://nortaxa.artsdatabanken.no/api/v1/TaxonName/Children/ByScientificNameId/3767) var nøyaktig disse to underartene. Utdrag fra barnesvaret:

```json
[
  {"parentTaxonNameId":3331,"taxonNameId":3332,"taxonId":3709,"scientificNameId":3768,"presentationName":"Limosa limosa subsp. islandica","rank":"Subspecies"},
  {"parentTaxonNameId":3331,"taxonNameId":3333,"taxonId":3710,"scientificNameId":3769,"presentationName":"Limosa limosa subsp. limosa","rank":"Subspecies"}
]
```

[Barn av slekten 3763](https://nortaxa.artsdatabanken.no/api/v1/TaxonName/Children/ByScientificNameId/3763) kobler artens `taxonNameId=3331` til `scientificNameId=3767` og `taxonId=3708`. [Oppslag med takson-ID 3709](https://nortaxa.artsdatabanken.no/api/v1/TaxonName/ByTaxonId/3709) ga identisk JSON-kropp som navneoppslag 3768.

Dette dokumenterer dagens registrerte norske taksonomitre, ikke at alle verdens underarter er representert. Det dokumenterer heller ikke historikken til en bestemt gammel eksport. Brukerens krav om separate arts-/underartsgrupper skal derfor ikke avvises fordi det konkrete live-eksemplet har separate egen-ID-er.

### Kontroll med eksisterende Anser-test

[Anser albifrons 3468](https://nortaxa.artsdatabanken.no/api/v1/TaxonName/ByScientificNameId/3468): takson-ID **3433**, Species. [Underart 3469](https://nortaxa.artsdatabanken.no/api/v1/TaxonName/ByScientificNameId/3469): **Anser albifrons subsp. albifrons**, takson-ID **3434**, Subspecies; hierarkiet har foreldreart **3468**, familie **3424**, orden **253**. Dette samsvarer med de lokale MTM-006/007-testene, se `databehandling.py:702–737`.

### Synonymeksempel: navn-ID er ikke takson-ID, og svaret er ikke en automatisk retting

Live [295741](https://nortaxa.artsdatabanken.no/api/v1/TaxonName/ByScientificNameId/295741) og [3853](https://nortaxa.artsdatabanken.no/api/v1/TaxonName/ByScientificNameId/3853) returnerer samme `taxonId=223139`, med to navneposter:

```json
[
  {"id":295741,"taxonId":223139,"taxonomicStatus":"Accepted","scientificNamePresentation":"Astur gentilis "},
  {"id":3853,"taxonId":223139,"taxonomicStatus":"Synonym","scientificNamePresentation":"Accipiter gentilis"}
]
```

Det avsluttende mellomrommet i `"Astur gentilis "` er faktisk i live-svaret. Ved oppslag på **3853** er toppfeltet `scientificNameId` fortsatt **3853**, og hierarkiets Species/Genus følger `Accipiter gentilis`/`Accipiter`. Man kan derfor ikke generelt kalle toppfeltet «gjeldende accepted-ID» eller anta at et navneoppslag automatisk returnerer bare nåværende navn/hierarki. Status må skilles fra forespurt ID.

Lokal `ANF_MTM_005` bruker uttrykkelig **295741 sammen med `Accipiter gentilis`**, og krever at original-ID beholdes (`databehandling.py:1709–1733`). Det er et eksisterende eksempel på tolerert kildepar som ikke er navnepostens nåværende ID/navn-kombinasjon. **Samme eksport-ID med forskjellige tekster kan derfor ikke alene bevise separate biologiske taksa.** Det kan være kildevariasjon, synonym-/tidsforskjell eller feil kombinasjon. Å bevare separate kildegrupper er likevel mulig uten å avgjøre årsaken.

## 3. Den faktiske lokale dataflyten

Alle linjer i denne seksjonen gjelder `databehandling/databehandling.py`.

| Trinn | Faktisk logikk | Kilde |
|---|---|---|
| Inngang | Krever kolonnene `validScientificNameId`, `validScientificName`, `preferredPopularName`, `scientificNameRank` m.fl. `scientificNameId` og `TaxonId` er ikke påkrevd og brukes ikke som identitetskilde. CSV leses med `SELECT *`; ekstra felter kan følge mellomsteg, men sluttutvalget bestemmer eksporten. | 62–147, 3808–3812 |
| Observasjonsidentitet | `proxyId` blir `obs_id`. Krever utfylt, ikke-blank, unik verdi; dette er **ikke** regelen for takson-ID-er. | 87–96 |
| Årfilter | Gamle og null-daterte observasjoner fjernes før beriking. Tomt resultat får tomt sluttskjema uten API-kall. | 3814–3865 |
| NorTaxa-oppslag | Kall per unik kilde-`validScientificNameId`, via `ByScientificNameId`. Tilfører høyere taksonomi og norske familie-/ordennavn. Leser ikke `scientificNames` for å overskrive navn, ID eller rang. | 159–244, 281–435 |
| Foreldrehjelper | `ArtNavnId` kommer fra elementet med `taxonRank == "Species"` i `higherClassification`. Underartens eget Subspecies-element endrer ikke dette. Familie-/ordenhjelperne er også **scientificNameId**, ikke taxonId. | 195–224, 350–357 |
| Fuglegruppe | Bruker Aves og arts-/underartsnivå; rang trimmes/småbokstavkonverteres bare i uttrykket. Første regel er art → familie → orden. Underarter bruker `ArtNavnId`. Mangler nødvendig overordnet nøkkel, brukes ikke en usikker grovere regel; ellers `Ikke gruppert`. Original-ID/navn/rang bevares. | 839–918 |
| Rødlistebasert verdi | Leser kildens `category`; ingen nytt taksonoppslag eller omklassifisering av artsidentitet. | 1223–1248 |
| ANF/Mdir | To venstrejoins: først kilde-`validScientificNameId` mot `vitenskapelig_navn_id`, deretter eksakt kilde-`validScientificName` mot `vitenskapelig_navn`. `coalesce` prioriterer ID-verdier per resultatfelt, så navnefallback. **Bruker ikke `ArtNavnId`, TaxonId eller API-synonymliste.** | 1419–1502 |
| Sluttopprydding | `validScientificNameId → Artens ID`, `validScientificName → Art`, `preferredPopularName → Navn`, `scientificNameRank → Taksonomisk nivå`. Ingen parsing, ID-erstatning eller trim av disse kildefeltene her. Hjelpe-ID-er og `TaxonId` inngår ikke i sluttkolonnene. | 2409–2467 |
| Manuelt norsk navn | Etter opprydding finner CLI manglende `Navn` og spør brukeren. Mappingen er **Art → Navn**, ikke ID → Navn. Den fyller bare manglende norske navn og endrer ikke `Art`, `Artens ID` eller rang. | 3213–3356, 3742–3749 |
| Eksport | CLI skriver den ferdige raddatatabellen direkte med `df.write_parquet(output)`. Ingen taksonsammenslåing ved eksport. CLI-oppsummeringens «Unike arter» teller for øvrig `Art` alene; dette er ikke en felles identitetsdefinisjon for alle stegene. | 3751–3771 |

Den ytre pipeline-funksjonen returnerer resultatet **før** manuell norsk navneutfylling; det er CLI som gjør dette ekstra steget (`3785–3908` mot `3742–3752`). En direkte funksjonskallers resultat kan derfor ha manglende `Navn`.

### Konkret konsekvens for Limosa

Ved korrekte Artskart-kilde-ID-er gir den faktisk kjørte berikingsfunksjonen:

```text
validScientificNameId    ArtNavnId    FamilieNavnId    OrdenNavnId
3767                     3767         3716             161320
3768                     3767         3716             161320
3769                     3767         3716             161320
```

Etter opprydding er `Artens ID` fremdeles **3767, 3768, 3769**; hjelpekolonnene er borte. Dersom to syntetiske kilderader derimot faktisk leverer **samme** `validScientificNameId` med ulike `validScientificName`, bevarer oppryddingen begge kombinasjonene. Berikingen validerer ikke vitenskapelig navn mot API-oppføringen.

Read-only-kontroll av dagens lokale attributtdatabase ga:

```text
ANF: 3767 | Limosa limosa           | Svært stor verdi | prioriterte=None
ANF: 3768 | Limosa limosa islandica | Svært stor verdi | prioriterte=1
Fuglegruppe: family | 3716 | Vadefugler
```

Ingen ANF-rad med ID 3769 ble funnet i dette avgrensede oppslaget. At navn med/uten `subsp.` kan treffe på **ID**, gjør ikke navnefallbacken tekstnormaliserende. Underart 3769 arver ikke automatisk ANF-feltene til 3767; eventuell sluttverdi kan komme fra kildens rødlistekategori. Dette er observerte oppslag/regler, ikke en ny faglig anbefaling om verdi eller vern.

## 4. Manglende/null ID og navn: det koden faktisk gjør

| Tilfelle | Eksisterende oppførsel – ikke en ny foreslått regel |
|---|---|
| Hele obligatoriske ID-/navnekolonnen mangler | Inputkontrakten avviser manglende kolonne. Direkte berikingskall avviser manglende ID-kolonne. |
| Null/NaN eller ID der `int()` gir `ValueError`/`TypeError`, blant gyldige ID-er | Beriking hopper over API-kall for den verdien, advarer og bevarer originalraden med null berikingsfelter. MTM-003 verifiserer null blandet med gyldig heltalls-ID. Andre konverteringsfeil er ikke eksplisitt fanget. Dette er ikke en generell garanti for at enhver ugyldig datatype passerer senere ANF-joins. |
| Ingen gyldige ID-er i ikke-tomt input til beriking | `ValueError`: «Ingen gyldige validScientificNameId-verdier å hente fra NorTaxa API.» Tomt resultat **etter årfilter** har en separat, tillatt pipelinegren. |
| ID kan konverteres til int, men API feiler/er tomt | `RuntimeError` for berikingen, også ved mislykket familie-/ordenoppslag. Ingen stille eksport med disse oppslagsfeilene. |
| Strengt heltall/positivt ID-domene | Beriking bruker Python `int(raw_species_id)`, ikke eksplisitt validering av positivt heltall eller brøkfrihet. Originalkolonnens datatype/verdi bevares, og oppslags-ID casts tilbake til denne typen ved join. `Artens ID` casts ikke i opprydding. Ikke skriv om dette som en streng, allerede implementert heltallskontrakt. |
| Null vitenskapelig navn med kjent ID | ANF kan treffe på ID, og nullnavnet beholdes. Test ANF-MTM-004 bekrefter dette. |
| Null ID med kjent vitenskapelig navn | ANF kan treffe på eksakt navn, og null-ID beholdes. ANF-MTM-006 tester funksjonen direkte; et helt ikke-tomt datasett med bare null-ID ville ha stoppet tidligere i NorTaxa-beriking. |
| Null ID og null navn i ANF | Null matcher ikke null i oppslagstabellen; kriterier blir `Nei`, og verdi kan falle tilbake til kildekategori. ANF-MTM-007. |
| Null/tom/blank `Art` eller rang | Ingen eksplisitt per-rad-avvisning eller utfylling i inngangsvalidering/opprydding. Null/tom/blank verdi videresendes. En manglende rang gir ikke arts-/underartsgrupperegel. |
| Manglende norsk `Navn` | Null, tom streng og whitespace regnes som manglende i navnehjelperne. Brukerinput trimmes og blankt svar avbryter. Ikke-blanke eksisterende navn overskrives ikke. Dette er ikke en generell normalisering av vitenskapelige navn. |
| Null `Art` **og** manglende `Navn` i manuell runde | `prompt_mangler_navn` bruker visningsteksten `—` som mappingnøkkel. Tilbakejoin på `Art` matcher ikke null med denne teksten. Reprodusert: `{'—': 'Ukjent'}` lot `Navn=None` stå. CLI har ingen etterkontroll som garanterer utfylt navn etter join. Dette er et eksisterende kanttilfelle, ikke rettet her. |
| Samme vitenskapelige navn, ulike familie-/ordenskombinasjoner | `finn_mangler_navn` avviser dette på **Art**, også før filtrering til bare manglende navn. Den validerer ikke «ett vitenskapelig navn per Artens ID». |

Kilder: `databehandling.py:122–147, 287–435, 874–918, 1473–1499, 1681–1803, 2409–2467, 3228–3356`. Relevant testkontrakt: `453–455, 465–469, 1515–1531, 3362–3398`.

**Skillet som må beholdes i planleggingen:** `obs_id` har streng utfylt/unik identitet; `Artens ID` og `Art` har ikke den samme per-rad-garantien. Et tomt felt, en nullverdi og en manglende kolonne er heller ikke det samme.

## 5. Hva Fuglefilter kan følge uten ny taksonomi

### Fakta om tilgjengelig kontrakt

Sluttdata leverer `obs_id`, `Artens ID`, `Art`, `Navn`, `Taksonomisk nivå`, ferdig `Fuglegruppe`, familie-/ordennavn og ferdige kategori-/forvaltningsfelter. De leverer **ikke** `ArtNavnId`, `FamilieNavnId`, `OrdenNavnId`, API-`taxonId`, `taxonNameId`, API-navneposter med accepted/synonym-status eller et komplett foreldre-ID-hierarki (`databehandling.py:2409–2449`; test `2635`).

Dermed kan pluginen bevare rader, nøkkelverdier, navn, nivå og ferdige vurderinger direkte. Den kan ikke utlede en sikker foreldrearts-ID eller avgjøre synonymer fra Parquet alene uten en ny kontrakt/oppslagslogikk. Å telle unike artsnivåer ved å kutte navn til to ord ville være en ny, udokumentert klassifisering.

GIS-klargjøreren bevarer attributtene ved geometriomforming og ved opprettelse av polygonbarn (`~/qgis/.../scripts/prepare_bird_data.py:36–66, 103–123`). Den kontrollerer obligatoriske felt og streng `obs_id`, ikke fullstendig taksonidentitet (`127–141`).

`dataanalyse/data_analyse.py:311–342` validerer metadata og grupperer på **`["Artens ID", "Art"]`**. Norsk navn er presentasjonsmetadata, ikke identitet. Måned/år og sesong teller derimot **`Artens ID` alene, med null fjernet** (`1084–1089`, `1424–1431`). Dagens tellinger er altså ikke allerede samordnet. Det stemmer med den åpne grenen i rettingsplanen; dette notatet endrer dem ikke.

### Anbefalt beslutning, skilt fra API-fakta

1. **Behold kildeparet `(Artens ID, Art)` som rapportgruppering**, i tråd med brukerens avvisning av ID-alene. Følg samme besluttede nøkkel i relevante deltabeller, ikke `Navn`, foreldreart eller API-`taxonId`. Det bevarer både korrekte arts-/underarts-ID-er og brukerens krevde tilfelle med samme ID/ulike vitenskapelige navn.
2. **Ikke slå sammen, avvis eller skriv om slike kildepar automatisk fordi dagens API viser et synonym eller en annen ID/navn-kombinasjon.** De kan være egne rapportgrupper uten at programmet lover at hver gruppe er et nytt biologisk takson. Forklar tellingen som grupper etter de leverte taksonnøklene; ikke ubetinget antall arter.
3. **Ingen nye API-kall, navneparsing eller ny verdivurdering i pluginen.** Bruk allerede behandlet `Navn`, `Fuglegruppe`, `Kategori`, `Verdi M1941` og forvaltningskriterier. Den manuelle navnerunden er upstream-utfylling, ikke en navnekorrigering pluginen skal gjenta.

Dette er en bevaringsregel, ikke påstanden om at preprocessing har én universell sammensatt nøkkel. Beriking bruker ID, ANF bruker ID med navnefallback, manuelle norske navn bruker Art, CLI-oppsummeringen teller Art, og artsstatistikken bruker paret. Bare de opprinnelige identitetsfeltene passerer gjennom alle disse stegene.

### Åpne beslutninger til neste grilling-runde

- Hvordan skal rapportgrupper og taksontelling håndtere null-ID med navn, ID med null/blankt navn, og begge manglende? Koden avgjør ikke denne rapportpolicyen. Ikke innfør avvisning som om preprocessing allerede garanterte komplette nøkler.
- Skal vitenskapelig navnetekst sammenlignes helt eksakt eller med uttrykkelig avgrenset whitespace-normalisering? Dagens eksport normaliserer den ikke; `subsp.`-fjerning, casefolding eller synonymutjevning er **ikke** eksisterende kontrakt. Null og blanke strenger må avklares separat.
- Hvordan omtales alias-/konfliktgrupper i rapporten uten å fremstille antall grupper som et sikkert antall biologiske arter/taksa? Kravet om separate grupper er allerede besluttet; forklaring/telling er den åpne delen.
- Null-`Art`-kanttilfellet i manuell norsk navneutfylling bør registreres som upstream-begrensning. Det ligger utenfor godkjent produksjonsomfang, som fortsatt beskytter `databehandling.py`.

## 6. Uttakstid og bevarte responsfingeravtrykk

Alle tider er **2026-09-22 UTC**. SHA-256 gjelder rå HTTP-responskropp, ikke JSON etter omformatering. URL-røtter: **N** = `https://nortaxa.artsdatabanken.no`, **A** = `https://artskart.artsdatabanken.no/publicapi`. Utdragene over og reproduksjonskommandoene under gjør påstandene etterprøvbare selv om live-data senere endres.

| Kilde | Tid | SHA-256 |
|---|---|---|
| N `/swagger/v1/swagger.json` | 12:49:13 | `bf84f3d705d168ee2c7e91f676d62e3a5e4f6a69092f70e7554f68f3c0c86442` |
| N `/api/v1/TaxonName/ByScientificNameId/3767` | 12:49:12 | `54212dafef399009a69e9861d76737fd08d181008d252c0847af71a424bc4865` |
| N `/api/v1/TaxonName/ByScientificNameId/3768` | 12:49:47 | `379a3a6b1ff497d9ef1ce83cb1d63351927a3c68eab9c82a974cc01d9787fd1c` |
| N `/api/v1/TaxonName/ByScientificNameId/3769` | 12:49:47 | `f0dc7e6b7fde87e0e45b8aded61038516cd4a0477160729f1f7afb62a55dedfe` |
| N `/api/v1/TaxonName/Children/ByScientificNameId/3767` | 12:49:27 | `3dc2fa70034acc0acf4292a9a9ab06a2199eb86f0f4be25a6dc4ca8ac8db1229` |
| N `/api/v1/TaxonName/Children/ByScientificNameId/3763` | 12:49:47 | `762be7032607be9375564ac8c2005ffbf5b46397ae07529aa9d4b16590131612` |
| N `/api/v1/TaxonName/ByTaxonId/3709` | 12:49:47 | `379a3a6b1ff497d9ef1ce83cb1d63351927a3c68eab9c82a974cc01d9787fd1c` |
| N `/api/v1/TaxonName/ByScientificNameId/3468` | 12:49:27 | `bb5da32ba13ebddf56167b781ff69f1933d6e38f5e6f160197664960ff598bea` |
| N `/api/v1/TaxonName/ByScientificNameId/3469` | 12:49:27 | `8d75b73e30d0f348d32d3072c2c38f348ea7ee7633a9f43a2a69ae496caec762` |
| N `/api/v1/TaxonName/ByScientificNameId/295741` | 12:49:47 | `dca6f3341dda1510aa2f4eb25824e48a1a92091e165fe85dd4c96368d4f9e70a` |
| N `/api/v1/TaxonName/ByScientificNameId/3853` | 12:50:05 | `3c67c57458896b49e9d6196455c4feda925ef3c0ea41109c2342e8fbdc3b55b5` |
| A `/swagger/docs/v1` | 12:51:30 | `23a62033a7f86935881bffdb258a157615411287156841bf0e7e09f9aad73015` |
| A `/api/taxon/3708` | 12:51:41 | `8a2898bade970deb700e250adb44d48ab1660dc747c73922b182647855dc4af3` |
| A `/api/taxon/3709` | 12:51:41 | `26dc7d9961d8d493c3ad8c82002dcb5657984983661ca6c075f1a4eb04375698` |
| A `/api/taxon/3710` | 12:51:41 | `ae5c8c73bd5dfbca017f38e064e0d974afa9b33a52bd8cef112c99d1b90de799` |

Oppdagelsessøk ble brukt for å finne offisielle lenker, ikke som fasit for ID-ene. NorTaxas `/about` var JavaScript-renderet; beskrivelsen der ble ikke brukt som ubekreftet skjema. Prøvde Artskart-ruter `/appapi/swagger/v1/swagger.json`, `/appapi/swagger/docs/v1` og `/publicapi/swagger/v1/swagger.json` ga 404; den offisielt lenkede `/publicapi/swagger/docs/v1` virket.

## 7. Reproduksjon uten datamutering

### Enkle offentlige API-oppslag

Disse GET-kallene laster bare skjema og offentlige takson-/navneposter; ingen forekomstendepunkter:

```sh
curl -fsS https://nortaxa.artsdatabanken.no/swagger/v1/swagger.json | python3 -m json.tool
curl -fsS https://artskart.artsdatabanken.no/publicapi/swagger/docs/v1 | python3 -m json.tool
for id in 3767 3768 3769 3468 3469 295741 3853; do
  curl -fsS "https://nortaxa.artsdatabanken.no/api/v1/TaxonName/ByScientificNameId/$id" | python3 -m json.tool
done
for id in 3763 3767; do
  curl -fsS "https://nortaxa.artsdatabanken.no/api/v1/TaxonName/Children/ByScientificNameId/$id" | python3 -m json.tool
done
for id in 3708 3709 3710; do
  curl -fsS "https://artskart.artsdatabanken.no/publicapi/api/taxon/$id" | python3 -m json.tool
done
```

I undersøkelsen ble tilsvarende GET utført med Python `urllib.request`, med UTC-tid og SHA-256 skrevet til terminalen. Minste identiske uttaksmetode for én post:

```sh
python3 -B - <<'PY'
import hashlib, urllib.request
from datetime import datetime, timezone
url = 'https://nortaxa.artsdatabanken.no/api/v1/TaxonName/ByScientificNameId/3768'
with urllib.request.urlopen(url, timeout=30) as response:
    body = response.read()
    print(url, datetime.now(timezone.utc).isoformat(), response.status)
print(hashlib.sha256(body).hexdigest())
print(body.decode())
PY
```

### Selektive lokale kontroller som faktisk ble kjørt

**Resultat: 11 eksisterende testceller bestått**, pluss live Limosa-beriking og syntetiske kontroller av kildepar/null/blank og null-`Art` i navneprompt. Miljøet hadde Polars **1.43.2**, DuckDB **1.5.5**, pytest **9.1.1**. Ingen installasjon eller full pytest-/Marimo-/innlesingskjøring ble gjort.

Kommandoen under laster bare funksjonsdefinisjoner fra AST og fjerner Marimo-dekoratører. Den kjører ikke notatboken, CLI, hele CSV-pipelinen eller andre testceller. Attributtdatabasen åpnes bare lesbart. Syntetiske rader finnes bare i minnet; API-funksjonen sender kun ID-en i URL-en.

```sh
cd /home/havhje/koding/fugl_artsdataanalyser
.venv/bin/python -B - <<'PY'
import ast, inspect, time, requests, duckdb, pytest, typer
from datetime import date, datetime, date as dt_date
from functools import lru_cache
from pathlib import Path
from typing import Any
from unittest.mock import patch
import polars as pl
from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn, MofNCompleteColumn
p = Path('databehandling/databehandling.py')
nodes = [n for n in ast.parse(p.read_text()).body if isinstance(n, ast.FunctionDef)]
for n in nodes:
    n.decorator_list = []
exec(compile(ast.Module(body=nodes, type_ignores=[]), str(p), 'exec'))
DESIRED_RANKS, NORTAXA_API_BASE_URL, _ = definer_nortaxa_konstanter()
console = Console()
extract_hierarchy_and_ids, fetch_taxon_data, get_norwegian_name = definer_nortaxa_hjelpefunksjoner(DESIRED_RANKS, NORTAXA_API_BASE_URL)
process_and_enrich_data, = definer_process_and_enrich_data(DESIRED_RANKS, 0, console, extract_hierarchy_and_ids, fetch_taxon_data, get_norwegian_name)
for name in ['test_process_and_enrich_data_mtm_003', 'test_process_and_enrich_data_mtm_004', 'test_process_and_enrich_data_mtm_006', 'test_process_and_enrich_data_mtm_007']:
    fn = globals()[name]
    fn(**{arg: globals()[arg] for arg in inspect.signature(fn).parameters})
    print('PASS', name)
with duckdb.connect('databehandling/fugl_atributt_data', read_only=True) as bird_data:
    legg_til_arter_av_nasjonal_forvaltningsinteresse, = definer_anf_kriterier_og_m1941(bird_data)
    for name in ['ANF_MTM_003', 'ANF_MTM_004', 'ANF_MTM_005', 'ANF_MTM_006', 'ANF_MTM_007']:
        globals()[name](legg_til_arter_av_nasjonal_forvaltningsinteresse)
        print('PASS', name)
lag_rydd_navn_og_datatyper_input, _, rydd_navn_og_datatyper_forventede_kolonner = rydd_navn_og_datatyper_testhjelpere()
RYDD_MTM_001(lag_rydd_navn_og_datatyper_input, rydd_navn_og_datatyper_forventede_kolonner)
RYDD_MTM_005(lag_rydd_navn_og_datatyper_input)
print('PASS RYDD_MTM_001 og RYDD_MTM_005')
# Kun syntetiske navne-/ID-rader; ingen observasjonsdata fra disk.
ids = [3767, 3768, 3769]
df = pl.DataFrame({'validScientificNameId': ids})
enriched = process_and_enrich_data(df)
assert enriched['validScientificNameId'].to_list() == ids
assert enriched['ArtNavnId'].to_list() == [3767] * 3
assert enriched['FamilieNavnId'].to_list() == [3716] * 3
assert enriched['OrdenNavnId'].to_list() == [161320] * 3
print('PASS live Limosa: egne navne-ID-er, felles ArtNavnId')
df = lag_rydd_navn_og_datatyper_input([
    {'validScientificNameId': 3767, 'validScientificName': 'Limosa limosa'},
    {'validScientificNameId': 3767, 'validScientificName': 'Limosa limosa subsp. islandica'},
    {'validScientificNameId': None, 'validScientificName': None, 'preferredPopularName': None},
    {'validScientificNameId': None, 'validScientificName': '  ', 'preferredPopularName': '  '},
])
out = rydd_navn_og_datatyper(df)
assert out['Artens ID'].to_list() == [3767, 3767, None, None]
assert out['Art'].to_list() == ['Limosa limosa', 'Limosa limosa subsp. islandica', None, '  ']
assert out['Navn'].to_list()[-2:] == [None, '  ']
print('PASS same-ID/ulike-navn og null/blank bevares i opprydding')
prompt_mangler_navn, = definer_prompt_mangler_navn(console)
missing = finn_mangler_navn(out.filter(pl.col('Art').is_null()))
with patch.object(console, 'print'), patch.object(Prompt, 'ask', return_value='Ukjent'):
    mapping = prompt_mangler_navn(missing)
assert mapping == {'—': 'Ukjent'}
assert join_navn_til_orginal_df(out, mapping).filter(pl.col('Art').is_null())['Navn'].to_list() == [None]
print('PASS null-Art-kanttilfelle reprodusert: prompt fyller ikke nullnøkkel')
PY
```

Read-only-kontrollen av Limosa-reglene:

```sh
.venv/bin/python -B - <<'PY'
import duckdb
with duckdb.connect('databehandling/fugl_atributt_data', read_only=True) as con:
    print(con.execute("SELECT vitenskapelig_navn_id, vitenskapelig_navn, forvaltningsverdi, kriterium_prioriterte_arter, kriterium_ansvarsart FROM arter_av_nasjonal_forvaltningsinteresse WHERE vitenskapelig_navn_id IN (3767,3768,3769) OR vitenskapelig_navn ILIKE 'Limosa limosa%' ORDER BY vitenskapelig_navn_id").fetchall())
    print(con.execute("SELECT * FROM fuglegruppe_regler WHERE vitenskapelig_navn_id IN (3767,3768,3769,3716,161320)").fetchall())
PY
```

Arbeidskopi-/kildekontroll:

```sh
git rev-parse HEAD
git status --short
git -C /home/havhje/qgis rev-parse HEAD
git -C /home/havhje/qgis status --short
sha256sum databehandling/databehandling.py databehandling/fugl_atributt_data dataanalyse/data_analyse.py
rg -n 'scientificNameId|validScientificNameId|ArtNavnId|TaxonId|validScientificName|scientificNameRank|Artens ID|Taksonomisk nivå' databehandling/databehandling.py
```

**Begrensninger:** Ingen full innlesing eller ny eksport er kjørt, ingen brukerprosjekter er åpnet, og notatet validerer ikke alle historiske Artskart-eksporter. Den syntetiske raden med samme ID og underartsnavn viser bevaringsevne, ikke at akkurat den kombinasjonen leveres av dagens API. Nye null-/normaliserings-/tellevalg må fortsatt bekreftes av brukeren før implementering.

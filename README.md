# Kontrol af joblog

Kontrollerer automatisk om borgere i målgrupperne INT-KP og 6.2 har søgt det krævede antal jobs i jobloggen for den foregående måned, og opretter en opgave til sagsbehandleren i Momentum, hvis kravet ikke er opfyldt.

## Hvad gør robotten?

1. Henter borgere fra Momentum filtreret på målgruppekoderne INT-KP og 6.2 samt specifikke sagsbehandlerteams.
2. Udelukker borgere med barselsfravær eller aktiv fritagelse for joblog.
3. Tilføjer borgere til arbejdskøen i Automation Server, hvis de ikke allerede er behandlet i den aktuelle måned.
4. For hver borger i køen: henter personvisitationstatus for at afgøre, om borgeren er fritaget for joblog (op til 10 forsøg ved HTTP 504-fejl).
5. Henter det krævede antal jobansøgninger fra borgerens jobsøgningsdefinition i Momentum.
6. Henter joblog-aktiviteter for den foregående kalendermåned og deduplikerer ansøgninger baseret på titel, virksomhedsnavn, postnummer, by, afstand og ugentlige timer.
7. Sammenligner antallet af unikke ansøgninger med kravet og opretter en opgave til sagsbehandleren i Momentum med 7 dages forfaldsdato, hvis borgeren ikke har søgt nok jobs.

## Forudsætninger

- Python ≥ 3.13
- [`uv`](https://docs.astral.sh/uv/) til pakkehåndtering
- Adgang til **Automation Server** (arbejdskø)
- Adgang til **Momentum** (produktion)
- Adgang til **Odense SQL Server**

## Installation

```sh
uv sync
```

## Konfiguration

Credentials registreres i Automation Server:
- `Momentum - produktion`
- `Odense SQL Server`

| Miljøvariabel | Beskrivelse |
|---|---|
| `ATS_URL` | URL til Automation Server API (f.eks. `http://localhost/api`) |
| `ATS_TOKEN` | Bearer token til autentificering mod Automation Server |
| `ATS_WORKQUEUE_OVERRIDE` | Overskriver arbejdskø-ID til lokal testning |

## Kørsel

```sh
uv run python main.py --queue   # Fyld arbejdskøen
uv run python main.py           # Behandl arbejdskøen
```

## Afhængigheder

| Pakke | Formål |
|---|---|
| `automation-server-client` | Klient til Automation Server RPA-orkestratoren – håndterer arbejdskø, credentials og workitems |
| `momentum-client` | Klient til Momentum sagsstyringssystem – henter borgerdata, joblog og opretter opgaver |
| `odk-tools` | ODK-hjælpeværktøjer til rapportering og aktivitetssporing |
| `ruff` | Python linter til kodekvalitet |
| `tenacity` | Retry-bibliotek til håndtering af forbigående HTTP-fejl |

## GDPR og sikkerhed

Processen behandler følgende personoplysninger:
- CPR-numre på borgere i målgrupperne INT-KP og 6.2
- Joblog-data inkl. ansøgte stillinger, virksomhedsnavne, postnumre og arbejdssted

Data behandles udelukkende i hukommelsen under kørsel og gemmes ikke lokalt. Adgang til processen og Automation Server bør begrænses til medarbejdere med et tjenstligt behov.

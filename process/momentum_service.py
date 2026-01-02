import re

from datetime import datetime, timedelta, timezone
from momentum_client.manager import MomentumClientManager
from odk_tools.reporting import report
from odk_tools.tracking import Tracker
from automation_server_client import WorkItemError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from requests.exceptions import HTTPError

proces_navn = "Kontrol af joblog"

class MomentumService:
    def __init__(
        self,
        momentum: MomentumClientManager,
        tracker: Tracker,
    ):
        self.momentum = momentum        
        self.tracker = tracker

    def __parse_date(self, date_str):
        if isinstance(date_str, datetime):
            return date_str
        if isinstance(date_str, str):
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return datetime.min.replace(tzinfo=timezone.utc)

    def opret_opgave_til_sagsbehandler(self, borger, beskrivelse):
        medarbejder = self.momentum.borgere.hent_sagsbehandler("dorf")

        if not medarbejder:
            raise WorkItemError("Sagsbehandler 'dorf' ikke fundet i Momentum.")

        self.momentum.opgaver.opret_opgave(
            borger=borger,
            medarbejdere=[medarbejder],
            forfaldsdato=datetime.now(timezone.utc) + timedelta(days=7),
            titel="Kontrol af joblog",
            beskrivelse=beskrivelse,
        )
        

    @retry(
        retry=retry_if_exception_type(HTTPError),
        stop=stop_after_attempt(10),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True
    )
    def _hent_personvisitationstatus_med_retry(self, borger):
        """Hent personvisitationstatus med retry på 504 fejl."""
        return self.momentum.borgere.hent_personvisitationstatus(borger=borger)

    def fritaget_for_joblog(self, borger) -> bool:
        personvisitationstatus = self._hent_personvisitationstatus_med_retry(borger)

        if not personvisitationstatus:
            raise WorkItemError(f"Personvisitationstatus for borger med CPR {borger['cpr']} ikke fundet i Momentum.")
        
        person_exempt_names = personvisitationstatus.get('personExemptNames')

        if person_exempt_names and "Brug af Joblog" in person_exempt_names:                    
            report(
                report_id="kontrol_af_joblog",
                group="Manuel behandling",
                json={
                    "Cpr": borger["cpr"],
                    "Manuel beskrivelse": "Der skal ikke tjekkes mere, da borger er fritaget for brug af joblog."
                },
            )

            self.tracker.track_partial_task(proces_navn)
            return True
        
        return False

    def hent_krav_til_jobsøgning(self, borger) -> int|None:        
        job_definition = self.momentum.borgere.hent_jobsøgningsdefinition(borger=borger)

        if not job_definition:
            raise WorkItemError(f"Jobsøgningsdefinition for borger med CPR {borger['cpr']} ikke fundet i Momentum.")

        krav_tekst = job_definition.get("otherExpectations")

        if krav_tekst is None or len(krav_tekst) == 0:
            self.opret_opgave_til_sagsbehandler(borger, "'Krav til jobsøgning' blev ikke fundet.")

            report(
                report_id="kontrol_af_joblog",
                group="Behandlet",
                json={
                    "Cpr": borger["cpr"],
                    "Udført": "Opgave til sagsbehandler",
                    "Beskrivelse": "'Krav til jobsøgning' blev ikke fundet."
                },
            )
            self.tracker.track_task(proces_navn)
            return None


        match = re.search(r'(\d+)\s+job', krav_tekst, re.IGNORECASE)
        krav_antal = int(match.group(1)) if match else None

        if krav_antal is None:
            self.opret_opgave_til_sagsbehandler(borger, "Der mangler oplysninger om antallet af jobs i 'Krav til jobsøgning'.")

            report(
                report_id="kontrol_af_joblog",
                group="Behandlet",
                json={
                    "Cpr": borger["cpr"],
                    "Udført": "Opgave til sagsbehandler",
                    "Beskrivelse": "Der mangler oplysninger om antallet af jobs i 'Krav til jobsøgning'."
                },
            )
            self.tracker.track_task(proces_navn)
            return None

        if krav_antal == 0:
            self.tracker.track_partial_task(proces_navn)
            return None
        
        return krav_antal
    
    def hent_joblog_aktiviteter(self, borger) -> int:
        # Hent joblog aktiviteter
        joblog = self.momentum.borgere.hent_joblog(borger=borger)

        if not joblog:
            raise WorkItemError(f"Joblog for borger med CPR {borger['cpr']} ikke fundet i Momentum.")
        
        now = datetime.now(timezone.utc)
        start_dato = (now.replace(day=1) - timedelta(days=1)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        slut_dato = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0) - timedelta(microseconds=1)                
        tidligere_måneds_joblog = [entry for entry in joblog if start_dato <= self.__parse_date(entry.get('submissionDate')) <= slut_dato and start_dato <= self.__parse_date(entry.get('updatedAt')) <= slut_dato]
        
        # Hash joblog entries og filtrer duplicates
        unikke_jobs = {}
        for joblog_entry in tidligere_måneds_joblog:
            job_hash = f"{joblog_entry.get('title', '')} {joblog_entry.get('companyName', '')} {joblog_entry.get('companyPostCode', '')} {joblog_entry.get('companyTown', '')} {joblog_entry.get('distanceToCompanyInMeters', '')}"
            if job_hash not in unikke_jobs:
                unikke_jobs[job_hash] = joblog_entry
        
        antal_søgte_jobs = len(unikke_jobs)

        return antal_søgte_jobs
    
    def kontroller_jobsøgning(self, borger: dict, krav_til_jobsøgning: int, antal_søgte_jobs: int):
        if krav_til_jobsøgning > 0 and antal_søgte_jobs == 0:
            self.opret_opgave_til_sagsbehandler(borger, "Der var ikke registreret nogen jobs i joblog.")

            report(
                report_id="kontrol_af_joblog",
                group="Behandlet",
                json={
                    "Cpr": borger["cpr"],
                    "Udført": "Opgave til sagsbehandler",
                    "Beskrivelse": "Der var ikke registreret nogen jobs i joblog."
                },
            )
            self.tracker.track_task(proces_navn)

        elif antal_søgte_jobs < krav_til_jobsøgning:
            self.opret_opgave_til_sagsbehandler(borger, "Der er registreret for få job i joblog.")

            report(
                report_id="kontrol_af_joblog",
                group="Behandlet",
                json={
                    "Cpr": borger["cpr"],
                    "Udført": "Opgave til sagsbehandler",
                    "Beskrivelse": "Der er registreret for få job i joblog."
                },
            )
            self.tracker.track_task(proces_navn)
            
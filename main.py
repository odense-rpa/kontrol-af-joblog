import asyncio
import logging
import sys

from datetime import datetime
from automation_server_client import AutomationServer, Workqueue, WorkItemError, Credential, WorkItemStatus
from momentum_client.manager import MomentumClientManager
from odk_tools.tracking import Tracker
from process.momentum_service import MomentumService

momentum: MomentumClientManager
momentum_service: MomentumService

async def populate_queue(workqueue: Workqueue):
    filters = [
        {
            "customFilter": "",
            "fieldName": "targetGroupCode",
            "values": [
                "INT-KP",
                "6.2"
            ]
        },
        {
            "customFilter": "",
            "fieldName": "primaryCaseworkerTeamId",
            "values": [
                "",
                "b345ab13-e8b8-409f-b87b-6925268472de",
                "80180c8c-5863-40ae-a85b-e14d33597e6a",
                "c58e4d9f-af8e-4553-a3d0-c2b102cc33c2"
            ]
        },
        {
            "customFilter": "exclude",
            "fieldName": "absences",
            "values": [
                None,
                None,
                None,
                None,
                "",
                "ABSENCE_BARSEL",
                "ABSENCE_FRITAGELSE_FOR_JOBLOG"
            ]
        }

    ]

    borgere = momentum.borgere.hent_borgere(filters=filters)

    if not borgere or len(borgere['data']) == 0:
        return

    for borger in borgere['data']:
        eksisterende_kødata = workqueue.get_item_by_reference(
            str(borger["cpr"]), status=WorkItemStatus.COMPLETED
        )
        eksisterende_kødata = [
            item
            for item in eksisterende_kødata
            if item.updated_at > datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        ]

        if len(eksisterende_kødata) == 0:
            borger_data = {
                "cpr": borger["cpr"]                
            }

            workqueue.add_item(borger_data, reference=str(borger["cpr"]))


async def process_workqueue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)

    for item in workqueue:
        with item:
            data = item.data
 
            try:
                borger = momentum.borgere.hent_borger(cpr=item.reference)

                if not borger:
                    raise WorkItemError(f"Borger med CPR {item.reference} ikke fundet i Momentum.")

                if momentum_service.fritaget_for_joblog(borger):
                    continue
                
                krav_til_jobsøgning = momentum_service.hent_krav_til_jobsøgning(borger)

                if krav_til_jobsøgning is None:
                    continue

                antal_søgte_jobs = momentum_service.hent_joblog_aktiviteter(borger)
                
                momentum_service.kontroller_jobsøgning(
                    borger=borger,
                    krav_til_jobsøgning=krav_til_jobsøgning,
                    antal_søgte_jobs=antal_søgte_jobs
                )
                
            except WorkItemError as e:
                # A WorkItemError represents a soft error that indicates the item should be passed to manual processing or a business logic fault
                logger.error(f"Error processing item: {data}. Error: {e}")
                item.fail(str(e))


if __name__ == "__main__":
    ats = AutomationServer.from_environment()
    workqueue = ats.workqueue()

    momentum_credential = Credential.get_credential("Momentum - produktion")
    momentum = MomentumClientManager(
        base_url=momentum_credential.data["base_url"],
        client_id=momentum_credential.username,
        client_secret=momentum_credential.password,
        api_key=momentum_credential.data["api_key"],
        resource=momentum_credential.data["resource"],
    )

    tracking_credential = Credential.get_credential("Odense SQL Server")
    tracker = Tracker(
        username=tracking_credential.username, password=tracking_credential.password
    )

    momentum_service = MomentumService(
        momentum=momentum,
        tracker=tracker,
    )

    # Queue management
    if "--queue" in sys.argv:
        workqueue.clear_workqueue(WorkItemStatus.NEW)
        asyncio.run(populate_queue(workqueue))
        exit(0)

    # Process workqueue
    asyncio.run(process_workqueue(workqueue))

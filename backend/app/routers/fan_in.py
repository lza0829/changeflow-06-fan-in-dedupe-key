from fastapi import APIRouter, HTTPException

from app.schemas.entities import EntryPatch, FanInRequest, FanInResetRequest
from app.services import fan_in_service

router = APIRouter(tags=["fan_in"])


@router.get("/fan-in/entries")
def list_entries():
    return fan_in_service.list_entries()


@router.patch("/fan-in/entries/{entry_id}")
def patch_entry(entry_id: int, body: EntryPatch):
    try:
        return fan_in_service.patch_entry(entry_id, enabled=body.enabled)
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/fan-in/entries/reset")
def reset_entries(body: FanInResetRequest | None = None):
    return fan_in_service.reset_entry_cursors(body.entry_ids if body else None)


@router.get("/fan-in/batches")
def list_batches():
    return fan_in_service.list_batches()


@router.get("/fan-in/batches/{batch_id}")
def get_batch(batch_id: int):
    detail = fan_in_service.get_batch_detail(batch_id)
    if not detail:
        raise HTTPException(404, "batch not found")
    return detail


@router.post("/fan-in/batches")
def run_batch(body: FanInRequest):
    try:
        return fan_in_service.run_fan_in(body.entry_ids, body.max_per_entry)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc

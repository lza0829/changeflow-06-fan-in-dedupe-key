from fastapi import APIRouter

from app.db import connect
from app.repositories import ledger as ledger_repo

router = APIRouter(tags=["ledger"])


@router.get("/ledger")
def list_ledger(batch_id: int | None = None):
    conn = connect()
    try:
        return ledger_repo.list_ledger(conn, batch_id=batch_id)
    finally:
        conn.close()

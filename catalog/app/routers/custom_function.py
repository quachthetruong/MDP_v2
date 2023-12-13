from fastapi import (
    APIRouter,
    Depends,
    
)
from sqlalchemy.orm import Session

from backend.session import create_session

from services.stream import StreamService


router = APIRouter()


@router.post("/get_record")
async def save_stream(
    
    session: Session = Depends(create_session),
) :
    """Create a new stream."""
    return StreamService(session)




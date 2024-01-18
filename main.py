from core.event.consume_event import consume_assign_token_event, consume_reused_refresh_token_event, consume_revoke_token_event, consume_update_token_event
import asyncio
from collections import Counter

async def main():
    await asyncio.gather(
        consume_assign_token_event(), 
        consume_reused_refresh_token_event(), 
        consume_revoke_token_event(), 
        consume_update_token_event(),
    )

asyncio.run(main=main())

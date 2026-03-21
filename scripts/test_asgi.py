import asyncio
import sys
from pathlib import Path

import httpx

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.main import app


async def main() -> None:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/v1/courses/00000000-0000-0000-0000-000000000000/versions",
            headers={"X-API-Key": "dev-local-secret-change-me"},
        )
        print(response.status_code)
        print(response.headers.get("content-type"))
        print(response.text)


if __name__ == "__main__":
    asyncio.run(main())


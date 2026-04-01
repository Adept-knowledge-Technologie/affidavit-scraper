"""
Control Plane API upload client.
Handles login, constituency mapping, signed URL upload, and job creation.
"""
import time
import httpx
import logging
from typing import Optional

logger = logging.getLogger("eci_affidavit")

API_BASE = "https://control-plane-backend-319443098926.asia-south1.run.app"
API_EMAIL = "ml_engineer2@ak-tech.in"
API_PASSWORD = "semil@0503##"
ELECTION_YEAR = 2026


class UploadClient:

    def __init__(self):
        self._token: Optional[str] = None
        self._constituency_map: dict = {}
        self._client: Optional[httpx.AsyncClient] = None
        self._login_time: float = 0

    async def init(self):
        self._client = httpx.AsyncClient(timeout=120)
        await self._login()
        await self._load_constituencies()
        logger.info(f"Upload client ready — {len(self._constituency_map)} constituencies mapped")

    async def close(self):
        if self._client:
            await self._client.aclose()

    async def _login(self):
        r = await self._client.post(f"{API_BASE}/auth/login", json={
            "email": API_EMAIL,
            "password": API_PASSWORD,
        })
        r.raise_for_status()
        self._token = r.json()["access_token"]
        self._login_time = time.time()
        logger.info("Logged in to Control Plane API")

    async def _ensure_token(self):
        """Re-login if token is older than 50 minutes."""
        if time.time() - self._login_time > 3000:  # 50 minutes
            logger.info("[Upload] Refreshing token...")
            await self._login()

    async def _load_constituencies(self):
        r = await self._client.get(
            f"{API_BASE}/tna/years/{ELECTION_YEAR}/constituencies",
            params={"limit": 500},
            headers={"Authorization": f"Bearer {self._token}"},
        )
        r.raise_for_status()
        for item in r.json().get("constituencies", []):
            name_en = item["constituency_name"].split("/")[0].strip().upper()
            self._constituency_map[name_en] = str(item["constituency_code"])

    def get_constituency_code(self, name: str) -> Optional[str]:
        name_upper = name.strip().upper()
        if name_upper in self._constituency_map:
            return self._constituency_map[name_upper]
        for key, code in self._constituency_map.items():
            if name_upper in key or key in name_upper:
                return code
        return None

    async def upload_pdf(self, constituency_name: str, filename: str, pdf_bytes: bytes) -> bool:
        code = self.get_constituency_code(constituency_name)
        if not code:
            logger.warning(f"No constituency code found for: {constituency_name}")
            return False

        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self._token}"}

        # Retry up to 3 times
        for attempt in range(3):
            try:
                # Step 1: Get signed URL
                r1 = await self._client.post(
                    f"{API_BASE}/api/storage/upload-url",
                    json={"year": ELECTION_YEAR, "folder_id": code, "filename": filename, "file_type": "pdf"},
                    headers=headers,
                )
                if r1.status_code in (401, 403):
                    await self._login()
                    headers = {"Authorization": f"Bearer {self._token}"}
                    continue
                if r1.status_code == 500:
                    logger.warning(f"  upload-url 500 (attempt {attempt+1}), retrying...")
                    await self._login()
                    headers = {"Authorization": f"Bearer {self._token}"}
                    import asyncio
                    await asyncio.sleep(2)
                    continue
                r1.raise_for_status()

                data = r1.json()
                signed_url = data["upload_url"]
                gcs_path = data["target_path"]

                if data.get("is_duplicate"):
                    logger.info(f"  Already uploaded: {filename}")
                    return True

                # Step 2: PUT to GCS
                r2 = await self._client.put(
                    signed_url,
                    content=pdf_bytes,
                    headers={"Content-Type": "application/pdf"},
                )
                r2.raise_for_status()

                # Step 3: Create job
                job_id = f"eci-affidavit-{code}-{int(time.time())}"
                r3 = await self._client.post(
                    f"{API_BASE}/jobs/create",
                    params={"overwrite": "true"},
                    json={
                        "job_id": job_id,
                        "pdf_filename": filename,
                        "pdf_storage_uri": f"gs://pdf-raw-data-prod-projectu/{gcs_path}",
                        "pdf_size_bytes": len(pdf_bytes),
                        "pdf_page_count": 1,
                        "job_context": {"year": ELECTION_YEAR, "constituency": code, "state": "TN", "source": "MANUAL_UPLOAD"},
                    },
                    headers=headers,
                )
                if r3.status_code not in (200, 409):
                    r3.raise_for_status()

                logger.info(f"  Uploaded: {constituency_name}/{filename}")
                return True

            except Exception as e:
                if attempt == 2:
                    logger.error(f"  Upload failed for {constituency_name}/{filename}: {e}")
                    return False
                logger.warning(f"  Upload attempt {attempt+1} failed: {e}, retrying...")
                import asyncio
                await asyncio.sleep(3)

        return False

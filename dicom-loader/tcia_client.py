"""TCIA REST API client for downloading public DICOM studies."""

import os
import io
import zipfile
import logging
import time
import requests

logger = logging.getLogger(__name__)

TCIA_BASE = "https://services.cancerimagingarchive.net/nbia-api/services/v1"


class TCIAClient:
    """Client for The Cancer Imaging Archive REST API."""

    def __init__(self, max_retries: int = 3, retry_delay: int = 5):
        self.session = requests.Session()
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def _request(self, endpoint: str, params: dict = None) -> requests.Response:
        """Make a GET request with retries."""
        url = f"{TCIA_BASE}/{endpoint}"
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=120)
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                if attempt == self.max_retries:
                    raise
                wait = self.retry_delay * attempt
                logger.warning("TCIA request failed (attempt %d/%d): %s — retrying in %ds",
                               attempt, self.max_retries, e, wait)
                time.sleep(wait)

    def get_studies(self, collection: str) -> list[dict]:
        """List available studies in a TCIA collection."""
        resp = self._request("getPatientStudy", params={"Collection": collection})
        return resp.json()

    def get_series_for_study(self, study_uid: str) -> list[dict]:
        """List series within a study."""
        resp = self._request("getSeries", params={"StudyInstanceUID": study_uid})
        return resp.json()

    def download_series(self, series_uid: str, output_dir: str) -> list[str]:
        """Download all DICOM files for a series as a ZIP and extract."""
        os.makedirs(output_dir, exist_ok=True)
        resp = self._request("getImage", params={"SeriesInstanceUID": series_uid})
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            zf.extractall(output_dir)
        return [os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.endswith('.dcm')]

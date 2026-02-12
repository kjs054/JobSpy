from __future__ import annotations

import hashlib
import json
import math
import re
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup

from jobspy.ziprecruiter.constant import headers
from jobspy.util import (
    extract_emails_from_text,
    create_session,
    markdown_converter,
    remove_attributes,
    create_logger,
)
from jobspy.model import (
    JobPost,
    Compensation,
    Location,
    JobResponse,
    Country,
    DescriptionFormat,
    Scraper,
    ScraperInput,
    Site,
)
from jobspy.ziprecruiter.util import get_job_type_enum

log = create_logger("ZipRecruiter")

# Map ZR internal enum values to human-readable
_PAY_INTERVAL_MAP = {
    1: "hourly",
    2: "daily",
    3: "weekly",
    4: "biweekly",
    5: "monthly",
    6: "yearly",
    "PAY_INTERVAL_HOUR": "hourly",
    "PAY_INTERVAL_DAY": "daily",
    "PAY_INTERVAL_WEEK": "weekly",
    "PAY_INTERVAL_YEAR": "yearly",
}

_EMPLOYMENT_TYPE_MAP = {
    1: "fulltime",
    2: "parttime",
    3: "contract",
    4: "temporary",
    5: "internship",
    "EMPLOYMENT_TYPE_NAME_FULL_TIME": "fulltime",
    "EMPLOYMENT_TYPE_NAME_PART_TIME": "parttime",
    "EMPLOYMENT_TYPE_NAME_CONTRACT": "contract",
    "EMPLOYMENT_TYPE_NAME_TEMPORARY": "temporary",
    "EMPLOYMENT_TYPE_NAME_INTERNSHIP": "internship",
}


class ZipRecruiter(Scraper):
    base_url = "https://www.ziprecruiter.com"

    def __init__(
        self,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
    ):
        """
        Initializes ZipRecruiterScraper with web scraping approach.
        Scrapes the public web search page and detail pages.
        """
        super().__init__(Site.ZIP_RECRUITER, proxies=proxies)

        self.scraper_input = None
        self.session = create_session(proxies=proxies, ca_cert=ca_cert)
        self.session.headers.update(headers)

        self.delay = 5
        self.jobs_per_page = 20
        self.seen_urls = set()

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """
        Scrapes ZipRecruiter for jobs with scraper_input criteria.
        :param scraper_input: Information about job search criteria.
        :return: JobResponse containing a list of jobs.
        """
        self.scraper_input = scraper_input
        job_list: list[JobPost] = []

        max_pages = math.ceil(scraper_input.results_wanted / self.jobs_per_page)
        for page in range(1, max_pages + 1):
            if len(job_list) >= scraper_input.results_wanted:
                break
            if page > 1:
                time.sleep(self.delay)
            log.info(f"search page: {page} / {max_pages}")
            jobs = self._scrape_page(page)
            if jobs:
                job_list.extend(jobs)
            else:
                break
        return JobResponse(jobs=job_list[: scraper_input.results_wanted])

    def _scrape_page(self, page: int) -> list[JobPost]:
        """Scrape a single page of ZipRecruiter web search results."""
        params = {
            "search": self.scraper_input.search_term,
            "location": self.scraper_input.location or "",
        }
        if page > 1:
            params["page"] = page
        if self.scraper_input.distance:
            params["radius"] = self.scraper_input.distance
        if self.scraper_input.is_remote:
            params["refine_by_location_type"] = "only_remote"
        if self.scraper_input.hours_old:
            days = max(self.scraper_input.hours_old // 24, 1)
            params["days"] = days
        if self.scraper_input.easy_apply:
            params["zipapply"] = 1

        try:
            res = self.session.get(f"{self.base_url}/jobs-search", params=params)
            if not res.ok:
                if res.status_code == 403:
                    log.error(
                        "403 Forbidden - ZipRecruiter is blocking the request. "
                        "Try using a proxy."
                    )
                elif res.status_code == 429:
                    log.error("429 Too Many Requests - rate limited by ZipRecruiter")
                else:
                    log.error(f"ZipRecruiter response status code: {res.status_code}")
                return []
        except Exception as e:
            if "Proxy responded with" in str(e):
                log.error("ZipRecruiter: Bad proxy")
            else:
                log.error(f"ZipRecruiter: {str(e)}")
            return []

        soup = BeautifulSoup(res.text, "html.parser")
        app_json_tag = soup.find("script", type="application/json")
        if not app_json_tag or not app_json_tag.string:
            log.error("No application/json script tag found on ZipRecruiter page")
            return []

        try:
            page_data = json.loads(app_json_tag.string)
        except json.JSONDecodeError:
            log.error("Failed to parse ZipRecruiter page JSON")
            return []

        # Extract listing keys
        keys_resp = page_data.get("listJobKeysResponse", {})
        job_keys = keys_resp.get("jobKeys", [])
        if not job_keys:
            log.info("No job keys found on page")
            return []

        # First job has full details from the search page SSR
        first_details = page_data.get("getJobDetailsResponse", {}).get(
            "jobDetails", {}
        )
        first_key = first_details.get("listingKey") if first_details else None

        jobs = []

        # Parse the first job from SSR data
        if first_details and first_key:
            job = self._parse_zr_details(first_details, from_search_page=True)
            if job:
                jobs.append(job)

        # Fetch remaining jobs' detail pages in parallel
        remaining_keys = [
            k["listingKey"] for k in job_keys if k["listingKey"] != first_key
        ]
        if remaining_keys:
            with ThreadPoolExecutor(max_workers=min(10, len(remaining_keys))) as executor:
                futures = {
                    executor.submit(self._fetch_job_detail, key): key
                    for key in remaining_keys
                }
                for future in as_completed(futures):
                    try:
                        job = future.result()
                        if job:
                            jobs.append(job)
                    except Exception as e:
                        log.error(f"Failed to fetch job detail: {e}")

        return jobs

    def _fetch_job_detail(self, listing_key: str) -> JobPost | None:
        """Fetch and parse a single job's detail page."""
        job_url = f"{self.base_url}/jobs//j?lvk={listing_key}"
        try:
            res = self.session.get(job_url)
            if not res.ok:
                log.error(
                    f"Failed to fetch job detail for {listing_key}: {res.status_code}"
                )
                return None
        except Exception as e:
            log.error(f"Request failed for {listing_key}: {e}")
            return None

        # Extract job details from RSC payload
        details = self._extract_details_from_rsc(res.text)
        if details:
            return self._parse_zr_details(details, from_search_page=False)

        # Fallback: try application/json script tag
        soup = BeautifulSoup(res.text, "html.parser")
        app_json_tag = soup.find("script", type="application/json")
        if app_json_tag and app_json_tag.string:
            try:
                data = json.loads(app_json_tag.string)
                jd = data.get("getJobDetailsResponse", {}).get("jobDetails", {})
                if jd:
                    return self._parse_zr_details(jd, from_search_page=False)
            except json.JSONDecodeError:
                pass

        return None

    def _extract_details_from_rsc(self, html_text: str) -> dict | None:
        """Extract job details from React Server Component payload.

        ZipRecruiter embeds job data in self.__next_f.push() calls with
        escaped JSON containing __jobDetailsSerializedData.
        """
        marker = '__jobDetailsSerializedData\\":'
        idx = html_text.find(marker)
        if idx < 0:
            # Try unescaped variant
            marker = '__jobDetailsSerializedData":'
            idx = html_text.find(marker)
        if idx < 0:
            return None

        # Find "jobDetails": after the marker
        jd_marker = '\\"jobDetails\\":'
        jd_idx = html_text.find(jd_marker, idx)
        if jd_idx < 0:
            jd_marker = '"jobDetails":'
            jd_idx = html_text.find(jd_marker, idx)
        if jd_idx < 0:
            return None

        # Extract a chunk of text starting from jobDetails and unescape
        chunk_start = jd_idx + len(jd_marker)
        chunk = html_text[chunk_start : chunk_start + 50000]

        # Unescape the RSC string format
        unescaped = (
            chunk.replace('\\"', '"')
            .replace("\\n", "\n")
            .replace("\\t", "\t")
            .replace("\\\\", "\\")
        )

        # Bracket-match to extract the job details object
        if not unescaped.startswith("{"):
            obj_start = unescaped.find("{")
            if obj_start < 0:
                return None
            unescaped = unescaped[obj_start:]

        depth = 0
        in_str = False
        esc = False
        for i, c in enumerate(unescaped):
            if esc:
                esc = False
                continue
            if c == "\\" and in_str:
                esc = True
                continue
            if c == '"':
                in_str = not in_str
                continue
            if in_str:
                continue
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(unescaped[: i + 1])
                    except json.JSONDecodeError:
                        return None

        return None

    def _parse_zr_details(
        self, details: dict, from_search_page: bool = False
    ) -> JobPost | None:
        """Parse a ZipRecruiter job details dict into a JobPost."""
        title = details.get("title", "")
        listing_key = details.get("listingKey", "")
        if not title or not listing_key:
            return None

        job_url = f"{self.base_url}/jobs//j?lvk={listing_key}"
        if job_url in self.seen_urls:
            return None
        self.seen_urls.add(job_url)

        # Company
        company = details.get("company", {})
        company_name = company.get("name", "") if isinstance(company, dict) else ""

        # Location
        loc = details.get("location", {})
        if isinstance(loc, dict):
            city = loc.get("city", "")
            state = loc.get("stateCode") or loc.get("state", "")
            country_str = loc.get("countryCode") or loc.get("country", "")
        else:
            city = state = country_str = ""
        country_enum = Country.from_string(
            "usa" if country_str in ("US", "United States", "") else country_str.lower()
        )
        location = Location(city=city, state=state, country=country_enum)

        # Date posted
        status = details.get("status", {})
        date_posted = None
        posted_str = status.get("postedAtUtc") or status.get("rollingPostedAtUtc")
        if posted_str:
            try:
                date_posted = datetime.fromisoformat(
                    posted_str.replace("Z", "+00:00")
                ).date()
            except (ValueError, TypeError):
                pass

        # Pay / Compensation
        pay = details.get("pay", {})
        comp = None
        if isinstance(pay, dict) and (pay.get("min") or pay.get("max")):
            interval_raw = pay.get("interval", "")
            interval = _PAY_INTERVAL_MAP.get(interval_raw, str(interval_raw).lower())
            min_annual = pay.get("minAnnual") or pay.get("min")
            max_annual = pay.get("maxAnnual") or pay.get("max")
            comp = Compensation(
                interval=interval if interval != "hourly" else "yearly",
                min_amount=int(min_annual) if min_annual else None,
                max_amount=int(max_annual) if max_annual else None,
                currency="USD",
            )

        # Employment type
        emp_types = details.get("employmentTypes", [])
        job_type = None
        if emp_types and isinstance(emp_types, list):
            raw_type = emp_types[0].get("name", "")
            type_str = _EMPLOYMENT_TYPE_MAP.get(raw_type, str(raw_type).lower())
            job_type = get_job_type_enum(type_str.replace("_", ""))

        # Remote detection
        loc_types = details.get("locationTypes", [])
        is_remote = False
        if loc_types and isinstance(loc_types, list):
            for lt in loc_types:
                name = lt.get("name", "")
                if isinstance(name, str) and "REMOTE" in name.upper():
                    is_remote = True
                    break
                if isinstance(name, int) and name in (3, 4):
                    # 3 = remote required, 4 = remote optional
                    is_remote = True
                    break

        # Description
        description = details.get("htmlFullDescription", "")
        if isinstance(description, str) and description.startswith("$"):
            # RSC reference - description is loaded separately
            description = ""
        if (
            description
            and self.scraper_input
            and self.scraper_input.description_format == DescriptionFormat.MARKDOWN
        ):
            description = markdown_converter(description)

        return JobPost(
            id=f"zr-{listing_key}",
            title=title,
            company_name=company_name,
            location=location,
            job_type=job_type,
            compensation=comp,
            date_posted=date_posted,
            job_url=job_url,
            description=description,
            is_remote=is_remote,
            emails=extract_emails_from_text(description) if description else None,
        )

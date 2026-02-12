from __future__ import annotations

import re
import json
import time
import requests
from typing import Tuple
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from jobspy.glassdoor.constant import fallback_token, query_template, headers
from jobspy.glassdoor.util import (
    get_cursor_for_page,
    parse_compensation,
    parse_location,
)
from jobspy.util import (
    extract_emails_from_text,
    create_logger,
    create_session,
    markdown_converter,
)
from jobspy.exception import GlassdoorException
from jobspy.model import (
    JobPost,
    JobResponse,
    DescriptionFormat,
    Scraper,
    ScraperInput,
    Site,
)

log = create_logger("Glassdoor")


class Glassdoor(Scraper):
    def __init__(
        self, proxies: list[str] | str | None = None, ca_cert: str | None = None, user_agent: str | None = None
    ):
        """
        Initializes GlassdoorScraper with the Glassdoor job search url
        """
        site = Site(Site.GLASSDOOR)
        super().__init__(site, proxies=proxies, ca_cert=ca_cert, user_agent=user_agent)

        self.base_url = None
        self.country = None
        self.session = None
        self.scraper_input = None
        self.jobs_per_page = 30
        self.max_pages = 30
        self.seen_urls = set()

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """
        Scrapes Glassdoor for jobs with scraper_input criteria.
        :param scraper_input: Information about job search criteria.
        :return: JobResponse containing a list of jobs.
        """
        self.scraper_input = scraper_input
        self.scraper_input.results_wanted = min(900, scraper_input.results_wanted)
        self.base_url = self.scraper_input.country.get_glassdoor_url()

        self.session = create_session(
            proxies=self.proxies, ca_cert=self.ca_cert, has_retry=True
        )
        token = self._get_csrf_token()
        if not token:
            log.error("Failed to obtain CSRF token, Glassdoor scraping may fail")
        if token:
            headers["gd-csrf-token"] = token
        if self.user_agent:
            headers["user-agent"] = self.user_agent
        self.session.headers.update(headers)

        location_id, location_type = self._get_location(
            scraper_input.location, scraper_input.is_remote
        )
        if location_type is None:
            log.error("Glassdoor: location not parsed")
            return JobResponse(jobs=[])
        job_list: list[JobPost] = []
        cursor = None

        range_start = 1 + (scraper_input.offset // self.jobs_per_page)
        tot_pages = (scraper_input.results_wanted // self.jobs_per_page) + 2
        range_end = min(tot_pages, self.max_pages + 1)
        for page in range(range_start, range_end):
            log.info(f"search page: {page} / {range_end - 1}")
            try:
                jobs, cursor = self._fetch_jobs_page(
                    scraper_input, location_id, location_type, page, cursor
                )
                job_list.extend(jobs)
                if not jobs or len(job_list) >= scraper_input.results_wanted:
                    job_list = job_list[: scraper_input.results_wanted]
                    break
            except Exception as e:
                log.error(f"Glassdoor: {str(e)}")
                break
        return JobResponse(jobs=job_list)

    def _fetch_jobs_page(
        self,
        scraper_input: ScraperInput,
        location_id: int,
        location_type: str,
        page_num: int,
        cursor: str | None,
    ) -> Tuple[list[JobPost], str | None]:
        """
        Scrapes a page of Glassdoor for jobs with scraper_input criteria
        """
        jobs = []
        self.scraper_input = scraper_input
        try:
            payload = self._add_payload(location_id, location_type, page_num, cursor)
            response = self.session.post(
                f"{self.base_url}/graph",
                timeout_seconds=15,
                data=payload,
            )
            if response.status_code != 200:
                exc_msg = f"bad response status code: {response.status_code}"
                raise GlassdoorException(exc_msg)
            res_json = response.json()[0]
            if "errors" in res_json:
                raise ValueError("Error encountered in API response")
        except (
            requests.exceptions.ReadTimeout,
            GlassdoorException,
            ValueError,
            Exception,
        ) as e:
            log.error(f"Glassdoor: {str(e)}")
            return jobs, None

        jobs_data = res_json["data"]["jobListings"]["jobListings"]

        with ThreadPoolExecutor(max_workers=self.jobs_per_page) as executor:
            future_to_job_data = {
                executor.submit(self._process_job, job): job for job in jobs_data
            }
            for future in as_completed(future_to_job_data):
                try:
                    job_post = future.result()
                    if job_post:
                        jobs.append(job_post)
                except Exception as exc:
                    raise GlassdoorException(f"Glassdoor generated an exception: {exc}")

        return jobs, get_cursor_for_page(
            res_json["data"]["jobListings"]["paginationCursors"], page_num + 1
        )

    def _get_csrf_token(self):
        """
        Fetches CSRF token needed for API by visiting a generic page.
        Tries multiple URLs and regex patterns for resilience.
        """
        urls = [
            f"{self.base_url}/Job/computer-science-jobs.htm",
            f"{self.base_url}/Job/software-engineer-jobs.htm",
            f"{self.base_url}/",
        ]
        patterns = [
            r'"token":\s*"([^"]+)"',
            r'gdCSRFToken["\']\s*[:=]\s*["\']([^"\']+)',
            r'"gdToken":\s*"([^"]+)"',
            r'csrf[Tt]oken["\']\s*[:=]\s*["\']([^"\']+)',
        ]
        for url in urls:
            try:
                res = self.session.get(url)
                if not res.ok:
                    continue
                for pattern in patterns:
                    matches = re.findall(pattern, res.text)
                    if matches:
                        token = matches[0]
                        if isinstance(token, tuple):
                            token = token[-1]
                        if token and len(token) > 10:
                            return token
            except Exception as e:
                log.warning(f"Failed to fetch CSRF token from {url}: {e}")
                continue
        log.error("Could not extract CSRF token from Glassdoor")
        return None

    def _process_job(self, job_data):
        """
        Processes a single job and fetches its description.
        """
        job_id = job_data["jobview"]["job"]["listingId"]
        job_url = f"{self.base_url}job-listing/j?jl={job_id}"
        if job_url in self.seen_urls:
            return None
        self.seen_urls.add(job_url)
        job = job_data["jobview"]
        title = job["job"]["jobTitleText"]
        company_name = job["header"]["employerNameFromSearch"]
        company_id = job_data["jobview"]["header"]["employer"]["id"]
        location_name = job["header"].get("locationName", "")
        location_type = job["header"].get("locationType", "")
        age_in_days = job["header"].get("ageInDays")
        is_remote, location = False, None
        date_diff = (datetime.now() - timedelta(days=age_in_days)).date()
        date_posted = date_diff if age_in_days is not None else None

        if location_type == "S":
            is_remote = True
        else:
            location = parse_location(location_name)

        compensation = parse_compensation(job["header"])
        try:
            description = self._fetch_job_description(job_id)
        except:
            description = None
        company_url = f"{self.base_url}Overview/W-EI_IE{company_id}.htm"
        company_logo = (
            job_data["jobview"].get("overview", {}).get("squareLogoUrl", None)
        )
        listing_type = (
            job_data["jobview"]
            .get("header", {})
            .get("adOrderSponsorshipLevel", "")
            .lower()
        )
        return JobPost(
            id=f"gd-{job_id}",
            title=title,
            company_url=company_url if company_id else None,
            company_name=company_name,
            date_posted=date_posted,
            job_url=job_url,
            location=location,
            compensation=compensation,
            is_remote=is_remote,
            description=description,
            emails=extract_emails_from_text(description) if description else None,
            company_logo=company_logo,
            listing_type=listing_type,
        )

    def _fetch_job_description(self, job_id):
        """
        Fetches the job description for a single job ID.
        Routes through the TLS-aware session instead of raw requests
        to avoid Cloudflare blocks.
        """
        url = f"{self.base_url}/graph"
        body = [
            {
                "operationName": "JobDetailQuery",
                "variables": {
                    "jl": job_id,
                    "queryString": "q",
                    "pageTypeEnum": "SERP",
                },
                "query": """
                query JobDetailQuery($jl: Long!, $queryString: String, $pageTypeEnum: PageTypeEnum) {
                    jobview: jobView(
                        listingId: $jl
                        contextHolder: {queryString: $queryString, pageTypeEnum: $pageTypeEnum}
                    ) {
                        job {
                            description
                            __typename
                        }
                        __typename
                    }
                }
                """,
            }
        ]
        try:
            res = self.session.post(url, data=json.dumps(body))
            if res.status_code != 200:
                return None
            data = res.json()[0]
            desc = data["data"]["jobview"]["job"]["description"]
            if self.scraper_input.description_format == DescriptionFormat.MARKDOWN:
                desc = markdown_converter(desc)
            return desc
        except Exception as e:
            log.error(f"Failed to fetch job description for {job_id}: {e}")
            return None

    def _get_location(self, location: str, is_remote: bool) -> (int, str):
        if not location or is_remote:
            return "11047", "STATE"  # remote options
        url = f"{self.base_url}/findPopularLocationAjax.htm?maxLocationsToReturn=10&term={location}"
        max_retries = 3
        for attempt in range(max_retries):
            try:
                res = self.session.get(url)
                if res.status_code == 200:
                    items = res.json()
                    if not items:
                        raise ValueError(
                            f"Location '{location}' not found on Glassdoor"
                        )
                    loc_type = items[0]["locationType"]
                    loc_type_map = {"C": "CITY", "S": "STATE", "N": "COUNTRY"}
                    location_type = loc_type_map.get(loc_type, loc_type)
                    return int(items[0]["locationId"]), location_type

                if res.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    log.warning(
                        f"429 rate limit on Glassdoor location lookup, "
                        f"retrying in {wait}s ({attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait)
                    continue

                log.error(
                    f"Glassdoor location lookup failed: {res.status_code} - {res.text}"
                )
                return None, None
            except ValueError:
                raise
            except Exception as e:
                log.error(f"Glassdoor location lookup error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None, None
        log.error("Glassdoor location lookup: max retries exceeded")
        return None, None

    def _add_payload(
        self,
        location_id: int,
        location_type: str,
        page_num: int,
        cursor: str | None = None,
    ) -> str:
        fromage = None
        if self.scraper_input.hours_old:
            fromage = max(self.scraper_input.hours_old // 24, 1)
        filter_params = []
        if self.scraper_input.easy_apply:
            filter_params.append({"filterKey": "applicationType", "values": "1"})
        if fromage:
            filter_params.append({"filterKey": "fromAge", "values": str(fromage)})
        payload = {
            "operationName": "JobSearchResultsQuery",
            "variables": {
                "excludeJobListingIds": [],
                "filterParams": filter_params,
                "keyword": self.scraper_input.search_term,
                "numJobsToShow": 30,
                "locationType": location_type,
                "locationId": int(location_id),
                "parameterUrlInput": f"IL.0,12_I{location_type}{location_id}",
                "pageNumber": page_num,
                "pageCursor": cursor,
                "fromage": fromage,
                "sort": "date",
            },
            "query": query_template,
        }
        if self.scraper_input.job_type:
            payload["variables"]["filterParams"].append(
                {"filterKey": "jobType", "values": self.scraper_input.job_type.value[0]}
            )
        return json.dumps([payload])

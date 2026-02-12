from __future__ import annotations

import hashlib
import math
import re
import json
from typing import Tuple
from datetime import datetime, timedelta

from jobspy.google.constant import headers_jobs, headers_initial, async_param_fallback
from jobspy.model import (
    Scraper,
    ScraperInput,
    Site,
    JobPost,
    JobResponse,
    Location,
    JobType,
)
from jobspy.util import extract_emails_from_text, extract_job_type, create_session
from jobspy.google.util import log, find_job_info_initial_page, find_job_info


class Google(Scraper):
    def __init__(
        self, proxies: list[str] | str | None = None, ca_cert: str | None = None, user_agent: str | None = None
    ):
        """
        Initializes Google Scraper with the Google jobs search url
        """
        site = Site(Site.GOOGLE)
        super().__init__(site, proxies=proxies, ca_cert=ca_cert)

        self.country = None
        self.session = None
        self.scraper_input = None
        self.jobs_per_page = 10
        self.seen_urls = set()
        self.url = "https://www.google.com/search"
        self.jobs_url = "https://www.google.com/async/callback:550"
        self._async_param = None

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """
        Scrapes Google for jobs with scraper_input criteria.
        :param scraper_input: Information about job search criteria.
        :return: JobResponse containing a list of jobs.
        """
        self.scraper_input = scraper_input
        self.scraper_input.results_wanted = min(900, scraper_input.results_wanted)

        self.session = create_session(
            proxies=self.proxies, ca_cert=self.ca_cert, is_tls=False, has_retry=True
        )
        forward_cursor, job_list = self._get_initial_cursor_and_jobs()
        if forward_cursor is None:
            log.warning(
                "initial cursor not found, try changing your query or there was at most 10 results"
            )
            return JobResponse(jobs=job_list)

        page = 1

        while (
            len(self.seen_urls) < scraper_input.results_wanted + scraper_input.offset
            and forward_cursor
        ):
            log.info(
                f"search page: {page} / {math.ceil(scraper_input.results_wanted / self.jobs_per_page)}"
            )
            try:
                jobs, forward_cursor = self._get_jobs_next_page(forward_cursor)
            except Exception as e:
                log.error(f"failed to get jobs on page: {page}, {e}")
                break
            if not jobs:
                log.info(f"found no jobs on page: {page}")
                break
            job_list += jobs
            page += 1
        return JobResponse(
            jobs=job_list[
                scraper_input.offset : scraper_input.offset
                + scraper_input.results_wanted
            ]
        )

    def _get_initial_cursor_and_jobs(self) -> Tuple[str, list[JobPost]]:
        """Gets initial cursor and jobs to paginate through job listings"""
        query = f"{self.scraper_input.search_term} jobs"

        def get_time_range(hours_old):
            if hours_old <= 24:
                return "since yesterday"
            elif hours_old <= 72:
                return "in the last 3 days"
            elif hours_old <= 168:
                return "in the last week"
            else:
                return "in the last month"

        job_type_mapping = {
            JobType.FULL_TIME: "Full time",
            JobType.PART_TIME: "Part time",
            JobType.INTERNSHIP: "Internship",
            JobType.CONTRACT: "Contract",
        }

        if self.scraper_input.job_type in job_type_mapping:
            query += f" {job_type_mapping[self.scraper_input.job_type]}"

        if self.scraper_input.location:
            query += f" near {self.scraper_input.location}"

        if self.scraper_input.hours_old:
            time_filter = get_time_range(self.scraper_input.hours_old)
            query += f" {time_filter}"

        if self.scraper_input.is_remote:
            query += " remote"

        if self.scraper_input.google_search_term:
            query = self.scraper_input.google_search_term

        params = {"q": query, "udm": "8"}
        response = self.session.get(self.url, headers=headers_initial, params=params)

        # Extract cursor with multiple fallback patterns
        data_async_fc = None
        cursor_patterns = [
            r'<div jsname="Yust4d"[^>]+data-async-fc="([^"]+)"',
            r'data-async-fc="([^"]+)"',
            r'"data-async-fc":\s*"([^"]+)"',
        ]
        for pattern in cursor_patterns:
            match = re.search(pattern, response.text)
            if match:
                data_async_fc = match.group(1)
                break

        # Extract async_param for pagination from this page
        self._async_param = self._extract_async_param(response.text)

        jobs_raw = find_job_info_initial_page(response.text)
        jobs = []
        for job_raw in jobs_raw:
            job_post = self._parse_job(job_raw)
            if job_post:
                jobs.append(job_post)
        return data_async_fc, jobs

    def _extract_async_param(self, html_text):
        """Extract the async callback parameter from the initial page HTML.

        This parameter contains references to Google's current JS/CSS bundles
        and changes with each frontend deployment. Dynamic extraction avoids
        the hardcoded value going stale.
        """
        # Method 1: Find full async param string in data attributes
        match = re.search(r'data-async="([^"]*_basejs:[^"]+)"', html_text)
        if match:
            return match.group(1)

        # Method 2: Find it in script blocks or inline config
        match = re.search(r'"(_basejs:/xjs/_/js/[^"]+)"', html_text)
        if match:
            return match.group(1)

        # Method 3: Construct from individual basejs + basecss URLs found in page
        basejs = re.search(r'["\'](/xjs/_/js/k=[^"\']+)["\']', html_text)
        basecss = re.search(r'["\'](/xjs/_/ss/k=[^"\']+)["\']', html_text)
        if basejs and basecss:
            param = f"_basejs:{basejs.group(1)},_basecss:{basecss.group(1)},_fmt:prog"
            log.info("Constructed async_param from page basejs/basecss URLs")
            return param

        # Method 4: Fall back to the hardcoded value (may be stale)
        log.warning("Could not extract async_param dynamically, using fallback")
        return async_param_fallback

    def _get_jobs_next_page(self, forward_cursor: str) -> Tuple[list[JobPost], str]:
        async_param = self._async_param or async_param_fallback
        params = {"fc": [forward_cursor], "fcv": ["3"], "async": [async_param]}
        response = self.session.get(self.jobs_url, headers=headers_jobs, params=params)
        return self._parse_jobs(response.text)

    def _parse_jobs(self, job_data: str) -> Tuple[list[JobPost], str]:
        """
        Parses jobs on a page with next page cursor
        """
        try:
            start_idx = job_data.find("[[[")
            if start_idx == -1:
                log.error("No job data found in pagination response")
                return [], None
            end_idx = job_data.rindex("]]]") + 3
            s = job_data[start_idx:end_idx]
            parsed = json.loads(s)[0]
        except (ValueError, json.JSONDecodeError, IndexError) as e:
            log.error(f"Failed to parse pagination response: {e}")
            return [], None

        # Extract next-page cursor with fallback patterns
        data_async_fc = None
        for pattern in [r'data-async-fc="([^"]+)"', r'"fc":"([^"]+)"']:
            match = re.search(pattern, job_data)
            if match:
                data_async_fc = match.group(1)
                break

        jobs_on_page = []
        for array in parsed:
            try:
                _, job_data_str = array
                if not isinstance(job_data_str, str) or not job_data_str.startswith("[[["):
                    continue
                job_d = json.loads(job_data_str)
                job_info = find_job_info(job_d)
                if job_info:
                    job_post = self._parse_job(job_info)
                    if job_post:
                        jobs_on_page.append(job_post)
            except (ValueError, json.JSONDecodeError) as e:
                log.error(f"Failed to parse job entry: {e}")
                continue
        return jobs_on_page, data_async_fc

    def _parse_job(self, job_info: list):
        if not job_info or not isinstance(job_info, list) or len(job_info) < 4:
            return None

        try:
            # Extract URL from nested structure
            job_url = None
            if isinstance(job_info[3], list) and job_info[3]:
                if isinstance(job_info[3][0], list) and job_info[3][0]:
                    job_url = job_info[3][0][0]

            if not job_url or not isinstance(job_url, str) or not job_url.startswith("http"):
                return None
            if job_url in self.seen_urls:
                return None
            self.seen_urls.add(job_url)

            title = str(job_info[0]) if job_info[0] else ""
            company_name = str(job_info[1]) if len(job_info) > 1 and job_info[1] else ""
            location = city = str(job_info[2]) if len(job_info) > 2 and job_info[2] else None
            state = country = date_posted = None
            if location and "," in location:
                city, state, *country = [x.strip() for x in location.split(",")]

            days_ago_str = job_info[12] if len(job_info) > 12 else None
            if isinstance(days_ago_str, str):
                match = re.search(r"\d+", days_ago_str)
                days_ago = int(match.group()) if match else None
                if days_ago is not None:
                    date_posted = (datetime.now() - timedelta(days=days_ago)).date()

            description = str(job_info[19]) if len(job_info) > 19 and job_info[19] else ""

            # ID with safe fallback
            job_id = None
            if len(job_info) > 28 and job_info[28]:
                job_id = str(job_info[28])
            if not job_id:
                job_id = hashlib.sha256(job_url.encode()).hexdigest()[:12]

            job_post = JobPost(
                id=f"go-{job_id}",
                title=title,
                company_name=company_name,
                location=Location(
                    city=city, state=state, country=country[0] if country else None
                ),
                job_url=job_url,
                date_posted=date_posted,
                is_remote=(
                    ("remote" in description.lower() or "wfh" in description.lower())
                    if description
                    else False
                ),
                description=description,
                emails=extract_emails_from_text(description) if description else None,
                job_type=extract_job_type(description) if description else None,
            )
            return job_post
        except Exception as e:
            log.error(f"Failed to parse Google job: {e}")
            return None

from __future__ import annotations

import json
import math
import threading
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode

from bs4 import BeautifulSoup

from jobspy.ziprecruiter.constant import headers
from jobspy.util import (
    extract_emails_from_text,
    create_session,
    markdown_converter,
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
    "PAY_INTERVAL_MONTH": "monthly",
    "PAY_INTERVAL_YEAR": "yearly",
    "pay_interval_hour": "hourly",
    "pay_interval_day": "daily",
    "pay_interval_week": "weekly",
    "pay_interval_month": "monthly",
    "pay_interval_year": "yearly",
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
        self.blocked = False

        # Playwright fallback state (lazy-initialized)
        self._playwright = None
        self._browser = None
        self._browser_context = None
        self._use_browser = False
        self._browser_lock = threading.Lock()

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """
        Scrapes ZipRecruiter for jobs with scraper_input criteria.
        :param scraper_input: Information about job search criteria.
        :return: JobResponse containing a list of jobs.
        """
        self.scraper_input = scraper_input
        job_list: list[JobPost] = []

        try:
            max_pages = math.ceil(scraper_input.results_wanted / self.jobs_per_page)
            for page in range(1, max_pages + 1):
                if len(job_list) >= scraper_input.results_wanted:
                    break
                if self.blocked:
                    break
                if page > 1:
                    time.sleep(self.delay)
                log.info(f"search page: {page} / {max_pages}")
                jobs = self._scrape_page(page)
                if jobs:
                    job_list.extend(jobs)
                else:
                    break
        finally:
            self._cleanup_browser()

        return JobResponse(
            jobs=job_list[: scraper_input.results_wanted],
            blocked=self.blocked,
        )

    def _build_search_params(self, page: int) -> dict:
        """Build query params for a ZipRecruiter search page."""
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
        return params

    def _scrape_page(self, page: int) -> list[JobPost]:
        """Scrape a single page of ZipRecruiter web search results."""
        params = self._build_search_params(page)
        html = None

        if self._use_browser:
            # Already know TLS is blocked — go straight to Playwright
            html = self._fetch_with_browser(
                f"{self.base_url}/jobs-search", params
            )
            if html is None:
                self.blocked = True
                return []
        else:
            try:
                res = self.session.get(
                    f"{self.base_url}/jobs-search", params=params,
                    timeout_seconds=15,
                )
                if self._detect_cloudflare(res):
                    log.info(
                        "Cloudflare challenge detected, falling back to Playwright"
                    )
                    html = self._fetch_with_browser(
                        f"{self.base_url}/jobs-search", params
                    )
                    if html is None:
                        self.blocked = True
                        return []
                    self._transfer_cookies_to_session()
                    self._use_browser = True
                elif not res.ok:
                    if res.status_code == 429:
                        log.error(
                            "429 Too Many Requests - rate limited by ZipRecruiter"
                        )
                        self.blocked = True
                    else:
                        log.error(
                            f"ZipRecruiter response status code: {res.status_code}"
                        )
                    return []
                else:
                    html = res.text
            except Exception as e:
                if "Proxy responded with" in str(e):
                    log.error("ZipRecruiter: Bad proxy")
                else:
                    log.error(f"ZipRecruiter: {str(e)}")
                return []

        soup = BeautifulSoup(html, "html.parser")

        # Try legacy application/json script tag first, then RSC payload
        page_data = None
        app_json_tag = soup.find("script", type="application/json")
        if app_json_tag and app_json_tag.string:
            try:
                page_data = json.loads(app_json_tag.string)
            except json.JSONDecodeError:
                pass

        if page_data:
            # Legacy format: listJobKeysResponse + getJobDetailsResponse
            return self._parse_legacy_search_page(page_data, html)

        # New format: RSC payload with serializedJobCardsData
        cards_data = self._extract_cards_from_rsc(html)
        if cards_data:
            return self._parse_rsc_search_page(cards_data)

        log.error("No job data found on ZipRecruiter search page")
        return []

    def _parse_legacy_search_page(self, page_data: dict, html: str) -> list[JobPost]:
        """Parse the legacy application/json format (listJobKeysResponse)."""
        keys_resp = page_data.get("listJobKeysResponse", {})
        job_keys = keys_resp.get("jobKeys", [])
        if not job_keys:
            log.info("No job keys found on page")
            return []

        first_details = page_data.get("getJobDetailsResponse", {}).get(
            "jobDetails", {}
        )
        first_key = first_details.get("listingKey") if first_details else None

        jobs = []
        if first_details and first_key:
            job = self._parse_zr_details(first_details)
            if job:
                if not job.company_url:
                    job.company_url = self._extract_company_website_from_html(html)
                jobs.append(job)

        remaining_keys = [
            k["listingKey"] for k in job_keys if k["listingKey"] != first_key
        ]
        if remaining_keys:
            with ThreadPoolExecutor(max_workers=min(3, len(remaining_keys))) as executor:
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

    def _extract_cards_from_rsc(self, html_text: str) -> dict | None:
        """Extract serializedJobCardsData from RSC self.__next_f.push() payload."""
        soup = BeautifulSoup(html_text, "html.parser")
        for script in soup.find_all("script"):
            text = script.string
            if not text or "serializedJobCardsData" not in text:
                continue

            # Extract the string payload from self.__next_f.push([1,"..."])
            import re
            match = re.search(
                r'self\.__next_f\.push\(\[1,"(.*)"\]\)', text, re.DOTALL
            )
            if not match:
                continue

            try:
                decoded = json.loads('"' + match.group(1) + '"')
            except (json.JSONDecodeError, ValueError):
                continue

            # Bracket-match serializedJobCardsData:{...}
            marker = '"serializedJobCardsData":'
            idx = decoded.find(marker)
            if idx < 0:
                continue

            start = decoded.find("{", idx + len(marker))
            if start < 0:
                continue

            depth = 0
            for i in range(start, len(decoded)):
                if decoded[i] == "{":
                    depth += 1
                elif decoded[i] == "}":
                    depth -= 1
                    if depth == 0:
                        try:
                            return json.loads(decoded[start : i + 1])
                        except json.JSONDecodeError:
                            break

        return None

    def _parse_rsc_search_page(self, cards_data: dict) -> list[JobPost]:
        """Parse jobs from the RSC serializedJobCardsData format.

        The card data includes title, company, location, pay, employment type,
        and posted date — everything except the full description. We use
        shortDescription as a lightweight fallback rather than fetching each
        detail page (which are Cloudflare-protected and cause timeouts).
        """
        job_keys = cards_data.get("jobKeys", [])
        job_map = cards_data.get("jobKeysMap", {})

        if not job_keys or not job_map:
            log.info("No job data in RSC payload")
            return []

        log.info(f"Found {len(job_keys)} jobs in RSC payload")

        jobs = []
        for key_entry in job_keys:
            listing_key = key_entry.get("listingKey", "")
            details = job_map.get(listing_key)
            if not details:
                continue

            job = self._parse_zr_details(details)
            if job:
                jobs.append(job)

        return jobs

    def _fetch_job_detail(self, listing_key: str) -> JobPost | None:
        """Fetch and parse a single job's detail page."""
        job_url = f"{self.base_url}/jobs//j?lvk={listing_key}"
        html = None

        try:
            res = self.session.get(job_url, timeout_seconds=15)
            if self._detect_cloudflare(res):
                html = self._fetch_with_browser(job_url, {})
                if html is None:
                    return None
            elif not res.ok:
                log.error(
                    f"Failed to fetch job detail for {listing_key}: {res.status_code}"
                )
                return None
            else:
                html = res.text
        except Exception as e:
            log.error(f"Request failed for {listing_key}: {e}")
            return None

        # Extract company website URL and description from the detail page HTML
        company_website_url = self._extract_company_website_from_html(html)
        html_description = self._extract_description_from_html(html)

        # Convert HTML description to markdown if that format is requested
        if (
            html_description
            and self.scraper_input
            and self.scraper_input.description_format == DescriptionFormat.MARKDOWN
        ):
            html_description = markdown_converter(html_description)

        # Extract job details from RSC payload
        details = self._extract_details_from_rsc(html)
        if details:
            job = self._parse_zr_details(details)
            if job:
                if company_website_url and not job.company_url:
                    job.company_url = company_website_url
                if html_description and not job.description:
                    job.description = html_description
            return job

        # Fallback: try application/json script tag
        soup = BeautifulSoup(html, "html.parser")
        app_json_tag = soup.find("script", type="application/json")
        if app_json_tag and app_json_tag.string:
            try:
                data = json.loads(app_json_tag.string)
                jd = data.get("getJobDetailsResponse", {}).get("jobDetails", {})
                if jd:
                    job = self._parse_zr_details(jd)
                    if job:
                        if company_website_url and not job.company_url:
                            job.company_url = company_website_url
                        if html_description and not job.description:
                            job.description = html_description
                    return job
            except json.JSONDecodeError:
                pass

        return None

    # ── Playwright fallback helpers ──────────────────────────────────

    @staticmethod
    def _detect_cloudflare(res) -> bool:
        """Return True if the response looks like a Cloudflare JS challenge."""
        if res.status_code == 403:
            body = res.text[:2000] if hasattr(res, "text") else ""
            if "Just a moment" in body or "cf-browser-verification" in body:
                return True
            # Some 403s from Cloudflare don't have the challenge text
            # but still indicate blocking
            return True
        return False

    def _ensure_browser(self):
        """Lazily launch Playwright headless Chromium."""
        if self._browser_context is not None:
            return

        from playwright.sync_api import sync_playwright

        log.info("Launching Playwright browser for Cloudflare bypass")
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=True)
        self._browser_context = self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=headers.get(
                "user-agent",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/132.0.0.0 Safari/537.36",
            ),
        )

    def _fetch_with_browser(self, url: str, params: dict) -> str | None:
        """Fetch a page using Playwright, waiting for Cloudflare to resolve.

        Thread-safe: serialized via self._browser_lock since Playwright's
        sync API is not safe for concurrent use from multiple threads.
        """
        with self._browser_lock:
            try:
                self._ensure_browser()
            except Exception as e:
                log.error(f"Failed to launch Playwright: {e}")
                return None

            full_url = url
            if params:
                full_url = f"{url}?{urlencode(params)}"

            page = self._browser_context.new_page()
            try:
                page.goto(full_url, wait_until="domcontentloaded", timeout=30000)

                # Wait for Cloudflare challenge to resolve (title changes from
                # "Just a moment..." to the actual page title)
                try:
                    page.wait_for_function(
                        "() => !document.title.includes('Just a moment')",
                        timeout=15000,
                    )
                except Exception:
                    # May already be past the challenge or title differs
                    pass

                # Give the page a moment to finish rendering
                page.wait_for_load_state("networkidle", timeout=10000)
                html = page.content()
                log.info(
                    f"Playwright fetched {len(html)} bytes from {full_url[:80]}"
                )
                return html
            except Exception as e:
                log.error(f"Playwright fetch failed for {full_url[:80]}: {e}")
                return None
            finally:
                page.close()

    def _transfer_cookies_to_session(self):
        """Transfer Playwright cookies to the TLS client session."""
        if self._browser_context is None:
            return

        cookies = self._browser_context.cookies()
        for cookie in cookies:
            self.session.cookies.set(
                cookie["name"],
                cookie["value"],
                domain=cookie.get("domain", ""),
                path=cookie.get("path", "/"),
            )
        log.info(f"Transferred {len(cookies)} cookies to TLS session")

    def _cleanup_browser(self):
        """Close Playwright resources if they were initialized."""
        if self._browser_context is not None:
            try:
                self._browser_context.close()
            except Exception:
                pass
            self._browser_context = None
        if self._browser is not None:
            try:
                self._browser.close()
            except Exception:
                pass
            self._browser = None
        if self._playwright is not None:
            try:
                self._playwright.stop()
            except Exception:
                pass
            self._playwright = None

    @staticmethod
    def _extract_company_website_from_html(html_text: str) -> str | None:
        """Extract the company website URL from a ZR job detail page.

        ZR detail pages include an external link to the company website with
        target="_blank" and referrerpolicy="no-referrer".  The link text is
        typically the bare domain (e.g. "macys.com").
        """
        soup = BeautifulSoup(html_text, "html.parser")
        for a_tag in soup.find_all(
            "a", attrs={"target": "_blank", "referrerpolicy": "no-referrer"}
        ):
            href = (a_tag.get("href") or "").strip()
            if not href:
                continue
            # Skip ZR internal links and non-http links
            if "ziprecruiter.com" in href:
                continue
            if href.startswith("http"):
                return href
        return None

    @staticmethod
    def _extract_description_from_html(html_text: str) -> str | None:
        """Extract the job description from a ZR detail page's rendered HTML.

        The RSC JSON often has htmlFullDescription as a "$" reference, but the
        actual description is rendered in the DOM.  This method finds it by
        looking for the largest rich-text content block on the page.
        """
        import re as _re

        soup = BeautifulSoup(html_text, "html.parser")

        # Remove nav, header, footer, script, style elements to avoid noise
        for tag in soup.find_all(
            ["nav", "header", "footer", "script", "style", "noscript"]
        ):
            tag.decompose()

        # Strategy 1: Find a container whose text includes "Job Description"
        # or "Full Job Description" heading, then take the sibling content.
        # ZR often has a heading followed by the description div.

        # Strategy 2: Find the largest div containing rich content (multiple
        # <p>, <li>, <strong>, <br> tags). The description is almost always
        # the densest block of formatted content on the page.
        best_container = None
        best_score = 0

        for div in soup.find_all("div"):
            # Count rich content indicators
            p_tags = div.find_all("p", recursive=False)
            li_tags = div.find_all("li")
            br_tags = div.find_all("br")

            # Must have meaningful content
            text_len = len(div.get_text(strip=True))
            if text_len < 100:
                continue

            # Score based on rich content density
            score = (
                len(p_tags) * 3
                + len(li_tags) * 2
                + len(br_tags)
                + text_len // 50
            )

            # Penalize very large containers (likely page-level wrappers)
            nested_divs = len(div.find_all("div"))
            if nested_divs > 20:
                score = score // 3

            # Boost containers that look like description content
            div_text = div.get_text()[:200].lower()
            if any(kw in div_text for kw in [
                "responsibilities", "qualifications", "requirements",
                "about the role", "what you", "job summary",
                "position summary", "essential duties",
            ]):
                score *= 2

            if score > best_score:
                best_score = score
                best_container = div

        if best_container and best_score >= 5:
            # Return the inner HTML of the description container
            html_content = best_container.decode_contents()
            # Basic cleanup: collapse excessive whitespace
            html_content = _re.sub(r"\n{3,}", "\n\n", html_content).strip()
            if len(html_content) > 50:
                return html_content

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

    def _parse_zr_details(self, details: dict) -> JobPost | None:
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

        # Extract company URL if available in the response
        company_url = None
        if isinstance(company, dict):
            company_url = (
                company.get("websiteUrl")
                or company.get("website")
                or company.get("url")
                or company.get("companyUrl")
            )
            # ZR profile URL as fallback (filtered downstream by domain extraction)
            if not company_url and company.get("id"):
                company_url = f"{self.base_url}/c/{company['id']}"

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

        # Description — prefer full HTML, fall back to shortDescription
        description = details.get("htmlFullDescription", "")
        if isinstance(description, str) and description.startswith("$"):
            # RSC reference - description is loaded separately
            description = ""
        if not description:
            description = details.get("shortDescription", "")
        if (
            description
            and self.scraper_input
            and self.scraper_input.description_format == DescriptionFormat.MARKDOWN
        ):
            description = markdown_converter(description)

        # Prefer SEO URL (canonical /c/Company/Job/Title/-in-Location?jid=...)
        seo_path = details.get("rawCanonicalZipJobPageUrl", "")
        if seo_path:
            job_url = f"{self.base_url}{seo_path}"

        return JobPost(
            id=f"zr-{listing_key}",
            title=title,
            company_name=company_name,
            company_url=company_url,
            location=location,
            job_type=job_type,
            compensation=comp,
            date_posted=date_posted,
            job_url=job_url,
            description=description,
            is_remote=is_remote,
            emails=extract_emails_from_text(description) if description else None,
        )

import re
from datetime import date, datetime, timedelta

from bs4 import BeautifulSoup

from jobspy.model import JobType, Location
from jobspy.util import get_enum_from_job_type


def job_type_code(job_type_enum: JobType) -> str:
    return {
        JobType.FULL_TIME: "F",
        JobType.PART_TIME: "P",
        JobType.INTERNSHIP: "I",
        JobType.CONTRACT: "C",
        JobType.TEMPORARY: "T",
    }.get(job_type_enum, "")


def parse_job_type(soup_job_type: BeautifulSoup) -> list[JobType] | None:
    """
    Gets the job type from job page
    :param soup_job_type:
    :return: JobType
    """
    h3_tag = soup_job_type.find(
        "h3",
        class_="description__job-criteria-subheader",
        string=lambda text: "Employment type" in text,
    )
    employment_type = None
    if h3_tag:
        employment_type_span = h3_tag.find_next_sibling(
            "span",
            class_="description__job-criteria-text description__job-criteria-text--criteria",
        )
        if employment_type_span:
            employment_type = employment_type_span.get_text(strip=True)
            employment_type = employment_type.lower()
            employment_type = employment_type.replace("-", "")

    return [get_enum_from_job_type(employment_type)] if employment_type else []


def parse_job_level(soup_job_level: BeautifulSoup) -> str | None:
    """
    Gets the job level from job page
    :param soup_job_level:
    :return: str
    """
    h3_tag = soup_job_level.find(
        "h3",
        class_="description__job-criteria-subheader",
        string=lambda text: "Seniority level" in text,
    )
    job_level = None
    if h3_tag:
        job_level_span = h3_tag.find_next_sibling(
            "span",
            class_="description__job-criteria-text description__job-criteria-text--criteria",
        )
        if job_level_span:
            job_level = job_level_span.get_text(strip=True)

    return job_level


def parse_company_industry(soup_industry: BeautifulSoup) -> str | None:
    """
    Gets the company industry from job page
    :param soup_industry:
    :return: str
    """
    h3_tag = soup_industry.find(
        "h3",
        class_="description__job-criteria-subheader",
        string=lambda text: "Industries" in text,
    )
    industry = None
    if h3_tag:
        industry_span = h3_tag.find_next_sibling(
            "span",
            class_="description__job-criteria-text description__job-criteria-text--criteria",
        )
        if industry_span:
            industry = industry_span.get_text(strip=True)

    return industry


def parse_relative_date(text: str) -> date | None:
    """
    Parses relative date text like '2 weeks ago' or 'Reposted 3 days ago'
    into an approximate absolute datetime.
    """
    if not text:
        return None
    text = text.strip().lower()
    # Strip "reposted" prefix
    text = re.sub(r"^reposted\s+", "", text)

    match = re.search(r"(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago", text)
    if not match:
        return None

    value = int(match.group(1))
    unit = match.group(2)
    now = datetime.now()

    deltas = {
        "second": timedelta(seconds=value),
        "minute": timedelta(minutes=value),
        "hour": timedelta(hours=value),
        "day": timedelta(days=value),
        "week": timedelta(weeks=value),
        "month": timedelta(days=value * 30),
        "year": timedelta(days=value * 365),
    }
    delta = deltas.get(unit)
    if delta:
        return (now - delta).date()
    return None


def parse_posted_date(soup: BeautifulSoup) -> date | None:
    """
    Extracts posted date from LinkedIn job detail page.
    Looks for the 'posted-time-ago__text' span with relative date text.
    """
    time_tag = soup.find("span", class_=lambda c: c and "posted-time-ago__text" in c)
    if time_tag:
        return parse_relative_date(time_tag.get_text(strip=True))
    return None


def is_job_remote(title: dict, description: str, location: Location) -> bool:
    """
    Searches the title, location, and description to check if job is remote
    """
    remote_keywords = ["remote", "work from home", "wfh"]
    location = location.display_location()
    full_string = f'{title} {description} {location}'.lower()
    is_remote = any(keyword in full_string for keyword in remote_keywords)
    return is_remote

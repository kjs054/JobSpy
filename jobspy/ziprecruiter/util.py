from jobspy.model import JobType


def get_job_type_enum(job_type_str: str) -> list[JobType] | None:
    for job_type in JobType:
        if job_type_str in job_type.value:
            return [job_type]
    return None

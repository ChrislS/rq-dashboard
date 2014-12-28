from rq import Queue as BaseQueue
from rq.job import Job, Status, requeue_job as base_requeue_job
from rq.compat import as_text, decode_redis_hash


def requeue_job(job_id, connection=None):
    "Reques a job whether it's failed or not."
    try: # if failed
        base_requeue_job(job_id)
    except: # InvalidJobOperationError
        # if started but not in the queue
        job = Job.fetch(job_id, connection=connection)
        job.set_status(Status.QUEUED)
        job.ended_at = None
        job.exc_info = None
        q = Queue(job.origin, connection=connection)
        q.enqueue_job(job)

class Queue(BaseQueue):
    "Queue containing started and finished jobs as well."

    redis_job_namespace_prefix = 'rq:job:'

    def _get_job_ids(self, sort=True, statuses=[Status.QUEUED, Status.STARTED, Status.FINISHED]):
        jobs = []
        keys = self.connection.keys(self.redis_job_namespace_prefix + '*')
        for key in keys:
            fields = ['origin', 'status', 'enqueued_at', 'ended_at']
            job = dict(zip(
                fields, decode_redis_hash(self.connection.hmget(key, fields))))
            if job.get('origin') == self.name and job.get('status') in statuses:
                job['id'] = key[len(self.redis_job_namespace_prefix):]
                jobs.append(job)
        if sort: # criteria
            jobs.sort(key=lambda j: j.get('ended_at') or j.get('enqueued_at'), 
                reverse=True)
        return [j['id'] for j in jobs]

    @property
    def count(self):
        if self.name == 'failed':
            return self.connection.llen(self.key)
        else:
            job_ids = self._get_job_ids(sort=False)
            return len(job_ids)

    def get_job_ids(self, offset=0, length=-1):
        start = offset
        if length >= 0:
            end = offset + (length - 1)
        else:
            end = length
        if self.name == 'failed':
            job_ids = self.connection.lrange(self.key, start, end)
        else:
            job_ids = self._get_job_ids()[start:end] # sorted
        return job_ids


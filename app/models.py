import json

from django.db import models
from django.utils.html import format_html
from django.conf import settings
from django.dispatch import receiver
from django.db.models.signals import post_save
from .util.helper import start_background_job, send_sighup_signal, kill_process


JOB_STATUS = (
    ("NEW", "New"),
    ("RUNNING", "Running"),
    ("COMPLETED", "Completed"),
    ("ERROR", "Error"),
    ("RESTART", "Restart"),
)

PROCESS_STATUS = (
    ("RUNNING", "Running"),
    ("KILLED", "Killed"),
)


def formatted_td_data(val):
    if isinstance(val, list):
        return "".join(["<li>{}</li>".format(v) for v in val])
    return val


class JobParams(models.Model):
    name = models.CharField(max_length=50, null=False, blank=False, unique=True)
    post_code = models.CharField(max_length=15, null=False, blank=False)
    delivery_date = models.DateField(null=False, blank=False)
    collection_date = models.DateField(null=False, blank=False)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        null=False,
        blank=False
    )

    def __str__(self):
        return self.name


class ProcessInfo(models.Model):
    job_count = models.IntegerField(null=False, blank=False, default=0)
    process_id = models.IntegerField(null=True, blank=True)
    status = models.CharField(blank=False, max_length=10, choices=PROCESS_STATUS, default="RUNNING")

    def save(self, *args, **kwargs):
        if self.process_id is None:
            self.process_id = start_background_job()
        super(ProcessInfo, self).save(*args, **kwargs)

    def delete(self, using=None, keep_parents=False):
        kill_process(self.process_id)
        super(ProcessInfo, self).delete()

    def __str__(self):
        return str(self.process_id)


class Job(models.Model):
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=10, choices=JOB_STATUS, default="NEW")
    result = models.TextField(null=True, blank=True)
    job_params = models.ForeignKey(
        JobParams,
        on_delete=models.CASCADE,
    )
    process = models.ForeignKey(
        ProcessInfo,
        on_delete=models.CASCADE,
    )

    def __str__(self):
        return self.status

    def save(self, *args, **kwargs):
        self.process = ProcessInfo.objects.filter(status="RUNNING").order_by("job_count")[0]
        self.process.job_count = self.process.job_count+1
        self.process.save()
        super(Job, self).save(*args, **kwargs)

    def final_result(self):
        try:
            result = json.loads(self.result or "{}")
            if len(result) > 0:
                if "error" in result.keys():
                    html = "<span>{}</span>".format(result.get("error"))
                else:
                    html = "<table id='final_result_table'>{}{}</table>"
                    thead = "<thead>{}</thead>"
                    all_keys = result.get("all_keys", [])
                    all_keys = sorted(all_keys)
                    formatted_keys = []
                    for key in all_keys:
                        key = "<th>{}</th>".format(key.replace("_", " "))
                        formatted_keys.append(key)
                    thead = thead.format("".join(formatted_keys))
                    tbody = "<tbody>{}</tbody>"
                    rows = result.get("rows", [])
                    trs = []
                    tr_append = trs.append
                    for each_row in rows:
                        tds = []
                        append = tds.extend
                        append(["<td>{}</td>".format(formatted_td_data(each_row.get(key))) for key in all_keys])
                        tr_append("<tr>{}</tr>".format("".join(tds)))
                    tbody = tbody.format("".join(trs))
                    html = html.format(thead, tbody)
            else:
                html = "<span>No Result...</span>"
        except Exception as ex:
            html = "<span>{}</span>".format(self.result)
        return format_html(html)


@receiver(post_save, sender=JobParams)
def job_params_post_save_handler(sender, **kwargs):
    if kwargs.get("created", False) is True:
        Job(job_params=kwargs["instance"]).save()


@receiver(post_save, sender=Job)
def job_post_save_handler(sender, **kwargs):
    process_id = kwargs["instance"].process.process_id
    send_sighup_signal(process_id=process_id)

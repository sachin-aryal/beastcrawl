from django.contrib import admin
from django.contrib.auth.models import Group
from django.http import HttpResponseRedirect
from django.urls import path
from django.utils.safestring import mark_safe
from django.contrib.admin import AdminSite
from .models import Job, JobParams, ProcessInfo
from .util.helper import start_background_job, kill_process, get_process_status


class JobParamsView(admin.ModelAdmin):
    list_display = (
        'name', 'post_code', 'delivery_date',
        'collection_date')
    fieldsets = [
        (None, {'fields': [('name', 'post_code', 'delivery_date', 'collection_date')]}),
    ]

    def __init__(self, model, admin_site, *args, **kwargs):
        super().__init__(model, admin_site)

    def has_add_permission(self, request):
        if len(ProcessInfo.objects.filter(status="RUNNING")) == 0:
            return False
        return True

    def save_model(self, request, obj, form, change):
        if getattr(obj, 'user', None) is None:
            obj.user = request.user
        obj.save()


class JobView(admin.ModelAdmin, AdminSite):
    list_display = (
        'parent_job', 'status', 'started_at',
        'completed_at', 'restart_job_link')
    exclude = ('result', )

    change_form_template = 'admin/job_result.html'

    no_link_dict = {}

    def has_change_permission(self, request, obj=None):
        if "_restart_job" in request.POST:
            return True
        return False

    def has_add_permission(self, request):
        return False

    def get_readonly_fields(self, request, obj=None):
        if obj:
            return self.readonly_fields + ('status', 'started_at', 'completed_at', 'job_params', 'final_result')
        return self.readonly_fields

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('restart_job/<int:job_id>', self.admin_site.admin_view(self.restart_job))
        ]
        return custom_urls + urls

    def restart_job(self, request, job_id):
        job = Job.objects.get(pk=job_id)
        if job:
            job.status = "RESTART"
            job.save()
        self.message_user(request, "Job status changed to Restart.")
        return HttpResponseRedirect("../")

    def restart_job_link(self, obj):
        if not self.no_link_dict:
            if len(ProcessInfo.objects.filter(status="RUNNING")) == 0:
                self.no_link_dict["no_link"] = True
            else:
                self.no_link_dict["no_link"] = False
        if self.no_link_dict["no_link"]:
            return mark_safe("<span>Start at least one process</span>")
        return mark_safe('<a href="restart_job/{}">Restart</a>'.format(obj.id))

    def parent_job(self, obj):
        return obj.job_params.name

    def __init__(self, model, admin_site, *args, **kwargs):
        super().__init__(model, admin_site)

    def response_change(self, request, obj):
        if "_restart_job" in request.POST:
            obj.status = "RESTART"
            obj.save()
            self.message_user(request, "Job status changed to Restart.")
            return HttpResponseRedirect(".")
        return super().response_change(request, obj)


class ProcessInfoView(admin.ModelAdmin):
    list_display = (
        'job_count', 'process_id', 'status', 'action', 'restart_process_link')

    exclude = ('status',)

    def __init__(self, model, admin_site, *args, **kwargs):
        super().__init__(model, admin_site)

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('start_process/<int:process_info_id>', self.admin_site.admin_view(self.start_process)),
            path('stop_process/<int:process_info_id>', self.admin_site.admin_view(self.stop_process)),
            path('restart/<int:process_info_id>', self.admin_site.admin_view(self.restart_process)),
        ]
        return custom_urls + urls

    def has_change_permission(self, request, obj=None):
        return False

    def save_model(self, request, obj, form, change):
        if getattr(obj, 'author', None) is None:
            obj.user = request.user
        obj.save()

    def get_readonly_fields(self, request, obj=None):
        return self.readonly_fields + ('job_count', 'process_id')

    def restart_process(self, request, process_info_id):
        process_info = ProcessInfo.objects.get(pk=process_info_id)
        kill_process(process_info.process_id)
        new_process_id = start_background_job()
        process_info.process_id = new_process_id
        process_info.job_count = 0
        process_info.save()
        self.message_user(request, "Process restarted successfully.")
        return HttpResponseRedirect("../")

    def stop_process(self, request, process_info_id):
        process_info = ProcessInfo.objects.get(pk=process_info_id)
        kill_process(process_info.process_id)
        process_info.job_count = 0
        process_info.process_id = 0
        process_info.status = "KILLED"
        process_info.save()
        self.message_user(request, "Process stopped successfully.")
        return HttpResponseRedirect("../")

    def start_process(self, request, process_info_id):
        process_info = ProcessInfo.objects.get(pk=process_info_id)
        new_process_id = start_background_job()
        process_info.process_id = new_process_id
        process_info.job_count = 0
        process_info.status = "RUNNING"
        process_info.save()
        self.message_user(request, "Process started successfully.")
        return HttpResponseRedirect("../")

    def restart_process_link(self, obj):
        return mark_safe('<a href="restart/{}">Restart</a>'.format(obj.id))

    def action(self, obj):
        if get_process_status(obj.process_id):
            return mark_safe('<a href="stop_process/{}">Stop</a>'.format(obj.id))
        else:
            return mark_safe('<a href="start_process/{}">Start</a>'.format(obj.id))


admin.site.register(JobParams, JobParamsView)
admin.site.register(Job, JobView)
admin.site.register(ProcessInfo, ProcessInfoView)
admin.site.unregister(Group)

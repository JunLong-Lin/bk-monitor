"""分片异步导出流程使用的数据模型。

这些模型刻意不继承 ``OperateRecordModel``：ExportJob 已有任务创建者，
其长度和含义都与公共基类的请求审计字段不同。
"""

from django.core.exceptions import ValidationError
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _


class ExportRecord(models.Model):
    id = models.BigAutoField(primary_key=True)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)

    class Meta:
        abstract = True


class PlanningRecord(ExportRecord):
    """初始规划与局部规划共用的持久化尝试生命周期。"""

    planning_attempts = models.PositiveIntegerField(default=0)
    planning_generation = models.PositiveBigIntegerField(default=0)
    planning_started_at = models.DateTimeField(null=True, blank=True)
    planning_lease_until = models.DateTimeField(null=True, blank=True)
    next_planning_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        abstract = True


class ExportJob(PlanningRecord):
    """用户可见的一条逻辑导出任务生命周期。"""

    class QueryKind(models.TextChoices):
        SINGLE = "single", _("单索引")
        UNION = "union", _("联合查询")
        SCENE = "scene", _("场景检索")

    class Status(models.TextChoices):
        PENDING = "PENDING", _("待规划")
        PLANNING = "PLANNING", _("规划中")
        READY = "READY", _("待调度")
        RUNNING = "RUNNING", _("执行中")
        SUCCESS = "SUCCESS", _("成功")
        FAILED = "FAILED", _("失败")
        CANCELED = "CANCELED", _("已取消")

    class Stage(models.TextChoices):
        DOWNLOAD_LOG = "DOWNLOAD_LOG", _("取数")
        PACKAGE = "PACKAGE", _("打包")
        UPLOAD = "UPLOAD", _("上传")
        FINALIZING = "FINALIZING", _("生成清单")

    space_uid = models.CharField(_("空间标识"), max_length=256)
    created_by = models.CharField(_("创建者"), max_length=64)
    source_app_code = models.CharField(_("来源应用"), max_length=32, blank=True, default="")
    request_id = models.CharField(_("幂等请求标识"), max_length=128, null=True, blank=True, default=None)
    query_kind = models.CharField(_("查询类型"), max_length=16, choices=QueryKind.choices)
    index_set_ids = models.JSONField(_("原始索引集ID"), default=list, blank=True)
    query_snapshot = models.JSONField(_("规范化查询快照"))
    query_hash = models.CharField(_("查询摘要"), max_length=64)
    routing_snapshot = models.JSONField(_("固定路由快照"), default=dict, blank=True)
    resolved_resource_ids = models.JSONField(_("完整资源集合"), default=list, blank=True)
    policy_snapshot = models.JSONField(_("执行策略快照"), default=dict, blank=True)
    # 时间单位与精度由 query_snapshot 决定，且对该 Job 的所有分片保持稳定。
    start_time = models.BigIntegerField(_("包含的时间下界"))
    end_time = models.BigIntegerField(_("不包含的时间上界"))
    time_tick = models.PositiveBigIntegerField(_("最小时间步长"), validators=[MinValueValidator(1)])
    status = models.CharField(_("状态"), max_length=16, choices=Status.choices, default=Status.PENDING)
    estimated_total = models.PositiveBigIntegerField(_("预计行数"), null=True, blank=True)
    actual_total = models.PositiveBigIntegerField(_("成功叶子实际行数"), default=0)
    requested_parallelism = models.PositiveSmallIntegerField(
        _("期望并行上限"), default=4, validators=[MinValueValidator(1), MaxValueValidator(8)]
    )
    current_plan_version = models.PositiveIntegerField(
        _("当前有效计划版本"), null=True, blank=True, validators=[MinValueValidator(1)]
    )
    state_version = models.PositiveBigIntegerField(_("状态更新版本"), default=0)
    manifest_object_key = models.CharField(_("有效清单对象键"), max_length=1024, blank=True, default="")
    manifest_checksum = models.CharField(_("清单SHA256"), max_length=64, blank=True, default="")
    manifest_bytes = models.PositiveBigIntegerField(_("清单字节数"), null=True, blank=True)
    artifacts_cleaned_at = models.DateTimeField(_("产物清理完成时间"), null=True, blank=True)
    finalization_attempts = models.PositiveIntegerField(default=0)
    next_finalization_at = models.DateTimeField(null=True, blank=True)
    started_at = models.DateTimeField(_("开始时间"), null=True, blank=True)
    last_dispatched_at = models.DateTimeField(_("最近一次投递时间"), null=True, blank=True)
    completed_at = models.DateTimeField(_("完成时间"), null=True, blank=True)
    expires_at = models.DateTimeField(_("成功产物到期时间"), null=True, blank=True)
    error_code = models.CharField(_("错误分类"), max_length=64, blank=True, default="")
    error_detail = models.TextField(_("脱敏错误详情"), blank=True, default="")

    class Meta:
        db_table = "log_export_job"
        unique_together = (("space_uid", "created_by", "request_id"),)
        indexes = [
            models.Index(fields=["created_by", "status", "created_at"], name="export_job_user_status"),
            models.Index(fields=["space_uid", "created_at", "id"], name="export_job_space_history"),
            models.Index(fields=["status", "updated_at"], name="export_job_recovery"),
            models.Index(fields=["expires_at"], name="export_job_expiry"),
        ]


class ExportPlan(ExportRecord):
    """一份完整且带版本的计划；一个 Job 同时只有一个生效版本。"""

    class Status(models.TextChoices):
        PLANNING = "PLANNING", _("规划中")
        READY = "READY", _("已完整持久化")
        FAILED = "FAILED", _("规划失败")
        SUPERSEDED = "SUPERSEDED", _("已替换")

    job = models.ForeignKey(ExportJob, on_delete=models.PROTECT, related_name="plans")
    plan_version = models.PositiveIntegerField(_("计划版本"), validators=[MinValueValidator(1)])
    status = models.CharField(_("状态"), max_length=16, choices=Status.choices, default=Status.PLANNING)
    planning_input = models.JSONField(_("规划输入快照"), default=dict, blank=True)
    query_hash = models.CharField(_("查询摘要"), max_length=64)
    statistics_at = models.DateTimeField(_("统计时间"), null=True, blank=True)
    target_rows = models.PositiveBigIntegerField(_("单片目标行数"), validators=[MinValueValidator(1)])
    target_bytes = models.PositiveBigIntegerField(_("单片目标字节数"), validators=[MinValueValidator(1)])
    histogram_interval = models.PositiveBigIntegerField(_("初始桶宽，单位同Job时间"), validators=[MinValueValidator(1)])
    part_count = models.PositiveIntegerField(_("有效叶子数量"), default=0)
    error_code = models.CharField(_("错误分类"), max_length=64, blank=True, default="")
    error_detail = models.TextField(_("脱敏错误详情"), blank=True, default="")

    class Meta:
        db_table = "log_export_plan"
        unique_together = (("job", "plan_version"),)
        indexes = []


class ExportPart(PlanningRecord):
    """计划中一个固定的左闭右开时间范围。"""

    class Status(models.TextChoices):
        WAITING = "WAITING", _("待投递")
        DISPATCHED = "DISPATCHED", _("已授权投递")
        RUNNING = "RUNNING", _("执行中")
        UPLOADING = "UPLOADING", _("上传中")
        SUCCESS = "SUCCESS", _("成功")
        FAILED = "FAILED", _("失败")
        SPLIT = "SPLIT", _("已拆分")
        CANCELED = "CANCELED", _("已取消")

    plan = models.ForeignKey(ExportPlan, on_delete=models.PROTECT, related_name="parts")
    part_no = models.PositiveIntegerField(_("计划内稳定序号"), validators=[MinValueValidator(1)])
    parent = models.ForeignKey("self", on_delete=models.PROTECT, related_name="children", null=True, blank=True)
    start_time = models.BigIntegerField(_("包含的时间下界"))
    end_time = models.BigIntegerField(_("不包含的时间上界"))
    is_leaf = models.BooleanField(_("有效叶子"), default=True)
    oversized = models.BooleanField(_("不可继续按时间拆分的超量片"), default=False)
    estimated_rows = models.PositiveBigIntegerField(_("预计行数"), null=True, blank=True)
    estimated_bytes = models.PositiveBigIntegerField(_("预计字节数"), null=True, blank=True)
    actual_rows = models.PositiveBigIntegerField(_("成功产物实际行数"), null=True, blank=True)
    actual_bytes = models.PositiveBigIntegerField(_("成功产物JSONL字节数"), null=True, blank=True)
    compressed_bytes = models.PositiveBigIntegerField(_("成功产物压缩字节数"), null=True, blank=True)
    processed_rows = models.PositiveBigIntegerField(_("本次尝试已写行数，可回退"), default=0)
    status = models.CharField(_("状态"), max_length=16, choices=Status.choices, default=Status.WAITING)
    stage = models.CharField(_("阶段"), max_length=16, choices=ExportJob.Stage.choices, blank=True, default="")
    attempts = models.PositiveIntegerField(_("累计执行次数"), default=0)
    dispatch_generation = models.PositiveBigIntegerField(_("投递代次"), default=0)
    task_id = models.CharField(_("Celery任务ID"), max_length=255, blank=True, default="")
    lease_id = models.CharField(_("令牌租约所有权标识"), max_length=128, blank=True, default="")
    lease_until = models.DateTimeField(_("租约到期时间"), null=True, blank=True)
    heartbeat_at = models.DateTimeField(_("执行心跳"), null=True, blank=True)
    next_retry_at = models.DateTimeField(_("下次允许尝试时间"), null=True, blank=True)
    worker_id = models.CharField(_("执行进程标识"), max_length=255, blank=True, default="")
    object_key = models.CharField(_("获胜产物对象键"), max_length=1024, blank=True, default="")
    checksum = models.CharField(_("获胜产物SHA256"), max_length=64, blank=True, default="")
    content_checksum = models.CharField(_("日志内容SHA256"), max_length=64, blank=True, default="")
    published_at = models.DateTimeField(_("消息发布时间"), null=True, blank=True)
    started_at = models.DateTimeField(_("本次执行开始时间"), null=True, blank=True)
    finished_at = models.DateTimeField(_("本次执行完成时间"), null=True, blank=True)
    error_code = models.CharField(_("错误分类"), max_length=64, blank=True, default="")
    error_detail = models.TextField(_("脱敏错误详情"), blank=True, default="")

    def clean(self):
        super().clean()
        if self.plan_id:
            job = self.plan.job
            if self.start_time is not None and self.end_time is not None:
                if self.start_time < job.start_time or self.end_time > job.end_time:
                    raise ValidationError(_("分片时间范围必须位于Job范围内"))
        if self.start_time is not None and self.end_time is not None and self.end_time <= self.start_time:
            raise ValidationError({"end_time": _("分片结束时间必须大于开始时间")})
        if self.parent_id:
            parent = self.parent
            if self.pk == self.parent_id or parent.plan_id != self.plan_id:
                raise ValidationError({"parent": _("父分片必须属于同一计划且不能是自身")})
            if not (
                parent.start_time <= self.start_time < self.end_time <= parent.end_time
                and (self.start_time, self.end_time) != (parent.start_time, parent.end_time)
            ):
                raise ValidationError({"parent": _("子分片必须是父分片时间范围的真子区间")})
        if self.status == self.Status.SPLIT and self.is_leaf:
            raise ValidationError({"is_leaf": _("已拆分的父分片不能继续作为有效叶子")})

    class Meta:
        db_table = "log_export_part"
        unique_together = (("plan", "part_no"),)
        indexes = [
            models.Index(fields=["status", "next_retry_at"], name="export_part_waiting"),
            models.Index(fields=["plan", "is_leaf", "status"], name="export_part_plan_leaves"),
            models.Index(fields=["lease_until"], name="export_part_lease"),
        ]


class ExportDispatchGate(models.Model):
    """在多个 Coordinator 之间串行化账本重建与投递。

    数据库锁在 Redis 丢失后依然有效，可避免两个独立的替代账本同时生效；
    cursor 用于在重启后保留轮转位置。
    """

    namespace = models.CharField(max_length=128, primary_key=True)
    cursor = models.PositiveBigIntegerField(default=0)

    class Meta:
        db_table = "log_export_dispatch_gate"

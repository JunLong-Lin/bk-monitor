# Generated by Django 3.2.25 on 2025-06-27 08:28

from django.db import migrations, models


def update_report_contents_width_height(apps, schema_editor):
    """
    更新ReportContents的width和height字段默认值
    """
    ReportContents = apps.get_model("bkmonitor", "ReportContents")
    update_report_contents = []
    for report_content in ReportContents.objects.all():
        # 如果图表是全屏，则设置width为1600，height为None
        if report_content.graphs:
            if "*" in report_content.graphs[0]:
                report_content.width = 1600
                report_content.height = None
                update_report_contents.append(report_content)
                continue

        # 如果图表是单图，则根据row_pictures_num设置width和height
        if report_content.row_pictures_num == 1:
            report_content.width = 800
            report_content.height = 270
        elif report_content.row_pictures_num == 2:
            report_content.width = 620
            report_content.height = 300
        update_report_contents.append(report_content)
    ReportContents.objects.bulk_update(update_report_contents, ["width", "height"], batch_size=1000)


class Migration(migrations.Migration):
    dependencies = [
        ("bkmonitor", "0183_bcscluster_bk_tenant_id"),
    ]

    operations = [
        migrations.AddField(
            model_name="reportcontents",
            name="height",
            field=models.IntegerField(blank=True, null=True, verbose_name="单图高度"),
        ),
        migrations.AddField(
            model_name="reportcontents",
            name="width",
            field=models.IntegerField(blank=True, null=True, verbose_name="单图宽度"),
        ),
        migrations.RunPython(update_report_contents_width_height),
    ]

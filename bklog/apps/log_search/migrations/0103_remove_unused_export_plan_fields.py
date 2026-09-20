from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("log_search", "0102_export_job_plan_part"),
    ]

    operations = [
        migrations.RemoveField(model_name="exportplan", name="status"),
        migrations.RemoveField(model_name="exportplan", name="error_code"),
        migrations.RemoveField(model_name="exportplan", name="error_detail"),
    ]

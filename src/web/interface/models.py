from django.db import models


# Create your models here.
class BaseModel(models.Model):
    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)


OPERATION_CHOICES = (
    ('equals', 'equals'),
    ('in', 'in'),
)


class Filter(BaseModel):
    name = models.CharField(max_length=100)
    operation = models.CharField(max_length=10, choices=OPERATION_CHOICES)
    produced_data_path = models.CharField(max_length=200)
    right_data_path = models.CharField(max_length=200)


class TestCase(BaseModel):
    name = models.CharField(max_length=100)
    data = models.JSONField()
    consumer_timeout = models.FloatField(null=True)


PRODUCER_TYPES = (
    ('kafka', 'kafka'),
    ('api', 'api'),
)

CONSUMER_TYPES = (
    ('kafka', 'kafka'),
    ('druid', 'druid'),
)


class TestCaseProducer(models.Model):
    test_case = models.ForeignKey(TestCase, on_delete=models.CASCADE, related_name='producers')
    producer_type = models.CharField(max_length=20, choices=PRODUCER_TYPES, default='kafka')
    kafka_topic = models.CharField(max_length=100, null=True)


class TestCaseConsumer(models.Model):
    test_case = models.ForeignKey(TestCase, related_name='consumers', on_delete=models.CASCADE)
    consumer_type = models.CharField(max_length=20, choices=CONSUMER_TYPES, default='kafka')
    data_filter = models.ForeignKey(Filter, null=True, on_delete=models.CASCADE)
    expected_count = models.PositiveIntegerField(null=True)
    kafka_topic = models.CharField(max_length=100, null=True)
    druid_datastore = models.CharField(max_length=100, null=True)


class TestRun(BaseModel):
    test_case = models.ForeignKey(TestCase, on_delete=models.CASCADE)
    passed = models.BooleanField(default=False)


class TestRunOffset(models.Model):
    test_run = models.ForeignKey(TestRun, on_delete=models.CASCADE)
    offsets = models.JSONField()


class TestRunData(models.Model):
    test_run = models.ForeignKey(TestRun, on_delete=models.CASCADE)
    consumer = models.ForeignKey(TestCaseConsumer, on_delete=models.CASCADE)
    data = models.JSONField()  # list of messages in case of kafka, records for druid
    data_count = models.PositiveIntegerField()
    filtered_count = models.PositiveIntegerField()

    def ignored_count(self):
        return self.data_count - self.filtered_count

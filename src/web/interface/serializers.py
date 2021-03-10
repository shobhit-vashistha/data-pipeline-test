from rest_framework import serializers
from rest_framework.relations import PrimaryKeyRelatedField

from interface.models import TestCase


class TestCaseDetailSerializer(serializers.ModelSerializer):
    producers = PrimaryKeyRelatedField(many=True)
    consumers = PrimaryKeyRelatedField(many=True)

    class Meta:
        model = TestCase
        fields = ['id', 'name', 'data', 'consumer_timeout', 'producers', 'consumers']

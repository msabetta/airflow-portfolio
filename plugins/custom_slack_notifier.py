import requests
import json
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator

class SlackNotifierHook(BaseHook):
    def __init__(self, slack_webhook_url: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.slack_webhook_url = slack_webhook_url

    def send_message(self, message: str):
        payload = {
            "text": message
        }
        headers = {
            "Content-Type": "application/json"
        }
        response = requests.post(
            self.slack_webhook_url,
            data=json.dumps(payload),
            headers=headers
        )
        response.raise_for_status()

class SlackNotifierOperator(BaseOperator):
    def __init__(self, slack_webhook_url: str, message: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.slack_webhook_url = slack_webhook_url
        self.message = message

    def execute(self, context):
        hook = SlackNotifierHook(self.slack_webhook_url)
        hook.send_message(self.message)
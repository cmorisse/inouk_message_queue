import pickle
import datetime
import json
import logging
import sys
import traceback
import base64
import uuid

import requests
import slackdown

import odoo
from odoo import models, fields, api
from odoo.tools.translate import _
from odoo.tools.safe_eval import safe_eval

import boto3
import pymsteams

from ..api import send_message

_logger = logging.getLogger('IMQ.queue')


class IMQQueueSlack(models.Model):
    _inherit = 'imq.queue'

    slack_team = fields.Char()
    slack_access_token = fields.Char()
    slack_webhook_channel = fields.Char()
    slack_webhook_url = fields.Char("Slack webhook URL")
    slack_webhook_config_url = fields.Char()
    slack_oauth_access_response = fields.Text()

    def btn_add_to_slack(self):
        """ Launch Slack oauth."""
        self.ensure_one()
        icp_model = self.env['ir.config_parameter'] 
        base_url = icp_model.sudo().get_param('web.base.url')
        SLACK_APP_CLIENT_ID = icp_model.get_param('imq.SLACK_APP_CLIENT_ID')
        SLACK_OAUTH_CALLBACK = icp_model.get_param('imq.SLACK_OAUTH_CALLBACK')  # Cloudflare worker

        imq_slack_oauth_ctrl = "%s/imq/v1/socb" % base_url  # socb = Slack Oauth Call-Back
        state_param_b = bytearray("%s/%s" % (imq_slack_oauth_ctrl, self.name), 'utf-8')
        state_param_b = base64.urlsafe_b64encode(state_param_b)
        state_param_str = state_param_b.decode('utf-8')
        
        # eg. https://slack.com/oauth/v2/authorize?client_id=5006003237.928870046194&scope=incoming-webhook
        slack_auth_uri = "https://slack.com/oauth/v2/authorize?client_id=%s&scope=incoming-webhook"\
                         "&redirect_uri=%s&state=%s" % (
                             SLACK_APP_CLIENT_ID, 
                             SLACK_OAUTH_CALLBACK, 
                             state_param_str,)

        _logger.debug("slack_auth_uri=%s", slack_auth_uri)
        return {
            "type": "ir.actions.act_url",
            "url": slack_auth_uri,
            "target": "self",
        }

    def btn_test_slack_notifications(self):
        """ Sends a Slack test notifications."""
        self.ensure_one()
        self.send_slack_notification('info', '', '', icon=":bear:")
        message = "Queue: *%s* is ready to send notifications." % self.name
        self.send_slack_notification('info', 'IMQ Notification Test', message)
        return

    def render_icon__slack(self, icon:str):
        # if message startwith 
        r_message = message.replace(':white_check_mark:', '<i class="fa fa-check-square"></i>')
        r_message = r_message.replace(':bear:', '<i class="fa fa-paw"/>')
        r_message = r_message.replace(':bangbang:', '<i class="fa fa-exclamation"></i>')
        r_message = r_message.replace(':warning:', '<i class="fa fa-exclamation-triangle"></i>')
        r_message = r_message.replace(':x:', '<i class="fa fa-times-circle"></i>')
        return r_message

    def send_slack_notification(
        self, message_type, message_title, message, icon=None, message_obj=None
    ):
        """ Send message to slack channels of all queues in record set """
        _msg = "\n%s" % message if message else ''
        notification_text = f"*{message_title}*{_msg}"

        if icon:
            notification_text = f"{icon} {notification_text}"

        for record in self:
            if record.slack_webhook_url:

                if message_obj:
                    notification_text += "\nMessage: *<%s|%s>*" % (message_obj.get_form_url(), message_obj.name,)
                payload = { "text": notification_text }
                    
                headers = {'Content-type': 'application/json'}
                try:
                    result = requests.post(record.slack_webhook_url, 
                                        data=json.dumps(payload), 
                                        headers=headers)
                    _logger.debug("requests.post(%s, data=%s, headers=%s) => %s", 
                        record.slack_webhook_url, 
                        json.dumps(payload), 
                        headers, 
                        result
                    )
                    _logger.info("result.text => %s", result.text)
                except:
                    exc_type, exc_value, exc_traceback = exc_info = sys.exc_info()
                    _logger.error("Failed to send slack notification !!!!!")
                    exc_message = traceback.format_exception(exc_type, 
                                                            exc_value, 
                                                            exc_traceback)
                    result_output = "\n".join(exc_message)
                    _logger.error(result_output)

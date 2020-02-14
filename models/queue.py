import pickle
import datetime
import json
import logging
import requests
import base64

from odoo import models, fields, api
import odoo
from odoo.exceptions import except_orm
from odoo.tools.translate import _

from ..api import send_message

_logger = logging.getLogger(__name__)

QUEUE_PROVIDERS = [
    ('aws_sqs', "AWS SQS"),
]

class IMQQueue(models.Model):
    _name = 'imq.queue'
    _description = "IMQ - Queue"
    _order= 'name'
    _sql_constraints = [
        (
            'name_queue_uniq', 
            'UNIQUE(name)', 
            _("Queue name must be unique among all queue providers.")
        )
    ]
    
    # fields
    name = fields.Char(size=20, index=True, uniq=True, required=True)
    sqs_name = fields.Char(compute='_compute_sqs_name')
    provider = fields.Selection(QUEUE_PROVIDERS, required=True)
    active = fields.Boolean(default=True)
    region = fields.Char()
    key = fields.Char()
    secret = fields.Char()
    description = fields.Text()
    test_result = fields.Text()

    slack_team = fields.Char()
    slack_access_token = fields.Char()
    slack_webhook_channel = fields.Char()
    slack_webhook_url = fields.Char("Slack webhook URL")
    slack_webhook_config_url = fields.Char()
    slack_oauth_access_response = fields.Text()

    @api.multi
    @api.depends('name')
    def _compute_sqs_name(self):
        for record in self:
            record.sqs_name = "{}_{}".format(
                record.name,
                record.env.cr.dbname
            )

    @api.multi
    def copy(self, default=None):
        self.ensure_one()
        chosen_name = default.get('name') if default else ''
        new_name = chosen_name or _('%s (copy)') % self.name
        default = dict(default or {}, name=new_name)
        return super(IMQQueue, self).copy(default)    


    # TODO: Move to a mixin and update message.py which share the same code
    @api.multi
    def get_formview_id(self, access_uid=None):
        self.ensure_one()
        return self.env.ref('inouk_message_queue.imq_queue__form_view').id

    @api.multi
    def get_default_action(self, access_uid=None):
        self.ensure_one()
        return self.env.ref('inouk_message_queue.imq_queue__act_window')

    def get_form_url(self):
        self.ensure_one()
        web_base_url = self.env['ir.config_parameter'].get_param('web.base.url')
        action_dict = self.get_formview_action()
        action_dict['action_id'] = self.get_default_action().id
        action_dict['web_base_url'] = web_base_url
        # target:
        # https://xsid-dev.inouk.ovh/web?debug#id=1&action=257&model=imq.test_launcher&view_type=form&menu_id=140
        url_str = "{web_base_url}/web#id={res_id}&action={action_id}&model="\
                  "{res_model}&view_type={view_type}".format(**action_dict)
        _logger.debug("URL for %s = > %q", self, url_str)
        return url_str

    @api.multi
    def btn_send_simple_message(self):
        """ Sends a simple message"""
        self.ensure_one()
        result = send_message(
            self.env,
            self.name,
            "TestMessage",
            None,  # payload
            None,  # MessageGroup
            "This is a test Message name",
        )
        result_str = json.dumps(result, sort_keys=True, indent=4)
        self.test_result = result_str

    @api.multi
    def btn_add_to_slack(self):
        """ Launch Slack oauth."""
        self.ensure_one()
        icp_model = self.env['ir.config_parameter'] 

        base_url = icp_model.get_param('web.base.url')
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


    @api.multi
    def btn_test_slack_notifications(self):
        """ Sends a Slack test notifications."""
        self.ensure_one()
        self.send_slack_notification(":bear:")
        message = "Queue: *%s* is ready to send notifications." % self.name
        self.send_slack_notification(message)

        # Channel does not send notification to user (pop)
        #channel_id = self.env['mail.channel'].search([('id', '=', 3)])
        #notification = ('<div class="sale.order"><a href="#" class="o_redirect" data-oe-id="%s">#%s</a></div>') % (rec.id, rec.name,)
        #channel_id.message_post(body=message, subtype='mail.mt_comment')


        return

    @api.multi
    def send_slack_notification(self, message, obj=None):
        """ Send message to slack channels of all queues in record set """
        for record in self:
            if record.slack_webhook_url:
                if obj:
                    notification_text = message.format(
                        object_link="*<%s|%s>*" % (obj.get_form_url(), obj.name)
                    )
                    payload = { "text": notification_text }
                else:
                    payload = { "text": message }
                    
                headers = {'Content-type': 'application/json'}
                result = requests.post(record.slack_webhook_url, 
                                       data=json.dumps(payload), 
                                       headers=headers)
                _logger.info("requests.post(%s, data=%s, headers=%s) => %s", 
                    record.slack_webhook_url, 
                    json.dumps(payload), 
                    headers, 
                    result
                )
                _logger.info("result.text => %s", result.text)

    @api.multi
    def send_notification(self, message, obj=None):
        """ Send message to all 'channels' (slack, sms) of all queues in recordset """
        self.send_slack_notification(message, obj=obj)
            
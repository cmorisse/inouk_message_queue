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


class TeamsIMQQueue(models.Model):
    _inherit = 'imq.queue'

    use_msteams_notifications = fields.Boolean("Use Microsoft Teams notificatations")
    msteams_webhookurl = fields.Char("Microsoft Teams URL")
    msteams_http_timeout = fields.Float(
        "Microsoft Teams Webhook timeout", 
        help="In seconds. eg 0.324 => 324ms",
        default=0.5
    )
    msteams_https_proxy = fields.Char(
        "Teams HTTPS Proxy",
        help="Enter full URL of https proxy (eg. http://user:password@proxy_fqdns:proxy_port) to"
             "use to send Microsoft Teams notifications. Please note that user and password must be"
             " URL encoded."
    )
            
    def get_form_url(self):
        """Return URL to open this record in backend form view (Odoo 18 format)."""
        self.ensure_one()
        base_url = self.get_base_url()
        action = self.get_default_action()
        url_str = f"{base_url}/odoo/action-{action.id}/{self.id}"
        _logger.debug("get_form_url(%s) => %s", self, url_str)
        return url_str

    def send_teams_message(
        self, title:str, message:str=None, icon_url:str=None, obj_url:str=None, facts:dict=None
    ):
        # We set timeout to 500ms
        myTeamsMessage = pymsteams.connectorcard(
            self.msteams_webhookurl, 
            http_timeout=self.msteams_http_timeout,
            https_proxy=self.msteams_https_proxy or None
        )
        myTeamsMessage.color("0072C6")
        myTeamsMessage.summary(title)
        myTeamsMessage.title(title)

        if icon_url or message:
            myMessageSection = pymsteams.cardsection()
            # Activity Elements
            if icon_url:
                myMessageSection.activityImage(icon_url)
            if message:
                myMessageSection.activityTitle(message)
            if facts:
                for key in facts:
                    # Facts are key value pairs displayed in a list.
                    myMessageSection.addFact(key, facts[key])
            # Add your section to the connector card object before sending
            myTeamsMessage.addSection(myMessageSection)

        if obj_url:
            myTeamsPotentialAction1 = pymsteams.potentialaction(_name = "Open Message")
            myTeamsPotentialAction1.addOpenURI("Open Message",[{"os": "default", "uri": obj_url}])
            myTeamsMessage.addPotentialAction(myTeamsPotentialAction1)
        
        try:
            myTeamsMessage.send()
        except:
            exc_type, exc_value, exc_traceback = exc_info = sys.exc_info()
            returned_value = traceback.format_exception(exc_type, 
                                                        exc_value, 
                                                        exc_traceback)
            returned_value = "\n".join(returned_value)
            _logger.error("Failed to send notification to Teams.")
            _logger.error(returned_value)
        return

    def render_icon__teams(self, icon:str):
        """ return icon as https://mpy13c-dev-cyril.odizy.ovh/inouk_message_queue/static/notifs/299110_check_sign_icon.png """
        icp_model = self.env['ir.config_parameter'].sudo()
        default_icon = "285667_bubbles_icon.png"
        icon_map = {
            ":bear:": "85348_bear_teddy_icon.png",
            
            ":white_check_mark:": "299110_check_sign_icon.png",
            "success": "299110_check_sign_icon.png",

            ":bangbang:": "53803_new_bang_icon.png",
            "info": "53803_new_bang_icon.png",

            "warning": "299112_warning_shield_icon.png",

            ":x:": "299045_sign_error_icon.png",
            "danger": "299045_sign_error_icon.png",
        }
        _icon = icon_map.get(icon, default_icon)
        web_base_url = icp_model.get_param('web.base.url')
        _icon_url = f"{web_base_url}/inouk_message_queue/static/notifs/{_icon}"
        return _icon_url

    def send_teams_notification(
        self, message_type, message_title, message, icon=None, message_obj=None, facts=None
    ):
        """ Send message to teams channels of all queues in record set 
        :param message_type: defines the 
        """
        for record in self:
            if record.use_msteams_notifications and record.msteams_webhookurl:
                if icon:
                    icon_url = self.render_icon__teams(icon)
                else:
                    icon_url = self.render_icon__teams(message_type)

                obj_url = message_obj.get_form_url() if message_obj else None
                self.send_teams_message(
                    message_title, 
                    message=message, 
                    icon_url=icon_url, 
                    obj_url=obj_url, 
                    facts=facts
                )

    def btn_test_msteams_notifications(self):
        """ Sends a Teams test notifications."""
        self.ensure_one()
        _icon=":bear:"
        self.send_teams_notification(
            'info',
            "IMQ Notification Test",
            "Queue: %s is ready to send notifications." % self.name,
            icon=_icon
        )
        return


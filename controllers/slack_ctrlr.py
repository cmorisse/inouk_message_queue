import json
import functools
import datetime
import logging
import uuid

from dateutil import relativedelta, parser
import werkzeug.wrappers
import werkzeug

from odoo import models, fields
from odoo.http import request, route, Controller, Response
from odoo.tools.safe_eval import safe_eval


_logger = logging.getLogger("IMQSLackWebController")


class IMQSlackControllerV2(Controller):
    """ 
    """

    def get_base_url(self):
        """ Use Odoo web.base.url ir.config_parameter to return correct base url 
        when proxy prevent to get it.
        Don't forget to add a 'web.base.url.freeze' to force value of web.base.url
        """
        base_url_root = request.env["ir.config_parameter"].sudo().get_param("web.base.url")
        if base_url_root:
            if base_url_root[-1] != '/': 
                base_url_root += '/'
            return base_url_root
        raise Exception("Missing 'web.base_url' system parameter.")

    @route('/imq/v1/socb/<string:queue_name>', methods=['POST'], type='json', auth='none', csrf=False, save_session=False)
    def slack_oauth_callback(self, queue_name=None):
        """ Called by CF Worker once User has accepted to install app in a channel and CFW 
        has called Slack oauth_access.
        """
        debug = 'debug' in request.httprequest.args
        _logger.warning("queue_name=%s", queue_name)
        queue_obj = request.env['imq.queue'].sudo().search([('name','=',queue_name)])
        if not queue_obj:
            raise werkzeug.exceptions.InternalServerError()

        payload = request.jsonrequest
        _logger.warning("body=%s", payload)
        queue_obj.write({
            "slack_team": payload["team"]["name"],
            "slack_access_token": payload["access_token"],
            "slack_webhook_channel": payload["incoming_webhook"]["channel"],
            "slack_webhook_url": payload["incoming_webhook"]["url"],
            "slack_webhook_config_url": payload["incoming_webhook"]["configuration_url"],
            "slack_oauth_access_response": json.dumps(payload, indent=4)
        })
        queue_url = request.env['imq.queue'].sudo().search([('name','=',queue_name)]).get_form_url()
        return queue_url


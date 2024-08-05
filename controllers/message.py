import json
import functools
import datetime
import logging
import uuid

from dateutil import relativedelta, parser
import werkzeug.wrappers
import werkzeug

from odoo import models, fields
from odoo.http import request, route, Controller, Response, AuthenticationError
from odoo.tools.safe_eval import safe_eval

from odoo.addons.muppy_core.utils import json_datetime_serializer


_logger = logging.getLogger(__file__)


class IMQMessageControllerV2(Controller):
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

    @route('/imq/message/<string:queue_message_id>', methods=['GET'], type='http', auth='none', csrf=False, save_session=False)
    def get_message(self, queue_message_id,kwargs=None):
        """ Return message data 
        curl --header 'Accept: application/json' -X GET  'https://mpy13c-k8s-journal-dev-cyril.truc-sbg3.odizy.ovh/imq/message/d703e30d-bcb9-4e48-95f2-bbbeb1ee74f4' 
        """
        debug = 'debug' in request.httprequest.args
        _logger.warning("queue_message_id=%s", queue_message_id)
        message_obj = request.env['imq.message'].sudo().search([('queue_message_id', '=', queue_message_id)])
        if not message_obj:
            raise werkzeug.exceptions.BadRequest()

        payload = message_obj.read()[0]
        _r =  json.dumps(payload, indent=4, default=json_datetime_serializer)
        return Response(_r, mimetype='application/json')


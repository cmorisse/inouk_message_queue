import datetime
import inspect
import logging
import re
import semver
import sys

from odoo import models, fields, api
from odoo.exceptions import UserError, MissingError
from odoo.models import NewId
from odoo.tools.translate import _
from odoo.tools.safe_eval import safe_eval



def split_multi(a_string, *args, **kwargs):
    """split a string according to a separators sequence.
    Useful for building an array of results from a stdout results 
    eg. ls -l
    """
    result = a_string  # TODO: don't modify parameter
    result =  filter(lambda p: bool(p), result.split(args[0]))
    result =  [filter(lambda p: bool(p), e.split(args[1])) for e in result]
    return result

# Cf. https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable/36142844#36142844
def json_datetime_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime.datetime):
        if obj.tzinfo:
            obj = obj.replace(tzinfo=None) - datetime.timedelta(seconds=obj.utcoffset().seconds)
        return "%sZ" % obj.isoformat()
    if isinstance(obj, datetime.date):
        return obj.isoformat()
    if isinstance(obj, (datetime.timedelta,)):
        return "%ss" % obj.total_seconds()
    return "__NOT_SERIALIZABLE_TYPE__%s" % (type(obj),)

mpy_json_encoder = json_datetime_serializer

def calc_normalized_semver_version(version:str):
    """ Accept a version string and return a version formated with {major:3}{minor:3}{patch:2}{qualifier}"""
    semver_version = semver.Version.parse(version)
    _qualifier = semver_version.prerelease or semver_version.build or ''
    return f"{semver_version.major:0>3}{semver_version.minor:0>3}{semver_version.patch:0>2}{_qualifier}"

def base36encode(number):
    """
    from: https://stackoverflow.com/questions/1181919/python-base-36-encoding
    """
    if number.__class__.__name__ == "NewId":
        number = number.origin
    if not isinstance(number, (int,) or number < 0):
        #_logger.error("number:'%s' is not a positive integer" % number)
        return "not-a-number"
    is_negative = number < 0
    number = abs(number)

    alphabet, base36 = ['0123456789abcdefghijklmnopqrstuvwxyz', '']
    while number:
        number, i = divmod(number, 36)
        base36 = alphabet[i] + base36
    #if is_negative:
    #    base36 = '-' + base36
    return base36 or alphabet[0]


def base36decode(number):
    return int(number, 36)
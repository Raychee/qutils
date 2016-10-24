import json

import requests

from . import is_list
from .json_model import JsonModel


class User(JsonModel):
    __fields__ = {
        "samAccountName": None,
        "displayName": None,
        "emailAddress": None,
        "GUID": None,
        "type": None,
        "dlPath": None
    }


class DL(JsonModel):
    __fields__ = {
        "samAccountName": None,
        "displayName": None,
        "isDLPublic": None,
        "isDLExternal": None,
        "isDLUniversal": None,
        "welcomeMessageStatus": None,
        "whenCreated": JsonModel.DateTimeType(),
        "emailAddress ": None,
        "GUID": None,
        "type": None,
        "owners": JsonModel.ModelType(User),
        "members": JsonModel.ModelType(User),
        "purpose": None,
        "extendedAttributes": None
    }


class Ticket(JsonModel):
    __fields__ = {
        "ID": None,
        "ticketOwner": None,
        "dlName": None,
        "type": None,
        "status": None,
        "date": JsonModel.DateTimeType(),
        "comment": None,
        "enableExternalEmail": None,
        "Boolean": None,
        "members": None,
        "owners": None,
        "purpose": None,
    }


class DLManager(object):
    PREFIX = 'https://dlmanager.paypalcorp.com/API'
    session = requests.Session()

    def __init__(self, executor, token, url_prefix=None):
        super(DLManager, self).__init__()
        self.executor = executor
        self.token = token
        if url_prefix is not None:
            self.PREFIX = url_prefix

    def get_dl_properties(self, dl_name):
        url = self.PREFIX + '/DL/properties/{}/{}/{}'.format(dl_name, self.executor, self.token)
        return DL(self._get_dl_info(url, dl_name, 'get the properties of').json())

    def get_dl_members(self, dl_name, recursive=False):
        url = self.PREFIX + '/DL/members{}/{}/{}/{}'.format('/recursive' if recursive else '',
                                                            dl_name, self.executor, self.token)
        return [User(u) for u in self._get_dl_info(url, dl_name, 'get the members of').json()]

    def get_user_memberships(self, user):
        if isinstance(user, User):
            user = user.samAccountName
        url = self.PREFIX + '/User/memberships/{}/{}/{}'.format(user, self.executor, self.token)
        return self._get_user_related_dls(url, user)

    def get_user_ownerships(self, user):
        if isinstance(user, User):
            user = user.samAccountName
        url = self.PREFIX + '/User/ownerships/{}/{}/{}'.format(user, self.executor, self.token)
        return self._get_user_related_dls(url, user)

    def get_user_properties(self, user_name):
        url = self.PREFIX + '/User/properties/{}/{}/{}'.format(user_name, self.executor, self.token)
        resp = self.session.get(url, verify=False)
        self._raise_for_error(resp, 'Cannot get properties of user "{}"'.format(user_name),
                              {requests.codes.server_error: 'invalid corp id / invalid executor / invalid token / internal server error'})
        return User(resp.json())

    def exists_dl(self, dl_name):
        url = self.PREFIX + '/DL/exist/{}/{}/{}'.format(dl_name, self.executor, self.token)
        resp = self.session.get(url, verify=False)
        self._raise_for_error(resp, 'Cannot check if DL "{}" exists'.format(dl_name),
                              {requests.codes.server_error: 'invalid executor / invalid token / internal server error'})
        return resp.text == 'true'

    def new_dl(self, dl_name=None, members=None, owners=None, ticket=None):
        url = self.PREFIX + '/DL/new/{}/{}'.format(self.executor, self.token)
        if dl_name is None and members is None and owners is None:
            if ticket is None:
                raise ValueError('Cannot create an empty DL without any specifications')
        else:
            if ticket is None:
                ticket = Ticket()
            if dl_name is not None:
                ticket.dlName = dl_name
            if members is not None:
                ticket.members = self._ensure_name_list(members)
            if owners is not None:
                ticket.owners = self._ensure_name_list(owners)
        resp = self.session.post(url, json=ticket.to_dict(), verify=False)
        self._raise_for_error(resp, 'Cannot create DL "{}"'.format(dl_name),
                              {requests.codes.unauthorized: 'invalid executor or token',
                               requests.codes.server_error: 'invalid ticket: {!r}'.format(ticket)})

    def rename_dl(self, dl_name, new_dl_name):
        url = self.PREFIX + '/DL/renameDL/{}/{}/{}/{}'.format(dl_name, new_dl_name, self.executor, self.token)
        self._get_dl_info(url, dl_name, 'rename to "{}" from'.format(new_dl_name))

    def set_public_dl(self, dl_name, public):
        url = self.PREFIX + '/DL/setPublic/{}/{}/{}/{}'.format(dl_name, public and 'true' or 'false',
                                                               self.executor, self.token)
        resp = self.session.get(url, verify=False)
        self._raise_for_error(resp, 'Cannot set DL "{}" to {}public'.format(dl_name, '' if public else 'non-'),
                              {requests.codes.unauthorized: 'invalid executor or token',
                               requests.codes.bad_request: 'invalid public status',
                               requests.codes.server_error: 'server error'},
                              dl_name, requests.codes.server_error)

    def search_dl(self, keyword):
        url = self.PREFIX + '/DL/search/{}/{}/{}'.format(keyword, self.executor, self.token)
        resp = self.session.get(url, verify=False)
        self._raise_for_error(resp, 'Cannot search DLs with keyword ""'.format(keyword),
                              {requests.codes.server_error: 'invalid executor / invalid token / server errors'})
        return [DL(d) for d in resp.json()]

    def delete_dl(self, dl_names):
        url = self.PREFIX + '/DL/delete/{}/{}'.format(self.executor, self.token)
        dl_names = self._ensure_name_list(dl_names)
        if any(not isinstance(d, basestring) for d in dl_names):
            raise ValueError('the dl_names must be one or a list of DLs')
        resp = self.session.post(url, data=json.dumps(dl_names),
                                 headers={'Content-Type': 'application/json'}, verify=False)
        self._raise_for_error(
            resp,
            'Cannot delete DLs {}'.format(', '.format('"{}"'.format(d) for d in dl_names)),
            {requests.codes.unauthorized: 'invalid executor or token',
             requests.codes.expectation_failed: 'invalid DL names or server errors',
             requests.codes.server_error: 'invalid DL names or server errors'}
        )

    def add_dl_members(self, dl_name, members):
        url = self.PREFIX + '/DL/members/add/{}/{}/{}'.format(dl_name, self.executor, self.token)
        return self._process_dl_users(url, dl_name, members, 'add member(s)', 'to')

    def add_dl_owners(self, dl_name, owners):
        url = self.PREFIX + '/DL/owners/add/{}/{}/{}'.format(dl_name, self.executor, self.token)
        return self._process_dl_users(url, dl_name, owners, 'add owners(s)', 'to')

    def remove_dl_members(self, dl_name, members):
        url = self.PREFIX + '/DL/members/remove/{}/{}/{}'.format(dl_name, self.executor, self.token)
        return self._process_dl_users(url, dl_name, members, 'remove member(s)', 'from')

    def remove_dl_owners(self, dl_name, owners):
        url = self.PREFIX + '/DL/owners/remove/{}/{}/{}'.format(dl_name, self.executor, self.token)
        return self._process_dl_users(url, dl_name, owners, 'remove owner(s)', 'from')

    def _process_dl_users(self, url, dl_name, users, log_action, log_action_prep):
        users = self._ensure_name_list(users)
        if any(not isinstance(u, basestring) for u in users):
            raise ValueError('the users must be one or a list of corp ids')
        resp = self.session.post(url, data=json.dumps(users),
                                 headers={'Content-Type': 'application/json'}, verify=False)
        self._raise_for_error(
            resp,
            'Cannot {} {} {} DL "{}"'.format(log_action, ', '.format('"{}"'.format(u) for u in users),
                                             log_action_prep, dl_name),
            {requests.codes.unauthorized: 'invalid executor or token',
             requests.codes.expectation_failed: 'invalid corp ids or server errors',
             requests.codes.server_error: 'invalid corp ids or server errors'}
        )

    def _get_dl_info(self, url, dl_name, log_action):
        resp = self.session.get(url, verify=False)
        self._raise_for_error(resp, 'Cannot {} DL "{}"'.format(log_action, dl_name),
                              {requests.codes.bad_request: 'invalid DL name',
                               requests.codes.server_error: 'invalid executor / invalid token / internal server error'},
                              dl_name)
        return resp

    def _get_user_related_dls(self, url, user):
        resp = self.session.get(url, verify=False)
        self._raise_for_error(resp, 'Cannot get related DLs of user "{}"'.format(user),
                              {requests.codes.server_error: 'invalid corp id / invalid executor / invalid token / internal server error'})
        return [DL(d) for d in resp.json()]

    @staticmethod
    def _ensure_name_list(users_or_dls):
        if not is_list(users_or_dls):
            users_or_dls = [users_or_dls]
        users_or_dls = [u.samAccountName if isinstance(u, User) else u for u in users_or_dls]
        return users_or_dls

    def _raise_for_error(self, resp, log_error_action, code_log_reason=None, check_exists_dl=None, check_exists_dl_when=None):
        if resp.status_code != requests.codes.ok:
            error_cls = self.CallAPIError
            reason_message = ''
            if check_exists_dl and (check_exists_dl_when is None or check_exists_dl_when == resp.status_code):
                try:
                    exist_dl = self.exists_dl(check_exists_dl)
                except self.CallAPIError:
                    pass
                else:
                    if not exist_dl:
                        error_cls = self.DLNotExist
                        reason_message = 'DL "{}" does not exist'.format(check_exists_dl)
            reason_message = reason_message or (code_log_reason or {}).get(resp.status_code, '')
            reason_message = reason_message and ' because of ' + reason_message
            raise error_cls('{} through API "{}"{}. Server response: <{}: {}> {}'
                            .format(log_error_action, resp.url, reason_message,
                                    resp.status_code, resp.reason, resp.text))

    class Error(Exception):
        def __init__(self, *args, **kwargs):
            super(DLManager.Error, self).__init__(*args, **kwargs)

    class CallAPIError(Error):
        def __init__(self, *args, **kwargs):
            super(DLManager.CallAPIError, self).__init__(*args, **kwargs)

    class DLNotExist(Error):
        def __init__(self, *args, **kwargs):
            super(DLManager.DLNotExist, self).__init__(*args, **kwargs)

'''Indexer State, Slack Notifications, and Build state to display'''
import datetime
import logging
import json
import re
import pytz
import requests

from snovault import COLLECTIONS
from . import SEARCH_MAX


log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class IndexerState(object):
    '''
    1. Keeps track of indexer state in elastic search
    2. Slack Integration
    3. Build state to display
    '''
    # pylint: disable=too-many-public-methods, too-many-instance-attributes
    def __init__(self, es_inst, index, title='primary', followups=[]):
        # pylint: disable=dangerous-default-value
        self.es_inst = es_inst
        self.index = index
        self.title = title
        self.state_id = self.title + '_indexer'
        self.todo_set = self.title + '_in_progress'
        self.troubled_set = self.title + '_troubled'
        self.last_set = self.title + '_last_cycle'
        self.success_set = None
        self.cleanup_this_cycle = [self.todo_set]
        self.cleanup_last_cycle = [self.last_set, self.troubled_set]
        self.override = 'reindex_' + self.title
        self.followup_prep_list = 'primary_followup_prep_list'
        self.staged_for_vis_list = 'staged_for_vis_indexer'
        self.staged_for_regions_list = 'staged_for_region_indexer'
        self.followup_lists = []
        for name in followups:
            if name != '':
                list_id = 'staged_for_' + name
                assert (
                    list_id == self.staged_for_vis_list or
                    list_id == self.staged_for_regions_list
                )
                self.followup_lists.append(list_id)
        self.clock = {}

    def get_obj(self, es_id, doc_type='meta'):
        '''ES Wrapper Function'''
        try:
            return self.es_inst.get(
                index=self.index,
                doc_type=doc_type,
                id=es_id
            ).get('_source', {})
        except:  # pylint: disable=bare-except
            return {}

    def put_obj(self, es_id, obj, doc_type='meta'):
        '''ES Wrapper Function'''
        try:
            self.es_inst.index(
                index=self.index,
                doc_type=doc_type,
                id=es_id,
                body=obj
            )
        except:  # pylint: disable=bare-except
            log.warning("Failed to save to es: %s", es_id, exc_info=True)

    def delete_objs(self, ids, doc_type='meta'):
        '''ES Wrapper Function'''
        for es_id in ids:
            try:
                self.es_inst.delete(
                    index=self.index,
                    doc_type=doc_type,
                    id=es_id
                )
            except:  # pylint: disable=bare-except
                pass

    def get_list(self, es_id):
        '''ES Wrapper Wrapper Function'''
        return self.get_obj(es_id).get('list', [])

    def get_count(self, es_id):
        '''ES Wrapper Wrapper Function'''
        return self.get_obj(es_id).get('count', 0)

    def put_list(self, es_id, a_list):
        '''ES Wrapper Wrapper Function'''
        return self.put_obj(es_id, {'list': a_list, 'count': len(a_list)})

    def set_add(self, es_id, vals):
        '''ES Wrapper Wrapper Function'''
        set_to_update = set(self.get_list(es_id))
        if set_to_update:
            set_to_update.update(vals)
        else:
            set_to_update = set(vals)
        self.put_list(es_id, set_to_update)

    def list_extend(self, es_id, vals):
        '''ES Wrapper Wrapper Function'''
        list_to_extend = self.get_list(id)
        if list_to_extend:
            list_to_extend.extend(vals)
        else:
            list_to_extend = vals
        self.put_list(es_id, list_to_extend)

    def rename_objs(self, from_id, to_id):
        '''ES Wrapper Wrapper Function'''
        val = self.get_list(from_id)
        if val:
            self.put_list(to_id, val)
            self.delete_objs([from_id])

    def get(self):
        '''
        Returns the basic state info
        ES Wrapper Wrapper Function
        '''
        return self.get_obj(self.state_id)

    def put(self, state):
        '''
        Update the basic state info
        ES Wrapper Wrapper Function
        '''
        errors = state.pop('errors', None)
        state['title'] = self.state_id
        self.put_obj(self.state_id, state)
        if errors is not None:
            state['errors'] = errors

    def request_reindex(self, requested):
        '''Requests full reindexing on next cycle'''
        if requested == 'all':
            if self.title == 'primary':
                self.delete_objs(["indexing"])
            else:
                self.put_obj(
                    self.override,
                    {
                        self.title : 'reindex',
                        'all_uuids': True
                    }
                )
        else:
            uuid_list = requested.split(',')
            uuids = set()
            while uuid_list:
                uuid = uuid_list.pop(0)
                uuid_match_re = (
                    "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                    "[0-9a-f]{4}-[0-9a-f]{12}"
                )
                is_uuid_match = re.match(uuid_match_re, uuid)
                if uuid and is_uuid_match:
                    uuids.add(uuid)
                else:
                    msg = (
                        "Requesting reindex of at least one "
                        "uninterpretable uuid: '%s'" % (
                            uuid
                        )
                    )
                    return msg
            override_obj = self.get_obj(self.override)
            if 'uuids' not in override_obj.keys():
                override_obj['uuids'] = list(uuids)
            else:
                uuids |= set(override_obj['uuids'])
                override_obj['uuids'] = list(uuids)
            self.put_obj(self.override, override_obj)
        return None

    @staticmethod
    def all_indexable_uuids(request):
        '''
        returns list of uuids pertinant to this indexer.
        Wrapper around all_uuids function
        '''
        return list(all_uuids(request.registry))

    def reindex_requested(self, request):
        '''returns list of uuids if a reindex was requested.'''
        override = self.get_obj(self.override)
        if override:
            if override.get('all_uuids', False):
                self.delete_objs([self.override] + self.followup_lists)
                return self.all_indexable_uuids(request)
            else:
                uuids = override.get('uuids', [])
                uuid_count = len(uuids)
                if uuid_count > 0:
                    if uuid_count > SEARCH_MAX:
                        self.delete_objs([self.override] + self.followup_lists)
                    else:
                        self.delete_objs([self.override])
                    return uuids
        return None

    def get_initial_state(self):
        '''Useful to initialize at idle cycle'''
        new_state = {
            'title': self.state_id,
            'status': 'idle'
        }
        state = self.get()
        for var in ['cycles']:
            val = state.pop(var, None)
            if val is not None:
                new_state[var] = val
        self.set_add("registered_indexers", [self.state_id])
        return new_state

    def start_clock(self, name):
        '''Can start a named clock and use it later to figure out elapsed time'''
        self.clock[name] = datetime.datetime.now(pytz.utc)

    def elapsed(self, name):
        '''Returns string of time elapsed since named clock started.'''
        start = self.clock.get(name)
        if start is None:
            return 'unknown'
        return str(datetime.datetime.now(pytz.utc) - start)

    def priority_cycle(self, request):
        '''
        Initial startup, reindex, or
        interupted prior cycle can all lead to a priority cycle.

        returns (discovered xmin, uuids,
        whether previous cycle was interupted).
        '''
        initialized = self.get_obj("indexing")
        if not initialized:
            self.delete_objs([self.override] + self.followup_lists)
            state = self.get()
            state['status'] = 'uninitialized'
            self.put(state)
            return (-1, [], False)
        state = self.get()
        reindex_uuids = self.reindex_requested(request)
        if reindex_uuids is not None and reindex_uuids != []:
            uuids_count = len(reindex_uuids)
            log.warning(
                '%s reindex of %d uuids requested',
                self.state_id,
                uuids_count,
            )
            return (-1, reindex_uuids, False)
        if state.get('status', '') != 'indexing':
            return (-1, [], False)
        xmin = state.get('xmin', -1)
        if xmin == -1:
            return (-1, [], False)
        undone_uuids = self.get_list(self.todo_set)
        if len(undone_uuids) <= 0:
            return (-1, [], False)
        return (xmin, undone_uuids, True)

    def prep_for_followup(self, xmin, uuids):
        '''
        Prepare a cycle of uuids for passing to a followup indexer
        (e.g. audits, viscache)
        '''
        prep_list = ["xmin:%s" % xmin]
        prep_list.extend(uuids)
        self.put_list(self.followup_prep_list, prep_list)

    def start_cycle(self, uuids, state=None):
        '''Every indexing cycle must be properly opened.'''
        self.clock = {}
        self.start_clock('cycle')
        if state is None:
            state = self.get()
        state['cycle_started'] = datetime.datetime.now().isoformat()
        state['status'] = 'indexing'
        state['cycle_count'] = len(uuids)
        self.put(state)
        self.delete_objs(self.cleanup_last_cycle)
        self.delete_objs(self.cleanup_this_cycle)
        self.put_list(self.todo_set, set(uuids))
        return state

    def add_errors(self, errors, finished=True):
        '''
        To avoid 16 worker concurency issues,
        errors are recorded at the end of a cycle.
        '''
        uuids = [err['uuid'] for err in errors]
        if uuids and finished:
            self.put_list(self.troubled_set, uuids)

    def finish_cycle(self, state, errors=None):
        '''Every indexing cycle must be properly closed.'''
        if errors:
            self.add_errors(errors)
        if self.followup_prep_list is not None:
            hand_off_list = self.get_list(self.followup_prep_list)
            if hand_off_list:
                for followup_id in self.followup_lists:
                    self.list_extend(followup_id, hand_off_list)
                self.delete_objs([self.followup_prep_list])
        done_count = self.get_count(self.todo_set)
        self.rename_objs(self.todo_set, self.last_set)
        if self.success_set is not None:
            state[self.title + '_updated'] = self.get_count(self.success_set)
        state['indexed'] = done_count
        self.delete_objs(self.cleanup_this_cycle)
        state['status'] = 'done'
        state['cycles'] = state.get('cycles', 0) + 1
        state['cycle_took'] = self.elapsed('cycle')
        self.put(state)
        return state

    def get_notice_user(self, user, bot_token):
        '''Returns the user token for a named user.'''
        slack_users = self.get_obj('slack_users', {})
        if not slack_users:
            try:
                res = requests.get(
                    'https://slack.com/api/users.list?token=%s' % (bot_token)
                )
                resp = json.loads(res.text)
                if not resp['ok']:
                    log.warning(resp)
                    return None
                members = resp.get('members', [])
                for member in members:
                    slack_users[member['name']] = member['id']
                self.put_obj('slack_users', slack_users)
            except:  # pylint: disable=bare-except
                return None
        return slack_users.get(user)

    def set_notices(self, from_host, who=None, bot_token=None, which=None):
        '''
        Set up notification so that slack bot can
        send a message when indexer finishes.
        '''
        # pylint: disable=too-many-branches
        if who is None and bot_token is None:
            return "ERROR: must specify who to notify or bot_token"
        if which is None:
            which = self.state_id
        if which == 'all':
            which = 'all_indexers'
        elif which not in self.get_list("registered_indexers"):
            if which + '_indexer' in self.get_list("registered_indexers"):
                which += '_indexer'
            else:
                return "ERROR: unknown indexer to monitor: %s" % (which)
        notify = self.get_obj('notify', 'default')
        if bot_token is not None:
            notify['bot_token'] = bot_token
        if 'from' not in notify:
            notify['from'] = from_host
        user_warns = ''
        if who is not None:
            if who in ['none', 'noone', 'nobody', 'stop', '']:
                notify.pop(which, None)
            else:
                indexer_notices = notify.get(which, {})
                if which == 'all_indexers':
                    if 'indexers' not in indexer_notices:
                        indexer_notices['indexers'] = self.get_list(
                            "registered_indexers"
                        )
                users = who.split(',')
                if 'bot_token' in notify:
                    for name in users:
                        notice_user = self.get_notice_user(
                            name,
                            notify['bot_token']
                        )
                        if notice_user is None:
                            user_warns += ', ' + name
                who_all = indexer_notices.get('who', [])
                who_all.extend(users)
                indexer_notices['who'] = list(set(who_all))
                notify[which] = indexer_notices
        self.put_obj('notify', notify, 'default')
        if user_warns != '':
            user_warns = 'Unknown users: ' + user_warns[2:]
        if 'bot_token' not in notify:
            return "WARNING: bot_token is required. " + user_warns
        elif user_warns != '':
            return "WARNING: " + user_warns
        return None

    def get_notices(self, full=False):
        '''Get the notifications'''
        notify = self.get_obj('notify', 'default')
        if full:
            return notify
        notify.pop('bot_token', None)
        notify.pop('from', None)
        for which in notify.keys():
            if len(notify[which].get('who', [])) == 1:
                notify[which] = notify[which]['who'][0]
        indexers = self.get_list("registered_indexers")
        if self.title == 'primary':
            indexers.append('all_indexers')
            for indexer in indexers:
                if notify.get(indexer, {}):
                    return notify
        else:
            indexers.remove(self.state_id)
            for indexer in indexers:
                notify.pop(indexer, None)
            for indexer in [self.state_id, 'all_indexers']:
                if notify.get(indexer, {}):
                    return notify
        return {}

    def send_notices(self):
        '''Sends notifications when indexer is done.'''
        # pylint: disable=too-many-statements, too-many-branches
        notify = self.get_obj('notify', 'default')
        if not notify:
            return
        if 'bot_token' not in notify or 'from' not in notify:
            return
        changed = False
        text = None
        who = []
        if 'all_indexers' in notify:
            indexers = notify['all_indexers'].get('indexers', [])
            if self.state_id in indexers:
                if (
                        self.state_id == 'primary_indexer' or
                        'primary_indexer' not in indexers
                    ):
                    indexers.remove(self.state_id)
                    if indexers:
                        notify['all_indexers']['indexers'] = indexers
                        changed = True
            if indexers:
                who.extend(notify['all_indexers'].get('who', []))
                notify.pop('all_indexers', None)
                changed = True
                text = 'All indexers are done for %s/_indexer_state' % (
                    notify['from']
                )
        if self.state_id in notify:
            who.extend(notify[self.state_id].get('who', []))
            notify.pop(self.state_id, None)
            changed = True
            if text is None:
                text = '%s is done for %s' % (self.state_id, notify['from'])
                if self.title == 'primary':
                    text += '/_indexer_state'
                else:
                    text += '/_%sindexer_state' % (self.title)
        if who and text is not None:
            who = list(set(who))
            users = ''
            msg = ''
            if len(who) == 1:
                channel = self.get_notice_user(who[0], notify['bot_token'])
                if channel:
                    msg = 'token=%s&channel=%s&text=%s' % (
                        notify['bot_token'],
                        channel,
                        text,
                    )
            if msg == '':
                channel = 'dcc-private'
                for user in who:
                    users += '@' + user + ', '
                msg = 'token=%s&channel=%s&link_names=true&text=%s%s' % (
                    notify['bot_token'],
                    channel,
                    users,
                    text
                )
            try:
                res = requests.get(
                    'https://slack.com/api/chat.postMessage?' + msg
                )
                resp = json.loads(res.text)
                if not resp['ok']:
                    log.warning(resp)
            except:  # pylint: disable=bare-except
                log.warning(
                    "Failed to notify via slack: [%s]",
                    msg
                )
        if changed:
            self.put_obj('notify', notify, 'default')

    def display(self, uuids=None):
        '''Create Json to Display'''
        # pylint: disable=too-many-statements, too-many-branches
        display = {}
        display['state'] = self.get()
        if (
                display['state'].get('status', '') == 'indexing' and
                'cycle_started' in display['state']
            ):
            started = datetime.datetime.strptime(
                display['state']['cycle_started'],
                '%Y-%m-%dT%H:%M:%S.%f'
            )
            display['state']['indexing_elapsed'] = str(datetime.datetime.now() - started)
        display['title'] = display['state'].get('title', self.state_id)
        display['uuids_in_progress'] = self.get_count(self.todo_set)
        display['uuids_troubled'] = self.get_count(self.troubled_set)
        display['uuids_last_cycle'] = self.get_count(self.last_set)
        if self.followup_prep_list is not None:
            display['to_be_staged_for_follow_up_indexers'] = self.get_count(
                self.followup_prep_list
            )
        if self.title == 'primary':
            for followup_id in self.followup_lists:
                display[followup_id] = self.get_count(followup_id)
        else:
            followup_id = 'staged_for_%s_list' % (self.title)
            display['staged_by_primary'] = self.get_count(followup_id)
        reindex = self.get_obj(self.override)
        if reindex:
            uuids = reindex.get('uuids')
            if uuids is not None:
                display['reindex_requested'] = uuids
            elif reindex.get('all_uuids', False):
                display['reindex_requested'] = 'all'
        notify = self.get_notices()
        if notify:
            display['notify_requested'] = notify
        display['now'] = datetime.datetime.now().isoformat()
        if uuids is not None:
            uuids_to_show = []
            uuid_list = self.get_obj(self.todo_set)
            if not uuid_list:
                uuids_to_show = 'No uuids indexing'
            else:
                uuid_start = 0
                try:
                    uuid_start = int(uuids)
                except:  #pylint: disable=bare-except
                    pass
                if uuid_start < uuid_list.get('count', 0):
                    uuid_end = uuid_start+100
                    if uuid_start > 0:
                        uuids_to_show.append(
                            "... skipped first %d uuids" % (
                                uuid_start
                            )
                        )
                    uuids_to_show.extend(
                        uuid_list['list'][uuid_start:uuid_end]
                    )
                    if uuid_list.get('count', 0) > uuid_end:
                        uuids_to_show.append(
                            "another %d uuids..." % (
                                uuid_list.get('count', 0) - uuid_end
                            )
                        )
                elif uuid_start > 0:
                    uuids_to_show.append(
                        "skipped past all %d uuids" % (
                            uuid_list.get('count', 0)
                        )
                    )
                else:
                    uuids_to_show = 'No uuids indexing'
            display['uuids_in_progress'] = uuids_to_show
        return display


def all_uuids(registry, types=None):
    '''Yield all uuids'''
    collections = registry[COLLECTIONS]
    initial = ['user', 'access_key']
    for collection_name in initial:
        collection = collections.by_item_type.get(collection_name, [])
        if types is not None and collection_name not in types:
            continue
        for uuid in collection:
            yield str(uuid)
    for collection_name in sorted(collections.by_item_type):
        if collection_name in initial:
            continue
        if types is not None and collection_name not in types:
            continue
        collection = collections.by_item_type[collection_name]
        for uuid in collection:
            yield str(uuid)

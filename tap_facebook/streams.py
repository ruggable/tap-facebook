# libraries

import os
import json
from datetime import datetime, timedelta
import singer

from singer import metadata
from singer import utils

from dateutil.parser import parse
from tap_facebook.context import Context

LOGGER = singer.get_logger()
KEY_PROPERTIES = ['id']

# functions

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def needs_parse_to_date(string):
    if isinstance(string, str):
        try:
            parse(string)
            return True
        except ValueError:
            return False
    return False


class Stream():
    name = None
    replication_method = None
    replication_key = None
    stream = None
    key_properties = KEY_PROPERTIES
    session_bookmark = None

    def __init__(self, client=None):
        self.client = client

    def get_bookmark(self, state, name=None):
        name = self.name if not name else name
        return (singer.get_bookmark(state, name, self.replication_key)
                or Context.config["start_date"])

    def update_bookmark(self, state, value, name=None):
        name = self.name if not name else name
        # when `value` is None, it means to set the bookmark to None
        if value is None or self.is_bookmark_old(state, value, name):
            singer.write_bookmark(state, name, self.replication_key, value)

    def get_end_date(self, start):
        end = start
        if self.client.end_date is not None:
            end = max( utils.strptime_with_tz( self.client.end_date ), start )
        else:
            end += timedelta(hours=24)
        return end


    def update_final_bookmark(self, state, starting_mark, name=None):
        name = self.name if not name else name
        bookmark = utils.strptime_with_tz( self.get_bookmark( state ) )
        starting_window = utils.strptime_with_tz( starting_mark )
        ending_window = self.get_end_date( starting_window )
        if bookmark < ending_window < utils.now():
            bookmark = ending_window
        singer.write_bookmark(state, name, self.replication_key, bookmark.strftime('%Y-%m-%d %H:%M:%SZ'))


    def is_bookmark_old(self, state, value, name):
        current_bookmark = self.get_bookmark(state, name)
        return utils.strptime_with_tz(value) > utils.strptime_with_tz(current_bookmark)

    def load_schema(self):
        schema_file = "schemas/{}.json".format(self.name)
        with open(get_abs_path(schema_file)) as file:
            schema = json.load(file)
        return schema

    def load_metadata(self):
        schema = self.load_schema()
        mdata = metadata.new()

        mdata = metadata.write(mdata, (), 'table-key-properties', self.key_properties)
        mdata = metadata.write(mdata, (), 'forced-replication-method', self.replication_method)

        if self.replication_key:
            mdata = metadata.write(mdata, (), 'valid-replication-keys', [self.replication_key])

        for field_name in schema['properties'].keys():
            if field_name in self.key_properties or field_name == self.replication_key:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
            else:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

        return metadata.to_list(mdata)

    def is_selected(self):
        return self.stream is not None

    # The main sync function.
    def sync(self, state):
        get_data = getattr(self.client, self.name)
        bookmark = self.get_bookmark(state)

        res = get_data(self.replication_key, bookmark)

        if self.replication_method == "INCREMENTAL":
            for item in res:
                self.update_bookmark(state, item[self.replication_key])
                yield (self.stream, item)
            self.update_final_bookmark(state, bookmark)

        elif self.replication_method == "FULL_TABLE":
            for item in res:
                yield (self.stream, item)

        else:
            raise Exception('Replication key not defined for {stream}'.format(stream=self.name))

class AdCreative(Stream):
    name = "adcreative"
    replication_method = "FULL_TABLE"
    key_properties = ["id"]

class Ads(Stream):
    name = "ads"
    replication_method = "FULL_TABLE"
    key_properties = ["id"]
    
class Campaigns(Stream):
    name = "campaigns"
    replication_method = "FULL_TABLE"
    key_properties = ["id"]
    
class AdsInsightsPlatformAndDevice(Stream):
    name = "ads_insights_platform_and_device"
    replication_method = "INCREMENTAL"
    replication_key = "date_start"
    key_properties = ["ad_id"]
    
STREAMS = {
    "adcreative": AdCreative,
    "ads": Ads,
    "campaigns": Campaigns,
    "ads_insights_platform_and_device": AdsInsightsPlatformAndDevice,
}

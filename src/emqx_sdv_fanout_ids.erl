-module(emqx_sdv_fanout_ids).

-export([
    create_tables/0,
    insert/4,
    first/1
]).

-include("emqx_sdv_fanout.hrl").

-define(KEY(VIN, RequestId), {VIN, RequestId}).

%% @doc Create the tables.
create_tables() ->
    ok = mria:create_table(?ID_TAB, [
        {type, ordered_set},
        {rlog_shard, ?DB_SHARD},
        {storage, rocksdb_copies},
        {record_name, ?ID_REC},
        {attributes, record_info(fields, ?ID_REC)}
    ]).

%% @doc Insert an ID into the table.
insert(VIN, RequestId, Ts, ID) ->
    mria:dirty_write(?ID_TAB, #?ID_REC{key = ?KEY(VIN, RequestId), ts = Ts, data_id = ID}).

%% @doc Get the first ID for a VIN.
first(VIN) ->
    case mnesia:dirty_first(?ID_TAB, seudo_prev(VIN)) of
        '$end_of_table' ->
            {error, empty};
        Key ->
            {ok, mnesia:dirty_read(?ID_TAB, Key)}
    end.

seudo_prev(VIN) ->
    ?KEY(VIN, <<>>).

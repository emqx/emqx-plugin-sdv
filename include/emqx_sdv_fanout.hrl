-ifndef(EMQX_SDV_FANOUT_HRL).
-define(EMQX_SDV_FANOUT_HRL, true).

-define(ID_TAB, sdv_fanout_ids).
-define(DATA_TAB, sdv_fanout_data).

-define(ID_REC, sdv_fanout_id).
-define(DATA_REC, sdv_fanout_data).

-record(?ID_REC, {
    key :: {VIN :: binary(), RequestID :: binary()},
    ts :: erlang:timestamp(),
    data_id :: binary()
}).

-record(?DATA_REC, {
    id :: binary(),
    ts :: erlang:timestamp(),
    data :: binary()
}).

-define(DB_SHARD, sdv_fanout).

-include_lib("emqx_plugin_helper/include/logger.hrl").
-define(LOGTAG, 'SDV_FANOUT').
-define(LOG(Level, Msg, Data), ?SLOG(Level, maps:merge(Data, #{msg => Msg}), #{tag => ?LOGTAG})).

-endif.

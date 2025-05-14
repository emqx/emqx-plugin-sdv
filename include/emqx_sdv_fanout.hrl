-ifndef(EMQX_SDV_FANOUT_HRL).
-define(EMQX_SDV_FANOUT_HRL, true).

-define(PLUGIN_NAME, "emqx_sdv_fanout").
-define(PLUGIN_VSN, ?plugin_rel_vsn).
-define(PLUGIN_NAME_VSN, <<?PLUGIN_NAME, "-", ?PLUGIN_VSN>>).

%% mnesia_rocksdb
-define(ID_TAB, sdv_fanout_ids).
-define(DATA_TAB, sdv_fanout_data).
%% ets
-define(INFLIGHT_TAB, sdv_fanout_inflight).

-define(ID_REC, sdv_fanout_id).
-define(DATA_REC, sdv_fanout_data).
-define(INFLIGHT_REC, sdv_fanout_inflight).

-record(?ID_REC, {
    key :: {VIN :: binary(), RequestID :: binary()},
    ts :: erlang:timestamp(),
    data_id :: binary(),
    extra = [] :: [any()]
}).

-record(?DATA_REC, {
    id :: binary(),
    ts :: erlang:timestamp(),
    data :: binary(),
    extra = [] :: [any()]
}).

-record(?INFLIGHT_REC, {
    pid :: pid(),
    vin :: binary(),
    request_id :: binary()
}).

-define(DB_SHARD, sdv_fanout).

-include_lib("emqx_plugin_helper/include/logger.hrl").
-define(LOGTAG, 'SDV_FANOUT').
-define(LOG(Level, Msg, Data), ?SLOG(Level, maps:merge(Data, #{msg => Msg}), #{tag => ?LOGTAG})).

-define(DISPATCHER_POOL, emqx_sdv_fanout_dispatcher).

-define(MAYBE_SEND(Trigger, Pid, VIN), {maybe_send, Trigger, Pid, VIN}).
-define(TRG_NEW_BATCH, new_batch_received).
-define(TRG_HEARTBEAT, vehicle_heartbeat).

-define(TRG_ACKED, vehicle_ack).
-define(ACKED(Pid, VIN, RequestId), {?TRG_ACKED, Pid, VIN, RequestId}).

-endif.

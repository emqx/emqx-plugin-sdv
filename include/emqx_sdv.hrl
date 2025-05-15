-ifndef(EMQX_SDV_HRL).
-define(EMQX_SDV_HRL, true).

-define(PLUGIN_NAME, "emqx_sdv").
-define(PLUGIN_VSN, ?plugin_rel_vsn).
-define(PLUGIN_NAME_VSN, <<?PLUGIN_NAME, "-", ?PLUGIN_VSN>>).

%% mnesia
-define(ID_TAB, sdv_fanout_ids).
-define(DATA_TAB, sdv_fanout_data).
-define(META_TAB, sdv_fanout_meta).
%% ets
-define(INFLIGHT_TAB, sdv_fanout_inflight).

-define(ID_REC, ?ID_TAB).
-define(DATA_REC, ?DATA_TAB).
-define(META_REC, ?META_TAB).
-define(INFLIGHT_REC, ?INFLIGHT_TAB).

-define(REF_KEY(VIN, Ts, RequestID), {VIN, Ts, RequestID}).
-type ref_key() :: ?REF_KEY(VIN :: binary(), Ts :: erlang:timestamp(), RequestID :: binary()).

-record(?ID_REC, {
    key :: ref_key(),
    data_id :: binary(),
    extra = [] :: [any()]
}).

%% Key is the SHA hash of the data.
-record(?DATA_REC, {
    data_id :: binary(),
    data :: binary()
}).

%% Key is the unique ID of the data.
%% Meta is the metadata of the data, including:
%%   - ts: timestamp for garbage collection
%% Detached from the data table to avoid read/overwrite large data body
%% when only need to read/update the metadata.
-record(?META_REC, {
    data_id :: binary(),
    meta = #{} :: map()
}).

-record(?INFLIGHT_REC, {
    pid :: pid(),
    ref :: ref_key(),
    mref :: reference()
}).

-define(DB_SHARD, sdv_shard).

-include_lib("emqx_plugin_helper/include/logger.hrl").
-define(LOGTAG, "SDV").
-define(LOG(Level, Msg, Data), ?SLOG(Level, maps:merge(Data, #{msg => Msg}), #{tag => ?LOGTAG})).

-define(DISPATCHER_POOL, emqx_sdv_fanout_dispatcher).

-define(MAYBE_SEND(Trigger, SubPid, VIN_Or_RefKey), {maybe_send, Trigger, SubPid, VIN_Or_RefKey}).
-define(TRG_NEW_BATCH, new_batch_received).
-define(TRG_HEARTBEAT, vehicle_heartbeat).

-define(TRG_ACKED, vehicle_ack).
-define(ACKED(SubPid, RefKey, MRef), {?TRG_ACKED, SubPid, RefKey, MRef}).

-endif.

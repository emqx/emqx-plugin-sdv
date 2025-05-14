-module(emqx_sdf_faount_inflight).

-export([
    create_tables/0,
    insert/3,
    delete/1,
    lookup/1,
    has_inflight/1
]).

-include("emqx_sdv_fanout.hrl").

%% @doc Create the tables.
create_tables() ->
    ets:new(?INFLIGHT_TAB, [
        named_table,
        {keypos, #?INFLIGHT_REC.pid},
        {type, ordered_set},
        {write_concurrency, true},
        {read_concurrency, true}
    ]).

%% @doc Insert a new inflight record.
insert(Pid, VIN, RequestID) ->
    ets:insert(?INFLIGHT_TAB, #?INFLIGHT_REC{pid = Pid, vin = VIN, request_id = RequestID}),
    ok.

%% @doc Delete an inflight record.
delete(Pid) ->
    ets:delete(?INFLIGHT_TAB, Pid),
    ok.

%% @doc Return 'true' if the pid has an inflight record.
has_inflight(Pid) ->
    ets:member(?INFLIGHT_TAB, Pid).

%% @doc Lookup the inflight record by the pid.
lookup(Pid) ->
    case ets:lookup(?INFLIGHT_TAB, Pid) of
        [Rec] -> {ok, Rec};
        [] -> {error, not_found}
    end.

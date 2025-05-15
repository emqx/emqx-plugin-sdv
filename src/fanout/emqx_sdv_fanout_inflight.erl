%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sdv_fanout_inflight).

-export([
    create_tables/0,
    insert/3,
    delete/1,
    lookup/1,
    exists/1
]).

-include("emqx_sdv.hrl").

%% @doc Create the tables.
create_tables() ->
    ets:new(?INFLIGHT_TAB, [
        public,
        named_table,
        set,
        {keypos, #?INFLIGHT_REC.pid},
        {write_concurrency, true},
        {read_concurrency, true}
    ]),
    ok.

%% @doc Insert a new inflight record.
-spec insert(Pid :: pid(), RefKey :: ref_key(), MRef :: reference()) -> ok.
insert(Pid, RefKey, MRef) ->
    ets:insert(?INFLIGHT_TAB, #?INFLIGHT_REC{pid = Pid, ref = RefKey, mref = MRef}),
    ok.

%% @doc Delete an inflight record.
-spec delete(Pid :: pid()) -> ok.
delete(Pid) ->
    ets:delete(?INFLIGHT_TAB, Pid),
    ok.

%% @doc Return 'true' if the pid has an inflight record.
-spec exists(Pid :: pid()) -> boolean().
exists(Pid) ->
    ets:member(?INFLIGHT_TAB, Pid).

%% @doc Lookup the inflight record by the pid.
-spec lookup(Pid :: pid()) -> {ok, ref_key(), reference()} | {error, not_found}.
lookup(Pid) ->
    case ets:lookup(?INFLIGHT_TAB, Pid) of
        [#?INFLIGHT_REC{ref = RefKey, mref = MRef}] -> {ok, RefKey, MRef};
        [] -> {error, not_found}
    end.

-module(emqx_sdv_fanout_dispatcher).

-export([batch/1, ack/2]).

-export([start_link/2]).

%% RPC
-export([notify_dispatcher/3]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("emqx_sdv_fanout.hrl").

%% @doc Handle a batch received from SDV platform.
batch(Payload) ->
    try emqx_utils_json:decode(Payload, [return_maps]) of
        #{<<"ids">> := VINs, <<"request_id">> := RequestId, <<"data">> := Data} ->
            ok = insert_batch(VINs, RequestId, Data),
            ok = notify_dispatchers(VINs),
            ok;
        _ ->
            {error, invalid_payload}
    catch
        _:_ ->
            {error, invalid_payload}
    end.

%% @doc Handle an ACK received from a vehicle.
ack(VIN, RequestId) ->
    Dispatcher = gproc_pool:pick_worker(?DISPATCHER_POOL, VIN),
    gen_server:cast(Dispatcher, ?ACKED(self(), VIN, RequestId)).

insert_batch(VINs, RequestId, Data) ->
    Now = now_ts(),
    {ok, ID} = emqx_sdv_fanout_data:insert(Now, Data),
    lists:foreach(fun(VIN) -> emqx_sdv_fanout_ids:insert(VIN, RequestId, Now, ID) end, VINs).

notify_dispatchers([]) ->
    ok;
notify_dispatchers([VIN | VINs]) ->
    case emqx_cm:lookup_channels(VIN) of
        [Pid] ->
            notify_dispatcher(Pid, VIN, ?TRG_NEW_BATCH);
        _ ->
            %% If no channel is found, ignore because the client is not connected
            %% If more than one channel is found, ignore because the client is in the middle of session takeover/resumption
            %% or the client is in a zombie state caused stale channels lingering
            ok
    end,
    notify_dispatchers(VINs).

%% @doc Notify the dispatcher of a new message.
notify_dispatcher(Pid, VIN, Trigger) when node(Pid) =:= node() ->
    case emqx_cm:is_channel_connected(Pid) of
        true ->
            do_notify_dispatcher(Pid, VIN, Trigger);
        false ->
            %% If the session is found but client is offline,
            %% the client will reconnect to trigger the send
            %% again, so we don't need to do anything here
            ok
    end;
notify_dispatcher(Pid, VIN, Trigger) ->
    %% the client is not on this node, RPC to the remote node
    %% and the remote node will notify the dispatcher
    emqx_rpc:cast(node(Pid), ?MODULE, notify_dispatcher, [Pid, VIN, Trigger]).

do_notify_dispatcher(Pid, VIN, Trigger) ->
    Dispatcher = gproc_pool:pick_worker(?DISPATCHER_POOL, VIN),
    gen_server:cast(Dispatcher, ?MAYBE_SEND(Trigger, Pid, VIN)).

now_ts() ->
    erlang:system_time(millisecond).

start_link(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_utils:proc_name(Pool, Id)},
        ?MODULE,
        [Pool, Id],
        [{hibernate_after, 1000}]
    ).

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #{pool => Pool, id => Id}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

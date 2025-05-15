%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sdv_fanout_dispatcher).

-export([batch/1, ack/2, heartbeat/1]).

-export([start_link/2]).

%% RPC
-export([notify_dispatcher_if_vehicle_online/3]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    handle_continue/2,
    terminate/2,
    code_change/3
]).

-include("emqx_sdv_fanout.hrl").

%% @doc Handle a batch received from SDV platform.
batch(Payload) ->
    try emqx_utils_json:decode(Payload, [return_maps]) of
        #{<<"ids">> := VINs, <<"request_id">> := RequestId, <<"data">> := Data} ->
            {ok, _ID} = insert_batch(VINs, RequestId, Data),
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
    case emqx_sdv_fanout_inflight:lookup(self()) of
        {ok, RefKey} ->
            %% do the heavy lifting in the client process
            %% because mria:dirty_delete/2 may involve RPC
            %% to core nodes if running in a replica node
            ok = emqx_sdv_fanout_ids:delete(RefKey),
            %% find the dispatcher for the VIN and tell it to send the next message
            Dispatcher = gproc_pool:pick_worker(?DISPATCHER_POOL, VIN),
            gen_server:cast(Dispatcher, ?ACKED(self(), RefKey));
        {error, not_found} ->
            %% half-transmitted QoS 1 was retried, ignore for now, fix until someone screams
            %% TODO: scan the ids table for the VIN + RequestId and delete it
            ?LOG(warning, "acked_unknown_request_id", #{
                vin => VIN,
                request_id => RequestId
            }),
            ok
    end.

%% @doc Handle a heartbeat from a vehicle.
heartbeat(VIN) ->
    %% check if the VIN has any messages inflight or pending to be sent in the client process
    %% so to minimize the number or message passing to the dispatcher pool
    case emqx_sdv_fanout_inflight:is_exist(self()) of
        true ->
            %% there are inflight messages, do not notify the dispatcher
            %% because the client process will do it when the inflight messages are acknowledged
            ?LOG(debug, "inflight_messages_detected", #{
                vin => VIN,
                trigger => ?TRG_HEARTBEAT
            }),
            ok;
        false ->
            case emqx_sdv_fanout_ids:next(VIN) of
                {ok, {_RefKey, _DataID}} ->
                    %% this is called by the vehicle client process itself,
                    %% so it's for sure online, directly notify the dispatcher
                    %% i.e. no need to call notify_dispatcher_if_vehicle_online/3
                    do_notify_dispatcher(self(), VIN, ?TRG_HEARTBEAT);
                {error, empty} ->
                    %% ignore if the VIN has no inflight messages
                    ?LOG(debug, "no_pending_messages_to_send", #{
                        vin => VIN,
                        trigger => ?TRG_HEARTBEAT
                    }),
                    ok
            end
    end.

insert_batch(VINs, RequestId, Data) ->
    Now = now_ts(),
    {ok, ID} = emqx_sdv_fanout_data:insert(Now, Data),
    lists:foreach(fun(VIN) -> emqx_sdv_fanout_ids:insert(VIN, Now, RequestId, ID) end, VINs),
    ?LOG(debug, "insert_new_batch", #{
        vin => VINs,
        request_id => RequestId,
        data_id => ID
    }),
    {ok, ID}.

notify_dispatchers(VINs) ->
    _ = proc_lib:spawn(fun() ->
        set_label(sdv_fanout_dispatcher_notify_dispatchers),
        do_notify_dispatchers(VINs)
    end),
    ok.

-if(?OTP_RELEASE >= 27).
set_label(Label) ->
    proc_lib:set_label(Label).
-else.
set_label(_Label) ->
    ok.
-endif.

do_notify_dispatchers([]) ->
    ok;
do_notify_dispatchers([VIN | VINs]) ->
    case emqx_cm:lookup_channels(VIN) of
        [Pid] ->
            notify_dispatcher_if_vehicle_online(Pid, VIN, ?TRG_NEW_BATCH);
        _ ->
            %% If no channel is found, ignore because the client is not connected
            %% If more than one channel is found, ignore because the client
            %% is in the middle of session takeover/resumption
            %% or the client is in a zombie state caused stale channels lingering
            ?LOG(debug, "no_session_found", #{vin => VIN}),
            ok
    end,
    do_notify_dispatchers(VINs).

%% @doc Notify the dispatcher of a new message.
notify_dispatcher_if_vehicle_online(Pid, VIN, Trigger) when node(Pid) =:= node() ->
    %% Do not trigger a send if the client is offline or has inflight messages
    IsConnected = emqx_cm:is_channel_connected(Pid),
    HasInflightMessages = emqx_sdv_fanout_inflight:is_exist(Pid),
    case IsConnected andalso not HasInflightMessages of
        true ->
            do_notify_dispatcher(Pid, VIN, Trigger);
        false ->
            ?LOG(debug, "client_offline_or_inflight_messages", #{
                vin => VIN,
                trigger => Trigger,
                is_connected => IsConnected,
                has_inflight_messages => HasInflightMessages
            }),
            ok
    end;
notify_dispatcher_if_vehicle_online(Pid, VIN, Trigger) ->
    %% the client is not on this node, RPC to the remote node
    %% and the remote node will notify the dispatcher
    emqx_rpc:cast(node(Pid), ?MODULE, notify_dispatcher_if_vehicle_online, [Pid, VIN, Trigger]).

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

handle_cast(?MAYBE_SEND(_Trigger, SubPid, _VIN_Or_RefKey) = Continue, State) ->
    %% check if there is already an inflight message
    %% this check is needed to avoid race condition:
    %% caller observed no inflight message while
    %% the dispatcher is sending the message.
    case emqx_sdv_fanout_inflight:is_exist(SubPid) of
        true ->
            {noreply, State};
        false ->
            {noreply, State, {continue, Continue}}
    end;
handle_cast(?ACKED(SubPid, RefKey), State) ->
    emqx_sdv_fanout_inflight:delete(SubPid),
    Continue = ?MAYBE_SEND(?TRG_ACKED, SubPid, RefKey),
    {noreply, State, {continue, Continue}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_continue(?MAYBE_SEND(Trigger, SubPid, VIN_Or_RefKey), #{id := Id} = State) ->
    %% continue to send the next message (if any)
    case maybe_send(Trigger, SubPid, VIN_Or_RefKey, Id) of
        {ok, RefKey} ->
            _ = erlang:monitor(process, SubPid),
            emqx_sdv_fanout_inflight:insert(SubPid, RefKey);
        ignore ->
            ok
    end,
    {noreply, State}.

handle_info({'DOWN', _Ref, process, SubPid, _Reason}, State) ->
    _ = emqx_sdv_fanout_inflight:delete(SubPid),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @doc Send the next message to the client process.
%% Returns ok if the message is sent.
%% Returns ignore if there is no message to send.
-spec maybe_send(
    Trigger :: atom(),
    SubPid :: pid(),
    VIN_Or_RefKey :: binary() | ref_key(),
    DispatcherId :: integer()
) ->
    {ok, ref_key()} | ignore.
maybe_send(Trigger, SubPid, VIN_Or_RefKey, DispatcherId) ->
    case emqx_sdv_fanout_ids:next(VIN_Or_RefKey) of
        {ok, {RefKey, DataID}} ->
            maybe_send2(Trigger, SubPid, RefKey, DataID, DispatcherId);
        {error, empty} ->
            ?LOG(debug, "no_pending_messages_to_send", #{
                key => VIN_Or_RefKey,
                trigger => Trigger
            }),
            ignore
    end.

maybe_send2(Trigger, SubPid, RefKey, DataID, DispatcherId) ->
    ?REF_KEY(VIN, _Ts, RequestId) = RefKey,
    case emqx_sdv_fanout_data:read(DataID) of
        {ok, Data} ->
            ?LOG(info, "publish_to_subscriber", #{
                trigger => Trigger,
                sub_pid => SubPid,
                vin => VIN,
                request_id => RequestId
            }),
            ok = deliver_to_subscriber(SubPid, VIN, RequestId, Data, DispatcherId),
            {ok, RefKey};
        {error, not_found} ->
            %% ignore if the data is not found
            %% maybe deleted by garbage collection
            ?LOG(debug, "data_not_found", #{
                vin => VIN,
                trigger => Trigger
            }),
            ignore
    end.

deliver_to_subscriber(SubPid, VIN, RequestId, Data, DispatcherId) ->
    Topic = <<"agent/", VIN/binary, "/proxy/request/", RequestId/binary>>,
    From = seudo_clientid(DispatcherId),
    Qos = 1,
    Message = emqx_message:make(From, Qos, Topic, Data),
    erlang:send(SubPid, {deliver, Topic, Message}),
    ?LOG(debug, "message_delivered_to_subscriber_session", #{
        vin => VIN,
        request_id => RequestId
    }),
    ok.

seudo_clientid(DispatcherId) ->
    iolist_to_binary(["sdv-fanout-dispatcher-", integer_to_binary(DispatcherId)]).

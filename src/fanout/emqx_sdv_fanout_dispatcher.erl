%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sdv_fanout_dispatcher).

-export([trigger/1, ack/2, heartbeat/1]).

-export([start_link/2]).

%% RPC
-export([notify_dispatchers_rpc_handler/1]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    handle_continue/2,
    terminate/2,
    code_change/3
]).

-include("emqx_sdv.hrl").

%% @doc Handle a trigger received from SDV platform.
trigger(Payload) ->
    try emqx_utils_json:decode(Payload, [return_maps]) of
        #{<<"ids">> := VINs, <<"request_id">> := RequestId, <<"data_id">> := DataID} ->
            insert_ids(VINs, RequestId, DataID),
            notify_dispatchers(VINs),
            ok;
        _ ->
            {error, #{cause => invalid_payload}}
    catch
        _:_ ->
            {error, #{cause => invalid_payload}}
    end.

%% @doc Handle an ACK received from a vehicle.
ack(VIN, _RequestId) ->
    SubPid = self(),
    case emqx_sdv_fanout_inflight:lookup(SubPid) of
        {ok, RefKey, DispatchPid, DispatchMRef} ->
            ?LOG(debug, "ack_received", #{vin => VIN, sub_pid => SubPid}),
            %% do the heavy lifting in the client process
            %% because mria:dirty_delete/2 may involve RPC
            %% to core nodes if running in a replica node
            ok = emqx_sdv_fanout_ids:delete(RefKey),
            gen_server:cast(DispatchPid, ?ACKED(SubPid, RefKey, DispatchMRef));
        {error, not_found} ->
            ?LOG(info, "inflight_not_found", #{vin => VIN, sub_pid => SubPid}),
            %% Race-condition:
            %% QoS-1 message delivered from session state after resume
            %% The inflight state is lost for the old process.
            ok = emqx_sdv_fanout_ids:delete_by_vin(VIN),
            Dispatcher = gproc_pool:pick_worker(?DISPATCHER_POOL, VIN),
            gen_server:cast(Dispatcher, ?MAYBE_SEND(?TRG_ACKED, SubPid, VIN))
    end.

%% @doc Handle a heartbeat from a vehicle.
heartbeat(VIN) ->
    %% check if the VIN has any messages inflight or pending to be sent in the client process
    %% so to minimize the number or message passing to the dispatcher pool
    case emqx_sdv_fanout_inflight:exists(self()) of
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
                    Dispatcher = gproc_pool:pick_worker(?DISPATCHER_POOL, VIN),
                    SubPid = self(),
                    gen_server:cast(Dispatcher, ?MAYBE_SEND(?TRG_HEARTBEAT, SubPid, VIN));
                {error, empty} ->
                    %% ignore if the VIN has no inflight messages
                    ?LOG(debug, "no_pending_messages_to_send", #{
                        vin => VIN,
                        trigger => ?TRG_HEARTBEAT
                    }),
                    ok
            end
    end.

insert_ids(VINs, RequestId, DataID) ->
    Now = now_ts(),
    lists:foreach(fun(VIN) -> emqx_sdv_fanout_ids:insert(VIN, Now, RequestId, DataID) end, VINs),
    ?LOG(debug, "insert_new_vins_for_fanout", #{
        vin => VINs,
        request_id => RequestId,
        data_id => DataID
    }).

%% @private
%% Group VINs by node. Returns a list of {Node, [{Pid, VIN}]} tuples.
partition_per_node(VINs) ->
    partition_per_node(VINs, #{}).

partition_per_node([], Acc) ->
    maps:to_list(Acc);
partition_per_node([VIN | VINs], Acc) ->
    case emqx_cm:lookup_channels(VIN) of
        [Pid] ->
            L = maps:get(node(Pid), Acc, []),
            partition_per_node(VINs, maps:put(node(Pid), [{Pid, VIN} | L], Acc));
        [_, _ | _] ->
            %% ignore if the VIN is subscribed to more than one session
            %% because the client is in the middle of session takeover/resumption
            %% or the client is in a zombie state caused stale channels lingering
            %% either case, delay the notification until the client is stable
            ?LOG(debug, "dispatch_ignored_due_to_multiple_sessions", #{vin => VIN}),
            partition_per_node(VINs, Acc);
        [] ->
            ?LOG(debug, "dispatch_ignored_due_to_no_session", #{vin => VIN}),
            partition_per_node(VINs, Acc)
    end.

%% @private
%% Group sessions by dispatcher. Returns a list of {Dispatcher, [{Pid, VIN}]} tuples.
partition_per_dispatcher(Sessions) ->
    partition_per_dispatcher(Sessions, #{}).

partition_per_dispatcher([], Acc) ->
    maps:to_list(Acc);
partition_per_dispatcher([{Pid, VIN} | Sessions], Acc) ->
    case is_online_sub_idle(Pid, VIN) of
        true ->
            Dispatcher = gproc_pool:pick_worker(?DISPATCHER_POOL, VIN),
            L = maps:get(Dispatcher, Acc, []),
            partition_per_dispatcher(Sessions, maps:put(Dispatcher, [{Pid, VIN} | L], Acc));
        {false, Reason} ->
            ?LOG(debug, "dispatch_ignored", #{vin => VIN, reason => Reason}),
            partition_per_dispatcher(Sessions, Acc)
    end.

notify_dispatchers(VINs) ->
    Sessions = partition_per_node(VINs),
    notify_dispatchers_per_node(Sessions).

notify_dispatchers_per_node([]) ->
    ok;
notify_dispatchers_per_node([{Node, Sessions} | Rest]) when node() =:= Node ->
    ok = notify_dispatchers_local(Sessions),
    notify_dispatchers_per_node(Rest);
notify_dispatchers_per_node([{Node, Sessions} | Rest]) ->
    %% the client is not on this node, RPC to the remote node
    %% and the remote node will notify the dispatcher
    _ = emqx_rpc:cast(Node, ?MODULE, notify_dispatchers_rpc_handler, [Sessions]),
    notify_dispatchers_per_node(Rest).

notify_dispatchers_local(Sessions) ->
    Dispatchers = partition_per_dispatcher(Sessions),
    lists:foreach(
        fun({DispatcherPid, SessionsPerDispatcher}) ->
            gen_server:cast(DispatcherPid, ?NOTIFY_BATCH(SessionsPerDispatcher))
        end,
        Dispatchers
    ).

notify_dispatchers_rpc_handler(Sessions) ->
    notify_dispatchers_local(Sessions).

%% @private
%% Returns 'true' if the client is
%% 1) online
%% 2) has subscribed to the topic
%% 3) has no inflight messages
%% Otherwise, returns {false, Reason}
is_online_sub_idle(Pid, VIN) when node(Pid) =:= node() ->
    Checks = [
        {fun() -> emqx_cm:is_channel_connected(Pid) end, not_connected},
        {fun() -> emqx_broker:subscribed(Pid, render_sub_topic(VIN)) end, not_subscribed},
        {fun() -> not emqx_sdv_fanout_inflight:exists(Pid) end, pending_on_ack}
    ],
    check(Checks).

check([]) ->
    true;
check([{Check, Reason} | Checks]) ->
    case Check() of
        true ->
            check(Checks);
        false ->
            {false, Reason}
    end.

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

handle_cast(?NOTIFY_BATCH(Sessions), State) ->
    %% Notified by SDV platform when a new fanout message is published
    Continues = lists:map(
        fun({SubPid, VIN}) -> ?MAYBE_SEND(?TRG_NEW_BATCH, SubPid, VIN) end, Sessions
    ),
    handle_batch(Continues, State);
handle_cast(?MAYBE_SEND(_Trigger, _SubPid, _VIN_Or_RefKey) = Continue, State) ->
    %% Notified by vehicle client process itself when 'online' or 'heartbeat' is received
    handle_batch([Continue], State);
handle_cast(?ACKED(SubPid, RefKey, MRef), State) ->
    %% Notified by vehicle client process itself when 'PUBACK' is received
    %% no flush, stale DOWN message does no harm
    _ = erlang:demonitor(MRef),
    emqx_sdv_fanout_inflight:delete(SubPid),
    Continue = ?MAYBE_SEND(?TRG_ACKED, SubPid, RefKey),
    {noreply, State, {continue, Continue}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_continue(?MAYBE_SEND(Trigger, SubPid, VIN_Or_RefKey), #{id := Id} = State) ->
    %% continue to send the next message (if any)
    %% we cannot always start from VIN, because the dirty delete
    %% in a replicant node may lag, causing the same message
    %% to be dispatched again
    case maybe_send(Trigger, SubPid, VIN_Or_RefKey, Id) of
        {ok, RefKey} ->
            MRef = erlang:monitor(process, SubPid),
            emqx_sdv_fanout_inflight:insert(SubPid, RefKey, self(), MRef);
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

handle_batch([], State) ->
    {noreply, State};
handle_batch([?MAYBE_SEND(_Trigger, SubPid, _VIN_Or_RefKey) = Continue | More], State) ->
    %% check if there is already an inflight message
    %% this check is needed to avoid race condition:
    %% caller observed no inflight message while
    %% the dispatcher is sending the message.
    case emqx_sdv_fanout_inflight:exists(SubPid) of
        true ->
            handle_batch(More, State);
        false ->
            {noreply, NewState} = handle_continue(Continue, State),
            handle_batch(More, NewState)
    end.

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
    ?REF_KEY(VIN, _Ts, _RequestId) = RefKey,
    SubTopic = render_sub_topic(VIN),
    case emqx_broker:subscribed(SubPid, SubTopic) of
        true ->
            maybe_send3(Trigger, SubPid, RefKey, DataID, DispatcherId);
        false ->
            ?LOG(debug, "vehicle_not_subscribed", #{
                vin => VIN,
                sub_topic => SubTopic
            }),
            ignore
    end.

maybe_send3(Trigger, SubPid, RefKey, DataID, DispatcherId) ->
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
    Topic = render_pub_topic(VIN, RequestId),
    From = pseudo_clientid(DispatcherId),
    Qos = 1,
    Message = emqx_message:make(From, Qos, Topic, Data),
    erlang:send(SubPid, {deliver, Topic, Message}),
    ?LOG(debug, "message_delivered_to_subscriber_session", #{
        vin => VIN,
        request_id => RequestId
    }),
    ok.

render_sub_topic(VIN) ->
    render_topic(VIN, "+").

render_pub_topic(VIN, RequestId) ->
    render_topic(VIN, RequestId).

render_topic(VIN, RequestId) ->
    TopicPrefix = emqx_sdv_config:get_topic_prefix(),
    [Prefix, Suffix] = binary:split(TopicPrefix, <<"{VIN}">>),
    bin([Prefix, VIN, Suffix, "/", RequestId]).

pseudo_clientid(DispatcherId) ->
    bin(["sdv-fanout-dispatcher-", integer_to_binary(DispatcherId)]).

bin(Iolist) ->
    iolist_to_binary(Iolist).

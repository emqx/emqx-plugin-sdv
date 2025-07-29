%% This test suite verifies the plugin as a blackbox.
%% It assumes that the broker with the plugin installed
%% is running on endpoint defined in environment variable
%% EMQX_MQTT_ENDPOINT.
-module(emqx_sdv_fanout_integration_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() ->
    [
        F
     || {F, _} <- ?MODULE:module_info(exports),
        is_test_function(F)
    ].

is_test_function(F) ->
    case atom_to_list(F) of
        "t_" ++ _ -> true;
        _ -> false
    end.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%% Vehicles connect and subscribe, then a batch is published by SDV platform.
t_realtime_dispatch(_Config) ->
    %% Test with a unique set of VINs to avoid contamination from previous tests.
    UniqueId = erlang:system_time(millisecond),
    VINs = [vin(UniqueId, I) || I <- lists:seq(1, 10)],
    {ok, SubPids} = start_vehicle_clients(VINs),
    try
        {ok, PubPid} = start_batch_publisher(),
        try
            {ok, {RequestId, Data}} = publish_batch(PubPid, VINs),
            ok = assert_payload_received(SubPids, RequestId, Data)
        after
            ok = stop_client(PubPid)
        end
    after
        ok = stop_clients(SubPids)
    end.

%% A batch is published by SDV platform, then vehicles connect and subscribe.
%% Expect the dispatches to be triggered by heartbeat messages.
t_late_subscribe(_Config) ->
    UniqueId = erlang:system_time(millisecond),
    VINs = [vin(UniqueId, I) || I <- lists:seq(1, 10)],
    {ok, PubPid} = start_batch_publisher(),
    try
        {ok, {RequestId, Data}} = publish_batch(PubPid, VINs),
        {ok, SubPids} = start_vehicle_clients(VINs),
        receive
            {publish_received, _, _, _} ->
                ct:fail("unexpected publish message received")
        after 1000 ->
            ok
        end,
        HeartbeatPid = send_heartbeats(SubPids),
        try
            ok = assert_payload_received(SubPids, RequestId, Data)
        after
            HeartbeatPid ! stop,
            ok = stop_clients(SubPids)
        end
    after
        ok = stop_client(PubPid)
    end.

%% Clients will not receive dispatch after reconnect (with session persisted)
%% but will receive after heartbeat is sent.
t_disconnected_session_does_not_receive_dispatch(_Config) ->
    UniqueId = erlang:system_time(millisecond),
    VINs = [vin(UniqueId, I) || I <- lists:seq(1, 5)],
    {ok, PubPid} = start_batch_publisher(),
    try
        {ok, {RequestId, Data}} = publish_batch(PubPid, VINs),
        Opts = [{clean_start, false}, {properties, #{'Session-Expiry-Interval' => 5}}],
        {ok, SubPids0} = start_vehicle_clients(VINs, Opts),
        ok = stop_clients(SubPids0),
        {ok, SubPids} = start_vehicle_clients(VINs, Opts),
        ok = assert_session_persisted(SubPids),
        receive
            {publish_received, _, _, _} ->
                ct:fail("unexpected publish message received")
        after 1000 ->
            ok
        end,
        HeartbeatPid = send_heartbeats(SubPids),
        try
            ok = assert_payload_received(SubPids, RequestId, Data)
        after
            HeartbeatPid ! stop,
            ok = stop_clients(SubPids)
        end
    after
        ok = stop_client(PubPid)
    end.

%% The vehicle client crashes after receiving the first message,
%% then reconnects and process the redelivered message normally.
t_reconnect_after_disconnect(_Config) ->
    UniqueId = erlang:system_time(millisecond),
    VIN = vin(UniqueId, 1),
    {ok, PubPid} = start_batch_publisher(),
    try
        {ok, {RequestId, Data}} = publish_batch(PubPid, [VIN]),
        Opts = [{clean_start, false}, {properties, #{'Session-Expiry-Interval' => 5}}],
        {ok, SubPid1, Mref1} = start_reconnect_vehicle_client(VIN, true, Opts),
        HeartbeatPid = send_heartbeats([{VIN, SubPid1}]),
        receive
            {'DOWN', Mref1, process, SubPid1, _Reason} ->
                ok
        after 5000 ->
            ct:fail(#{reason => "client is not down as expected"})
        end,
        HeartbeatPid ! stop,
        {ok, SubPid2, _Mref2} = start_reconnect_vehicle_client(VIN, false, Opts),
        %% Expect the message to be redelivered without any trigger,
        %% because the last session terminated before PUBACK is sent.
        receive
            {publish_received, SubPid2, VIN, Msg} ->
                ?assertEqual(Data, maps:get(payload, Msg))
        after 5000 ->
            ct:fail(#{reason => "client did not receive the message as expected"})
        end,
        ok = stop_client(SubPid2)
    after
        ok = stop_client(PubPid)
    end.

t_non_json_payload_causes_disconnect(_Config) ->
    test_invalid_trigger_payload(<<"not json">>).

t_invalid_payload_causes_disconnect(_Config) ->
    test_invalid_trigger_payload(<<"{\"ids\": [\"vin-1\"]}">>).

test_invalid_trigger_payload(Payload) ->
    {ok, PubPid} = start_batch_publisher(),
    unlink(PubPid),
    TriggerTopic = <<"$SDV-FANOUT/trigger">>,
    QoS = 1,
    ?assertMatch({error, {disconnected, _, _}}, emqtt:publish(PubPid, TriggerTopic, Payload, QoS)).

assert_payload_received([], _RequestId, _Data) ->
    ok;
assert_payload_received(SubPids, RequestId, Data) ->
    Remain =
        receive
            {publish_received, SubPid, VIN, Msg} ->
                ?assertMatch(#{qos := 1}, Msg),
                ?assertEqual(Data, maps:get(payload, Msg)),
                Topic = maps:get(topic, Msg),
                [<<"agent">>, VIN0, <<"proxy">>, <<"request">>, RequestId] = words(Topic),
                ?assertEqual(VIN, VIN0),
                case lists:member({VIN, SubPid}, SubPids) of
                    true ->
                        lists:delete({VIN, SubPid}, SubPids);
                    false ->
                        ct:fail(#{
                            reason => unexpected_subscriber,
                            got => {VIN, SubPid},
                            expected => SubPids
                        })
                end
        after 10000 ->
            ct:fail(#{
                reason => timeout,
                remain => SubPids
            })
        end,
    assert_payload_received(Remain, RequestId, Data).

start_batch_publisher() ->
    {ok, Pid} = emqtt:start_link([
        {host, mqtt_endpoint()}, {clientid, <<"sdv-batch-publisher">>}, {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(Pid),
    {ok, Pid}.

mqtt_endpoint() ->
    case os:getenv("EMQX_MQTT_ENDPOINT") of
        false ->
            "127.0.0.1";
        Endpoint ->
            Endpoint
    end.

publish_batch(Pid, VINs) ->
    DataID = integer_to_binary(erlang:system_time(millisecond)),
    DataTopic = bin(["$SDV-FANOUT/data/", DataID]),
    TriggerTopic = <<"$SDV-FANOUT/trigger">>,
    QoS = 1,
    Data = crypto:strong_rand_bytes(1024),
    RequestId = integer_to_binary(erlang:system_time(millisecond)),
    Trigger = emqx_utils_json:encode(#{ids => VINs, request_id => RequestId, data_id => DataID}),
    {ok, _} = emqtt:publish(Pid, DataTopic, Data, QoS),
    {ok, _} = emqtt:publish(Pid, TriggerTopic, Trigger, QoS),
    {ok, {RequestId, Data}}.

%% Start a number of vehicle clients which subscribes to the topic
%% "agent/<vin>/proxy/request/+".
start_vehicle_clients(VINs) ->
    start_vehicle_clients(VINs, []).

start_vehicle_clients(VINs, Opts) ->
    Owner = self(),
    lists:foldl(
        fun(VIN, {ok, Pids}) ->
            MsgHandler = #{publish => fun(Msg) -> Owner ! {publish_received, self(), VIN, Msg} end},
            {ok, Pid} = emqtt:start_link(
                [
                    {clientid, VIN}, {proto_ver, v5}, {msg_handler, MsgHandler}
                ] ++ Opts
            ),
            {ok, _} = emqtt:connect(Pid),
            QoS = 1,
            {ok, _, _} = emqtt:subscribe(Pid, sub_topic(VIN), QoS),
            {ok, [{VIN, Pid} | Pids]}
        end,
        {ok, []},
        VINs
    ).

start_reconnect_vehicle_client(VIN, ShouldDisconnect, Opts) ->
    Owner = self(),
    MsgHandler = #{
        publish => fun(Msg) ->
            ct:pal("publish received from EMQX: ~p", [Msg]),
            case ShouldDisconnect of
                true ->
                    exit(self(), kill);
                false ->
                    Owner ! {publish_received, self(), VIN, Msg}
            end
        end
    },
    {ok, Pid} = emqtt:start_link(
        [
            {clientid, VIN}, {proto_ver, v5}, {msg_handler, MsgHandler}
        ] ++ Opts
    ),
    {ok, _} = emqtt:connect(Pid),
    Mref = monitor(process, Pid),
    unlink(Pid),
    QoS = 1,
    {ok, _, _} = emqtt:subscribe(Pid, sub_topic(VIN), QoS),
    {ok, Pid, Mref}.

sub_topic(VIN) ->
    bin(["agent/", VIN, "/proxy/request/+"]).

stop_clients(Pids) ->
    lists:foreach(
        fun({_, Pid}) ->
            stop_client(Pid)
        end,
        Pids
    ).

stop_client(Pid) ->
    unlink(Pid),
    ok = emqtt:stop(Pid).

vin(UniqueId, I) ->
    list_to_binary(["vin-", integer_to_list(UniqueId), "-", integer_to_list(I)]).

words(Topic) ->
    binary:split(Topic, <<"/">>, [global]).

%% First send online message, then send heartbeat messages.
send_heartbeats(SubPids) ->
    lists:foreach(
        fun({VIN, Pid}) ->
            Topic = bin([<<"ecp/">>, VIN, <<"/online">>]),
            {ok, _} = emqtt:publish(Pid, Topic, <<"">>, 1)
        end,
        SubPids
    ),
    spawn_link(fun() -> heartbeat_loop(SubPids) end).

heartbeat_loop(SubPids) ->
    receive
        stop ->
            exit(normal)
    after 1000 ->
        lists:foreach(
            fun({VIN, Pid}) ->
                Topic = bin([<<"ecp/">>, VIN, <<"/heartbeat">>]),
                {ok, _} = emqtt:publish(Pid, Topic, <<"">>, 1)
            end,
            SubPids
        ),
        heartbeat_loop(SubPids)
    end.

bin(IoData) ->
    iolist_to_binary(IoData).

assert_session_persisted(SubPids) ->
    lists:foreach(
        fun({_VIN, Pid}) ->
            Info = emqtt:info(Pid),
            InfoMap = maps:from_list(Info),
            ?assertEqual(1, maps:get(session_present, InfoMap))
        end,
        SubPids
    ).

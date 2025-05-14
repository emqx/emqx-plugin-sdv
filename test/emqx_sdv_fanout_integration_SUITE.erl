%% This test suite verifies the plugin as a blackbox.
%% It assumes that the broker with the plugin installed
%% is running on endpoint defined in environment variable
%% EMQX_MQTT_ENDPOINT.
-module(emqx_sdv_fanout_integration_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile({nowarn_export_all, true}).

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

end_per_suite(Config) ->
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
        {ok, PubPid} = start_batch_publisher(VINs),
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
    {ok, PubPid} = start_batch_publisher(VINs),
    try
        {ok, {RequestId, Data}} = publish_batch(PubPid, VINs),
        {ok, SubPids} = start_vehicle_clients(VINs),
        receive
            {publish_received, _, _, _} ->
                ct:fail("unexpected publish message received")
        after 1000 ->
            ok
        end,
        ok = send_heartbeat(SubPids),
        try
            ok = assert_payload_received(SubPids, RequestId, Data)
        after
            ok = stop_clients(SubPids)
        end
    after
        ok = stop_client(PubPid)
    end.

%% Clients will not receive dispatch after reconnect (with session persisted)
%% but will receive after heartbeat is sent.
t_disconnected_session_does_not_receive_dispatch(_Config) ->
    UniqueId = erlang:system_time(millisecond),
    VINs = [vin(UniqueId, I) || I <- lists:seq(1, 1)],
    {ok, PubPid} = start_batch_publisher(VINs),
    try
        {ok, {RequestId, Data}} = publish_batch(PubPid, VINs),
        Opts = [{clean_start, false}, {properties, #{'Session-Expiry-Interval' => 60}}],
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
        ok = send_heartbeat(SubPids),
        try
            ok = assert_payload_received(SubPids, RequestId, Data)
        after
            ok = stop_clients(SubPids)
        end
    after
        ok = stop_client(PubPid)
    end.

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
                ?assert(lists:member({VIN, SubPid}, SubPids)),
                lists:delete({VIN, SubPid}, SubPids)
        after 10000 ->
            ct:fail(#{
                reason => timeout,
                remain => SubPids
            })
        end,
    assert_payload_received(Remain, RequestId, Data).

start_batch_publisher(BatchSize) ->
    {ok, Pid} = emqtt:start_link([{clientid, <<"sdv-batch-publisher">>}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(Pid),
    {ok, Pid}.

publish_batch(Pid, VINs) ->
    Topic = <<"$SDV-FANOUT">>,
    QoS = 1,
    Data = base64:encode(crypto:strong_rand_bytes(1024)),
    RequestId = integer_to_binary(erlang:system_time(millisecond)),
    Payload = emqx_utils_json:encode(#{ids => VINs, request_id => RequestId, data => Data}),
    {ok, _} = emqtt:publish(Pid, Topic, Payload, QoS),
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
            {ok, Pid} = emqtt:start_link([
                {clientid, VIN}, {proto_ver, v5}, {msg_handler, MsgHandler}
            ] ++ Opts),
            {ok, _} = emqtt:connect(Pid),
            QoS = 1,
            {ok, _, _} = emqtt:subscribe(Pid, sub_topic(VIN), QoS),
            {ok, [{VIN, Pid} | Pids]}
        end,
        {ok, []},
        VINs
    ).

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

send_heartbeat(SubPids) ->
    lists:foreach(
        fun({VIN, Pid}) ->
            Topic = bin([<<"ecp/">>, VIN, <<"/online">>]),
            {ok, _} = emqtt:publish(Pid, Topic, <<"">>, 1)
        end,
        SubPids
    ).

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

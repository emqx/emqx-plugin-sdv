-module(emqx_sdv_cli_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include("emqx_sdv.hrl").

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
    Token = login(),
    [{token, Token} | Config].

end_per_suite(Config) ->
    update_plugin_config(default_config(), Config),
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

t_gc_after_config_change(Config) ->
    %% Test with a unique set of VINs to avoid contamination from previous tests.
    UniqueId = erlang:system_time(millisecond),
    VINs = [vin(UniqueId, I) || I <- lists:seq(1, 10)],
    {ok, PubPid} = start_batch_publisher(),
    try
        {ok, _} = publish_batch(PubPid, VINs),
        Status1 = get_status(),
        ?assert(maps:get(<<"remaining_messages">>, Status1) >= 10),
        ?assert(maps:get(<<"unique_payloads">>, Status1) >= 1),
        PluginConfig = new_config(),
        ok = update_plugin_config(PluginConfig, Config),
        %% ensure ids and data are expired
        %% see new_config/0 for gc_interval and data_retention
        timer:sleep(1000),
        Status2 = get_status(),
        ?assertEqual(0, maps:get(<<"remaining_messages">>, Status2)),
        ?assertEqual(0, maps:get(<<"unique_payloads">>, Status2))
    after
        ok = stop_client(PubPid)
    end.

start_batch_publisher() ->
    {ok, Pid} = emqtt:start_link([
        {host, mqtt_endpoint()}, {clientid, <<"sdv-batch-publisher">>}, {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(Pid),
    {ok, Pid}.

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

mqtt_endpoint() ->
    case os:getenv("EMQX_MQTT_ENDPOINT") of
        false ->
            "127.0.0.1";
        Endpoint ->
            Endpoint
    end.

bin(S) ->
    iolist_to_binary(S).

url(Path) ->
    "http://" ++ mqtt_endpoint() ++ ":18083/api/v5/" ++ Path.

login() ->
    URL = url("login"),
    Headers = [{"Content-Type", "application/json"}],
    Body = emqx_utils_json:encode(#{username => "admin", password => "public"}),
    {ok, {{_, 200, _}, _Headers, RespBody}} = httpc:request(
        post, {URL, Headers, "application/json", Body}, [], []
    ),
    maps:get(<<"token">>, emqx_utils_json:decode(RespBody, [return_maps])).

default_config() ->
    #{
        topic_prefix => <<"agent/{VIN}/proxy/request">>,
        gc_interval => <<"1h">>,
        data_retention => <<"7d">>,
        dispatcher_pool_size => 0
    }.

new_config() ->
    maps:merge(default_config(), #{
        gc_interval => <<"1s">>,
        data_retention => <<"1s">>
    }).

name_vsn() ->
    "emqx_sdv-" ++ ?PLUGIN_VSN.

update_plugin_config(PluginConfig, CtConfig) ->
    URL = url("plugins/" ++ name_vsn() ++ "/config"),
    {token, Token} = lists:keyfind(token, 1, CtConfig),
    Headers = [
        {"Authorization", "Bearer " ++ binary_to_list(Token)},
        {"Content-Type", "application/json"}
    ],
    Body = emqx_utils_json:encode(PluginConfig),
    {ok, Resp} = httpc:request(
        put, {URL, Headers, "application/json", Body}, [], []
    ),
    {{_, StatusCode, _}, _, _} = Resp,
    case StatusCode of
        200 ->
            ok;
        204 ->
            ok;
        _ ->
            ct:pal("Failed to update plugin config: ~p", [Resp]),
            {error, StatusCode}
    end.

get_status() ->
    Out = os:cmd("../../../../scripts/cli.sh 1 status"),
    ct:pal("Status: ~s", [Out]),
    emqx_utils_json:decode(Out, [return_maps]).
